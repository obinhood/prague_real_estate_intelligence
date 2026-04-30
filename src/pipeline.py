from datetime import datetime
import os
import pandas as pd

from src.adapters.sreality import SrealityAdapter
from src.adapters.bezrealitky import BezrealitkyAdapter
from src.db.database import engine
from src.db.import_clean_csvs_to_postgres import import_clean_csvs_to_postgres
from src.db.io import (
    has_normalized_postgres_schema,
    init_db,
    read_postgres_current_state_df,
    read_postgres_history_df,
    read_table_df,
    write_dataframe_replace,
)
from src.reports.generate_reports import generate_daily_price_csv, generate_market_report_html, generate_removed_listings_csv
from src.utils.logger import get_logger
from src.utils.process_csv import process_master_dataframe
from src.utils.state import reconcile_current_with_previous, build_history_snapshot

logger = get_logger("pipeline")


def enrich_district_medians(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add ``price_vs_district_median_pct`` to *df*.

    For each active listing we compute how far its ``price_per_m2_czk`` sits
    relative to the district-level median price-per-m².  Removed listings and
    rows without a valid price or district are left as ``None``.

    Formula: ((listing_price_per_m2 / district_median_price_per_m2) - 1) * 100
      > 0  → listing is above district median (more expensive per m²)
      < 0  → listing is below district median (cheaper per m²)
    """
    if df.empty:
        df["price_vs_district_median_pct"] = None
        return df

    out = df.copy()
    out["price_vs_district_median_pct"] = None

    # Only compute for active listings with numeric data
    mask = (
        (out.get("is_active", pd.Series(True, index=out.index)) == True) &
        out["price_per_m2_czk"].notna() &
        out["district_name"].notna() &
        (out["district_name"] != "Praha - Ostatní")
    )
    active = out.loc[mask].copy()
    if active.empty:
        return out

    medians = (
        active.groupby("district_name")["price_per_m2_czk"]
        .median()
        .rename("district_median_ppm2")
    )
    active = active.join(medians, on="district_name")
    active["price_vs_district_median_pct"] = (
        (active["price_per_m2_czk"] / active["district_median_ppm2"] - 1) * 100
    ).round(2)

    out.loc[mask, "price_vs_district_median_pct"] = active["price_vs_district_median_pct"].values
    logger.info(
        f"STAGE: District median enrichment complete | "
        f"districts: {len(medians)} | enriched rows: {mask.sum()}"
    )
    return out


def collect_from_sources(include_bezrealitky=False):
    logger.info("STAGE: Source collection started")
    rows = []
    logger.info("STAGE: Running Sreality adapter")
    rows.extend(SrealityAdapter().scrape())
    if include_bezrealitky:
        logger.info("STAGE: Running Bezrealitky adapter")
        rows.extend(BezrealitkyAdapter().scrape())
    else:
        logger.info("STAGE: Skipping Bezrealitky by user choice")
    logger.info(f"STAGE: Source collection finished | total raw rows: {len(rows)}")
    return rows


def run_pipeline(include_bezrealitky=False):
    logger.info("========== PIPELINE START ==========")
    os.makedirs("data", exist_ok=True)
    init_db()
    now = datetime.utcnow()
    logger.info("STAGE: Collecting source data")
    raw_df = pd.DataFrame(collect_from_sources(include_bezrealitky=include_bezrealitky))
    if raw_df.empty:
        raw_df = pd.DataFrame(columns=["composite_id", "url_id", "source", "property_search_type", "url", "property_link", "title"])
    raw_df["timestamp"] = now
    raw_df["exists"] = True

    master_csv = "data/listings_master.csv"
    processed_csv = "data/listings_processed.csv"
    history_csv = "data/listing_history.csv"
    removed_csv = "data/removed_listings.csv"

    logger.info("STAGE: Writing raw master CSV mirror")
    raw_df.to_csv(master_csv, index=False)
    logger.info("STAGE: Processing scraped rows into structured dataset")
    processed_df = process_master_dataframe(raw_df)
    processed_df.to_csv(processed_csv, index=False)
    logger.info("STAGE: Loading previous market state")
    use_normalized_postgres = engine.dialect.name == "postgresql" and has_normalized_postgres_schema()
    if use_normalized_postgres:
        previous_state = read_postgres_current_state_df()
        previous_history = read_postgres_history_df()
    else:
        previous_state = read_table_df("listings")
        previous_history = read_table_df("listing_history")
    logger.info("STAGE: Reconciling current state with previous state")
    current_state, summary = reconcile_current_with_previous(processed_df, previous_state, now)
    logger.info("STAGE: Enriching with cross-listing district median fields")
    current_state = enrich_district_medians(current_state)
    logger.info("STAGE: Building history snapshot")
    history_snapshot = build_history_snapshot(processed_df, previous_state, now)
    logger.info("STAGE: Persisting history to database")
    full_history = pd.concat([previous_history, history_snapshot], ignore_index=True) if not previous_history.empty else history_snapshot.copy()
    # Deduplicate so repeated same-day runs don't accumulate duplicate history rows
    full_history = full_history.drop_duplicates(
        subset=["composite_id", "scraped_at", "exists_on_source"], keep="last"
    ).reset_index(drop=True)
    logger.info("STAGE: Writing CSV mirrors")
    current_state.to_csv(processed_csv, index=False)
    full_history.to_csv(history_csv, index=False)
    removed_state = current_state[current_state.get("is_removed", False) == True].copy() if "is_removed" in current_state.columns else pd.DataFrame()
    removed_state.to_csv(removed_csv, index=False)
    if use_normalized_postgres:
        logger.info("STAGE: Refreshing normalized PostgreSQL analytics tables")
        import_clean_csvs_to_postgres()
    else:
        logger.info("STAGE: Persisting current state to development database")
        write_dataframe_replace(current_state, "listings")
        logger.info("STAGE: Persisting history to development database")
        write_dataframe_replace(full_history, "listing_history")
    logger.info("STAGE: Generating reports")
    generate_daily_price_csv(full_history)
    generate_market_report_html(current_state)
    generate_removed_listings_csv(current_state, removed_csv)

    pipeline_summary = {
        "scraped_rows": int(len(raw_df)),
        "active_rows": int(summary.get("active_listings", 0)),
        "new_listings": int(summary.get("new_listings", 0)),
        "removed_listings": int(summary.get("removed_listings", 0)),
        "price_changes": int(summary.get("price_changes", 0)),
        "master_csv": master_csv,
        "processed_csv": processed_csv,
        "history_csv": history_csv,
        "removed_csv": removed_csv,
        "report_csv": "data/daily_price_report.csv",
        "html_report": "data/market_report.html",
        "log_file": "logs/tracker.log",
    }
    logger.info(f"PIPELINE SUMMARY: {pipeline_summary}")
    logger.info("========== PIPELINE END ==========")
    return pipeline_summary
