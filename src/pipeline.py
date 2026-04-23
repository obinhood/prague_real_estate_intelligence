from datetime import datetime
import os
import pandas as pd

from src.adapters.sreality import SrealityAdapter
from src.adapters.bezrealitky import BezrealitkyAdapter
from src.db.io import init_db, read_table_df, write_dataframe_replace
from src.reports.generate_reports import generate_daily_price_csv, generate_market_report_html, generate_removed_listings_csv
from src.utils.logger import get_logger
from src.utils.process_csv import process_master_csv
from src.utils.state import reconcile_current_with_previous, build_history_snapshot

logger = get_logger("pipeline")


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

    logger.info("STAGE: Writing raw master CSV")
    raw_df.to_csv(master_csv, index=False)
    logger.info("STAGE: Processing CSV into structured dataset")
    process_master_csv(master_csv, processed_csv)
    processed_df = pd.read_csv(processed_csv)
    logger.info("STAGE: Loading previous DB state")
    previous_state = read_table_df("listings")
    previous_history = read_table_df("listing_history")
    logger.info("STAGE: Reconciling current state with previous state")
    current_state, summary = reconcile_current_with_previous(processed_df, previous_state, now)
    logger.info("STAGE: Building history snapshot")
    history_snapshot = build_history_snapshot(processed_df, previous_state, now)
    logger.info("STAGE: Persisting current state to database")
    write_dataframe_replace(current_state, "listings")
    logger.info("STAGE: Persisting history to database")
    full_history = pd.concat([previous_history, history_snapshot], ignore_index=True) if not previous_history.empty else history_snapshot.copy()
    write_dataframe_replace(full_history, "listing_history")
    logger.info("STAGE: Writing CSV mirrors")
    current_state.to_csv(processed_csv, index=False)
    full_history.to_csv(history_csv, index=False)
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
