from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Tuple

import pandas as pd
from sqlalchemy import text

from src.config import CONFIG
from src.db.database import engine
from src.db.postgres_schema import apply_postgres_schema
from src.utils.logger import get_logger
from src.utils.process_csv import extract_detail_features, looks_like_listing_title, parse_title

logger = get_logger("postgres-import")


DATA_DIR = Path("data")
CLEAN_DIR = DATA_DIR / "cleaned"


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path) if path.exists() else pd.DataFrame()


def _safe_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def _safe_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _series_or_default(frame: pd.DataFrame, column_name: str, default=None) -> pd.Series:
    if column_name in frame.columns:
        return frame[column_name]
    return pd.Series([default] * len(frame), index=frame.index)


def _reparse_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "title" not in frame.columns:
        return frame.copy()
    out = frame.copy()
    out["title"] = out["title"].astype(str).str.strip()
    out = out[out["title"].apply(looks_like_listing_title)].copy()
    if out.empty:
        return out

    parsed = pd.DataFrame(
        [parse_title(title, property_search_type) for title, property_search_type in zip(out["title"].fillna(""), out.get("property_search_type", pd.Series([None] * len(out))))]
    )
    for column in ["property_type_code", "property_type", "layout_type", "area_m2", "price_czk", "price_per_m2_czk", "full_address", "street_address", "borough_name", "district_name", "prague_zone", "location_quality", "city_name", "region_name", "country_name"]:
        out[column] = parsed[column]

    if "details_json" in out.columns:
        feature_df = out["details_json"].apply(extract_detail_features).apply(pd.Series)
        for column in feature_df.columns:
            out[f"has_{column}" if not column.startswith("has_") else column] = feature_df[column]

    for column in ["price_czk", "price_per_m2_czk", "area_m2", "listing_duration_days", "removed_duration_days", "previous_price_czk", "price_change_czk", "latitude", "longitude"]:
        if column in out.columns:
            out[column] = _safe_numeric(out[column])
    for column in ["first_seen_at", "last_seen_at", "removed_at", "scraped_at", "timestamp"]:
        if column in out.columns:
            out[column] = _safe_datetime(out[column])
    return out


def clean_existing_csv_exports() -> Dict[str, Path]:
    CLEAN_DIR.mkdir(parents=True, exist_ok=True)

    listings_processed = _reparse_frame(_read_csv(DATA_DIR / "listings_processed.csv"))
    listing_history = _reparse_frame(_read_csv(DATA_DIR / "listing_history.csv"))
    removed_listings = _reparse_frame(_read_csv(DATA_DIR / "removed_listings.csv"))

    if not listings_processed.empty:
        listings_processed = listings_processed.drop_duplicates(subset=["composite_id"], keep="first")
    if not listing_history.empty:
        subset = [column for column in ["composite_id", "scraped_at", "exists_on_source"] if column in listing_history.columns]
        if subset:
            listing_history = listing_history.drop_duplicates(subset=subset, keep="last")
    if not removed_listings.empty:
        removed_listings = removed_listings.drop_duplicates(subset=["composite_id"], keep="first")

    paths = {
        "listings_processed": CLEAN_DIR / "listings_processed_clean.csv",
        "listing_history": CLEAN_DIR / "listing_history_clean.csv",
        "removed_listings": CLEAN_DIR / "removed_listings_clean.csv",
    }
    listings_processed.to_csv(paths["listings_processed"], index=False)
    listing_history.to_csv(paths["listing_history"], index=False)
    removed_listings.to_csv(paths["removed_listings"], index=False)

    logger.info(
        "Cleaned legacy CSVs | processed=%s history=%s removed=%s",
        len(listings_processed),
        len(listing_history),
        len(removed_listings),
    )
    return paths


def _seed_sources(conn) -> Dict[str, int]:
    mapping = {}
    for source_code, source_cfg in CONFIG["sources"].items():
        source_name = source_code.replace("_", " ").title()
        row = conn.execute(
            text(
                """
                INSERT INTO sources (source_code, source_name, source_domain)
                VALUES (:source_code, :source_name, :source_domain)
                ON CONFLICT (source_code)
                DO UPDATE SET
                    source_name = EXCLUDED.source_name,
                    source_domain = EXCLUDED.source_domain,
                    is_active = TRUE
                RETURNING source_id
                """
            ),
            {
                "source_code": source_code,
                "source_name": source_name,
                "source_domain": source_cfg.get("domain"),
            },
        ).scalar_one()
        mapping[source_code] = row
    return mapping


def _prepare_current_state(processed_df: pd.DataFrame, removed_df: pd.DataFrame) -> pd.DataFrame:
    current_df = processed_df.copy()
    if current_df.empty:
        return current_df

    if "property_link" not in current_df.columns and "listing_url" in current_df.columns:
        current_df["property_link"] = current_df["listing_url"]

    current_df["is_active"] = current_df.get("is_active", True)
    current_df["is_removed"] = current_df.get("is_removed", False)
    current_df["snapshot_date"] = pd.to_datetime(current_df.get("snapshot_date", current_df.get("last_seen_at")), errors="coerce").dt.date

    if not removed_df.empty:
        removed_ids = set(removed_df["composite_id"].astype(str))
        current_df.loc[current_df["composite_id"].astype(str).isin(removed_ids), "is_active"] = False
        current_df.loc[current_df["composite_id"].astype(str).isin(removed_ids), "is_removed"] = True
        if "removed_at" in removed_df.columns:
            removed_map = removed_df.set_index("composite_id")["removed_at"].to_dict()
            current_df["removed_at"] = current_df["composite_id"].map(removed_map).combine_first(_series_or_default(current_df, "removed_at"))
        if "removed_duration_days" in removed_df.columns:
            duration_map = removed_df.set_index("composite_id")["removed_duration_days"].to_dict()
            current_df["removed_duration_days"] = current_df["composite_id"].map(duration_map).combine_first(_series_or_default(current_df, "removed_duration_days"))
    return current_df


def _prepare_history(history_df: pd.DataFrame, current_df: pd.DataFrame) -> pd.DataFrame:
    out = history_df.copy()
    if out.empty:
        snapshot_now = pd.Timestamp.utcnow()
        snapshot_date = snapshot_now.date()
        active_current = current_df[current_df["is_active"] == True].copy()
        active_current["scraped_at"] = snapshot_now
        active_current["snapshot_date"] = snapshot_date
        active_current["exists_on_source"] = True
        out = active_current

    if "scraped_at" not in out.columns:
        fallback = pd.to_datetime(out.get("last_seen_at", pd.Timestamp.utcnow()), errors="coerce")
        out["scraped_at"] = fallback
    out["scraped_at"] = _safe_datetime(out["scraped_at"])
    out["snapshot_date"] = pd.to_datetime(out.get("snapshot_date", out["scraped_at"]), errors="coerce").dt.date
    if "exists_on_source" not in out.columns:
        out["exists_on_source"] = True
    return out


def _bool_value(value):
    if pd.isna(value):
        return None
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes"}


def _null_if_nan(value):
    return None if pd.isna(value) else value


def _json_payload(row: pd.Series) -> Dict:
    payload = {}
    keys = [
        # original fields
        "description", "details_json", "energy_class", "seller_type", "ownership_type", "floor",
        # derived analyst fields
        "bedroom_count", "is_studio", "size_band", "amenity_score",
        "floor_category", "prague_ring", "is_new_build", "price_tier",
        "price_change_pct", "listing_age_bucket", "price_vs_district_median_pct",
        "has_balcony", "has_parking", "has_terrace", "has_elevator", "has_cellar",
        "relisted", "relisted_at",
    ]
    for key in keys:
        if key in row:
            val = row[key]
            try:
                if pd.notna(val):
                    payload[key] = val
            except (TypeError, ValueError):
                if val is not None:
                    payload[key] = val
    return payload or None


def _upsert_listings(conn, current_df: pd.DataFrame, source_map: Dict[str, int]) -> Dict[str, int]:
    listing_id_map: Dict[str, int] = {}
    for row in current_df.to_dict("records"):
        source_code = row.get("source")
        listing_id = conn.execute(
            text(
                """
                INSERT INTO listings (
                    composite_id, source_id, source_listing_key, url_id, property_search_type,
                    property_type_code, property_type, transaction_type, listing_url, latest_title,
                    first_seen_at, last_seen_at, latest_snapshot_date, current_status,
                    last_known_price_czk, last_known_price_per_m2_czk, last_known_area_m2,
                    latest_district_name, latest_borough_name, latest_prague_zone,
                    location_quality, latest_source_payload
                )
                VALUES (
                    :composite_id, :source_id, :source_listing_key, :url_id, :property_search_type,
                    :property_type_code, :property_type, :transaction_type, :listing_url, :latest_title,
                    :first_seen_at, :last_seen_at, :latest_snapshot_date, :current_status,
                    :last_known_price_czk, :last_known_price_per_m2_czk, :last_known_area_m2,
                    :latest_district_name, :latest_borough_name, :latest_prague_zone,
                    :location_quality, CAST(:latest_source_payload AS JSONB)
                )
                ON CONFLICT (composite_id)
                DO UPDATE SET
                    source_id = EXCLUDED.source_id,
                    source_listing_key = EXCLUDED.source_listing_key,
                    url_id = EXCLUDED.url_id,
                    property_search_type = EXCLUDED.property_search_type,
                    property_type_code = EXCLUDED.property_type_code,
                    property_type = EXCLUDED.property_type,
                    transaction_type = EXCLUDED.transaction_type,
                    listing_url = EXCLUDED.listing_url,
                    latest_title = EXCLUDED.latest_title,
                    first_seen_at = EXCLUDED.first_seen_at,
                    last_seen_at = EXCLUDED.last_seen_at,
                    latest_snapshot_date = EXCLUDED.latest_snapshot_date,
                    current_status = EXCLUDED.current_status,
                    last_known_price_czk = EXCLUDED.last_known_price_czk,
                    last_known_price_per_m2_czk = EXCLUDED.last_known_price_per_m2_czk,
                    last_known_area_m2 = EXCLUDED.last_known_area_m2,
                    latest_district_name = EXCLUDED.latest_district_name,
                    latest_borough_name = EXCLUDED.latest_borough_name,
                    latest_prague_zone = EXCLUDED.latest_prague_zone,
                    location_quality = EXCLUDED.location_quality,
                    latest_source_payload = EXCLUDED.latest_source_payload,
                    updated_at = NOW()
                RETURNING listing_id
                """
            ),
            {
                "composite_id": row.get("composite_id"),
                "source_id": source_map[source_code],
                "source_listing_key": row.get("url_id"),
                "url_id": _null_if_nan(row.get("url_id")),
                "property_search_type": _null_if_nan(row.get("property_search_type")),
                "property_type_code": _null_if_nan(row.get("property_type_code")),
                "property_type": _null_if_nan(row.get("property_type")),
                "transaction_type": "sale",
                "listing_url": _null_if_nan(row.get("property_link")) or _null_if_nan(row.get("listing_url")),
                "latest_title": _null_if_nan(row.get("title")),
                "first_seen_at": row.get("first_seen_at") or row.get("last_seen_at") or datetime.utcnow(),
                "last_seen_at": row.get("last_seen_at") or row.get("first_seen_at") or datetime.utcnow(),
                "latest_snapshot_date": row.get("snapshot_date") or pd.Timestamp.utcnow().date(),
                "current_status": "removed" if _bool_value(row.get("is_removed")) else "active",
                "last_known_price_czk": _null_if_nan(row.get("price_czk")),
                "last_known_price_per_m2_czk": _null_if_nan(row.get("price_per_m2_czk")),
                "last_known_area_m2": _null_if_nan(row.get("area_m2")),
                "latest_district_name": _null_if_nan(row.get("district_name")),
                "latest_borough_name": _null_if_nan(row.get("borough_name")),
                "latest_prague_zone": _null_if_nan(row.get("prague_zone")),
                "location_quality": _null_if_nan(row.get("location_quality")) or "ok",
                "latest_source_payload": json.dumps(_json_payload(pd.Series(row)), ensure_ascii=False) if _json_payload(pd.Series(row)) else None,
            },
        ).scalar_one()
        listing_id_map[row["composite_id"]] = listing_id
    return listing_id_map


def _insert_scrape_runs(conn, history_df: pd.DataFrame) -> Dict[pd.Timestamp, int]:
    run_map = {}
    grouped = history_df.groupby("scraped_at", dropna=True)
    for scraped_at, frame in grouped:
        snapshot_date = pd.Timestamp(scraped_at).date()
        run_id = conn.execute(
            text(
                """
                INSERT INTO scrape_runs (
                    snapshot_date, started_at, completed_at, run_status,
                    include_bezrealitky, scraped_rows, active_rows
                )
                VALUES (
                    :snapshot_date, :started_at, :completed_at, 'completed',
                    :include_bezrealitky, :scraped_rows, :active_rows
                )
                ON CONFLICT (started_at)
                DO UPDATE SET
                    snapshot_date = EXCLUDED.snapshot_date,
                    completed_at = EXCLUDED.completed_at,
                    scraped_rows = EXCLUDED.scraped_rows,
                    active_rows = EXCLUDED.active_rows
                RETURNING scrape_run_id
                """
            ),
            {
                "snapshot_date": snapshot_date,
                "started_at": scraped_at,
                "completed_at": scraped_at,
                "include_bezrealitky": bool((frame.get("source") == "bezrealitky").any()) if "source" in frame.columns else False,
                "scraped_rows": int(len(frame)),
                "active_rows": int((frame.get("exists_on_source", True) == True).sum()),
            },
        ).scalar_one()
        run_map[pd.Timestamp(scraped_at)] = run_id
    return run_map


def _refresh_fact_tables(conn):
    conn.execute(text("TRUNCATE TABLE listing_status_events RESTART IDENTITY"))
    conn.execute(text("TRUNCATE TABLE listing_snapshots RESTART IDENTITY"))


def _insert_snapshots_and_events(
    conn,
    history_df: pd.DataFrame,
    current_df: pd.DataFrame,
    listing_id_map: Dict[str, int],
    source_map: Dict[str, int],
    scrape_run_map: Dict[pd.Timestamp, int],
):
    ordered_dates = sorted(date for date in history_df["snapshot_date"].dropna().unique().tolist())
    history_lookup = {
        snapshot_date: history_df[history_df["snapshot_date"] == snapshot_date].copy()
        for snapshot_date in ordered_dates
    }

    for row in history_df.to_dict("records"):
        listing_id = listing_id_map.get(row.get("composite_id"))
        if listing_id is None:
            continue
        conn.execute(
            text(
                """
                INSERT INTO listing_snapshots (
                    listing_id, scrape_run_id, snapshot_date, scraped_at, exists_on_source, source_id,
                    title, property_type_code, property_type, transaction_type, layout_type, area_m2,
                    price_czk, price_per_m2_czk, previous_price_czk, price_change_czk, full_address,
                    street_address, district_name, borough_name, prague_zone, location_quality,
                    city_name, region_name, country_name, latitude, longitude, seller_type,
                    ownership_type, floor, energy_class, has_balcony, has_terrace, has_parking,
                    has_elevator, has_cellar, description, details_json, listing_duration_days,
                    removed_duration_days
                )
                VALUES (
                    :listing_id, :scrape_run_id, :snapshot_date, :scraped_at, :exists_on_source, :source_id,
                    :title, :property_type_code, :property_type, 'sale', :layout_type, :area_m2,
                    :price_czk, :price_per_m2_czk, :previous_price_czk, :price_change_czk, :full_address,
                    :street_address, :district_name, :borough_name, :prague_zone, :location_quality,
                    :city_name, :region_name, :country_name, :latitude, :longitude, :seller_type,
                    :ownership_type, :floor, :energy_class, :has_balcony, :has_terrace, :has_parking,
                    :has_elevator, :has_cellar, :description, CAST(:details_json AS JSONB), :listing_duration_days,
                    :removed_duration_days
                )
                """
            ),
            {
                "listing_id": listing_id,
                "scrape_run_id": scrape_run_map[pd.Timestamp(row["scraped_at"])],
                "snapshot_date": row.get("snapshot_date"),
                "scraped_at": row.get("scraped_at"),
                "exists_on_source": _bool_value(row.get("exists_on_source")),
                "source_id": source_map[row.get("source")],
                "title": row.get("title"),
                "property_type_code": _null_if_nan(row.get("property_type_code")),
                "property_type": _null_if_nan(row.get("property_type")),
                "layout_type": _null_if_nan(row.get("layout_type")),
                "area_m2": _null_if_nan(row.get("area_m2")),
                "price_czk": _null_if_nan(row.get("price_czk")),
                "price_per_m2_czk": _null_if_nan(row.get("price_per_m2_czk")),
                "previous_price_czk": _null_if_nan(row.get("previous_price_czk")),
                "price_change_czk": _null_if_nan(row.get("price_change_czk")),
                "full_address": _null_if_nan(row.get("full_address")),
                "street_address": _null_if_nan(row.get("street_address")),
                "district_name": _null_if_nan(row.get("district_name")),
                "borough_name": _null_if_nan(row.get("borough_name")),
                "prague_zone": _null_if_nan(row.get("prague_zone")),
                "location_quality": _null_if_nan(row.get("location_quality")) or "ok",
                "city_name": _null_if_nan(row.get("city_name")) or "Praha",
                "region_name": _null_if_nan(row.get("region_name")) or "Praha",
                "country_name": _null_if_nan(row.get("country_name")) or "Czech Republic",
                "latitude": _null_if_nan(row.get("latitude")),
                "longitude": _null_if_nan(row.get("longitude")),
                "seller_type": _null_if_nan(row.get("seller_type")),
                "ownership_type": _null_if_nan(row.get("ownership_type")),
                "floor": _null_if_nan(row.get("floor")),
                "energy_class": _null_if_nan(row.get("energy_class")),
                "has_balcony": _bool_value(row.get("has_balcony")),
                "has_terrace": _bool_value(row.get("has_terrace")),
                "has_parking": _bool_value(row.get("has_parking")),
                "has_elevator": _bool_value(row.get("has_elevator")),
                "has_cellar": _bool_value(row.get("has_cellar")),
                "description": _null_if_nan(row.get("description")),
                "details_json": _null_if_nan(row.get("details_json")),
                "listing_duration_days": _null_if_nan(row.get("listing_duration_days")),
                "removed_duration_days": _null_if_nan(row.get("removed_duration_days")),
            },
        )

    for index, snapshot_date in enumerate(ordered_dates):
        if index == 0:
            continue
        previous_date = ordered_dates[index - 1]
        current_snapshot = history_lookup[snapshot_date]
        previous_snapshot = history_lookup[previous_date]
        curr_active = current_snapshot[current_snapshot["exists_on_source"] == True].set_index("composite_id", drop=False)
        prev_active = previous_snapshot[previous_snapshot["exists_on_source"] == True].set_index("composite_id", drop=False)
        current_ids = set(curr_active.index.tolist())
        previous_ids = set(prev_active.index.tolist())

        event_rows = []
        for composite_id in sorted(current_ids - previous_ids):
            row = curr_active.loc[composite_id]
            event_rows.append(("new", None, row.get("price_czk"), None, None, row))
        for composite_id in sorted(previous_ids - current_ids):
            row = prev_active.loc[composite_id]
            event_rows.append(("removed", row.get("price_czk"), None, None, None, row))
        for composite_id in sorted(current_ids & previous_ids):
            current_row = curr_active.loc[composite_id]
            previous_row = prev_active.loc[composite_id]
            current_price = current_row.get("price_czk")
            previous_price = previous_row.get("price_czk")
            if pd.notna(current_price) and pd.notna(previous_price) and float(current_price) != float(previous_price):
                change = float(current_price) - float(previous_price)
                event_rows.append(
                    (
                        "price_increase" if change > 0 else "price_reduction",
                        previous_price,
                        current_price,
                        change,
                        ((change / previous_price) * 100.0) if previous_price else None,
                        current_row,
                    )
                )

        scrape_run_id = int(current_snapshot["scraped_at"].dropna().map(lambda value: scrape_run_map[pd.Timestamp(value)]).iloc[0])
        for event_type, previous_price, current_price, change_amount, change_pct, row in event_rows:
            listing_id = listing_id_map.get(row.get("composite_id"))
            if listing_id is None:
                continue
            conn.execute(
                text(
                    """
                    INSERT INTO listing_status_events (
                        listing_id, scrape_run_id, snapshot_date, previous_snapshot_date,
                        event_type, event_at, source_id, previous_price_czk, current_price_czk,
                        price_change_czk, price_change_pct, details
                    )
                    VALUES (
                        :listing_id, :scrape_run_id, :snapshot_date, :previous_snapshot_date,
                        :event_type, :event_at, :source_id, :previous_price_czk, :current_price_czk,
                        :price_change_czk, :price_change_pct, CAST(:details AS JSONB)
                    )
                    """
                ),
                {
                    "listing_id": listing_id,
                    "scrape_run_id": scrape_run_id,
                    "snapshot_date": snapshot_date,
                    "previous_snapshot_date": previous_date,
                    "event_type": event_type,
                    "event_at": current_snapshot["scraped_at"].dropna().iloc[0],
                    "source_id": source_map[row.get("source")],
                    "previous_price_czk": previous_price,
                    "current_price_czk": current_price,
                    "price_change_czk": change_amount,
                    "price_change_pct": change_pct,
                    "details": json.dumps(
                        {
                            "composite_id": row.get("composite_id"),
                            "district_name": _null_if_nan(row.get("district_name")),
                            "borough_name": _null_if_nan(row.get("borough_name")),
                            "title": _null_if_nan(row.get("title")),
                        },
                        ensure_ascii=False,
                    ),
                },
            )


def _refresh_district_reference(conn, current_df: pd.DataFrame):
    conn.execute(text("TRUNCATE TABLE district_reference RESTART IDENTITY"))
    unique_rows = (
        current_df[["district_name", "borough_name", "prague_zone"]]
        .dropna(how="all")
        .drop_duplicates()
        .to_dict("records")
    )
    for row in unique_rows:
        conn.execute(
            text(
                """
                INSERT INTO district_reference (district_name, borough_name, prague_zone)
                VALUES (:district_name, :borough_name, :prague_zone)
                ON CONFLICT (district_name, borough_name, prague_zone) DO NOTHING
                """
            ),
            {
                "district_name": row.get("district_name") or "Praha - Ostatní",
                "borough_name": row.get("borough_name") or "Praha - Ostatní",
                "prague_zone": row.get("prague_zone") or row.get("district_name") or "Praha - Ostatní",
            },
        )


def import_clean_csvs_to_postgres() -> Tuple[Dict[str, Path], Dict[str, int]]:
    if engine.dialect.name != "postgresql":
        raise RuntimeError(
            f"Current DATABASE_URL points to {engine.dialect.name!r}. Set DATABASE_URL to a PostgreSQL database before importing."
        )

    cleaned_paths = clean_existing_csv_exports()
    processed_df = _read_csv(cleaned_paths["listings_processed"])
    history_df = _read_csv(cleaned_paths["listing_history"])
    removed_df = _read_csv(cleaned_paths["removed_listings"])

    processed_df = _reparse_frame(processed_df)
    history_df = _reparse_frame(history_df)
    removed_df = _reparse_frame(removed_df)

    current_df = _prepare_current_state(processed_df, removed_df)
    history_df = _prepare_history(history_df, current_df)

    apply_postgres_schema()

    with engine.begin() as conn:
        source_map = _seed_sources(conn)
        listing_id_map = _upsert_listings(conn, current_df, source_map)
        scrape_run_map = _insert_scrape_runs(conn, history_df)
        _refresh_fact_tables(conn)
        _insert_snapshots_and_events(conn, history_df, current_df, listing_id_map, source_map, scrape_run_map)
        _refresh_district_reference(conn, current_df)

    summary = {
        "clean_current_rows": int(len(current_df)),
        "clean_history_rows": int(len(history_df)),
        "clean_removed_rows": int(len(removed_df)),
        "sources_loaded": int(len(current_df["source"].dropna().unique())) if not current_df.empty and "source" in current_df.columns else 0,
        "listings_loaded": int(len(current_df)),
        "scrape_runs_loaded": int(len(history_df["scraped_at"].dropna().unique())) if not history_df.empty and "scraped_at" in history_df.columns else 0,
    }
    logger.info("Imported cleaned CSVs into PostgreSQL | %s", summary)
    return cleaned_paths, summary


if __name__ == "__main__":
    cleaned_paths, summary = import_clean_csvs_to_postgres()
    print("Cleaned CSV outputs:")
    for key, value in cleaned_paths.items():
        print(f"  {key}: {value}")
    print("Import summary:")
    for key, value in summary.items():
        print(f"  {key}: {value}")
