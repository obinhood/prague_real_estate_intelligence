from __future__ import annotations

from dataclasses import dataclass
import math
from pathlib import Path
from typing import Dict, Optional

import pandas as pd

from src.db.io import read_postgres_current_state_df, read_postgres_history_df
from src.utils.process_csv import NEIGHBOURHOOD_TO_ZONE, deduce_district_and_zone, is_valid_prague_zone


NUMERIC_COLUMNS = [
    "price_czk",
    "price_per_m2_czk",
    "area_m2",
    "listing_duration_days",
    "removed_duration_days",
    "previous_price_czk",
    "price_change_czk",
    "latitude",
    "longitude",
]

DATETIME_COLUMNS = [
    "first_seen_at",
    "last_seen_at",
    "removed_at",
    "scraped_at",
]

BOOLEAN_COLUMNS = [
    "is_active",
    "is_removed",
    "exists_on_source",
    "price_changed",
    "has_balcony",
    "has_parking",
    "has_terrace",
    "has_elevator",
    "has_cellar",
]


@dataclass
class MarketDataBundle:
    current_df: pd.DataFrame
    history_df: pd.DataFrame
    removed_df: pd.DataFrame


INVALID_BOROUGH_VALUES = {"", "Praha", "Praha - Ostatní"}
PRAGUE_LATITUDE_REFERENCE = 50.08


def _read_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path) if Path(path).exists() else pd.DataFrame()


def _normalize_bool(series: pd.Series) -> pd.Series:
    if series.dtype == bool:
        return series
    mapping = {
        "true": True,
        "false": False,
        "1": True,
        "0": False,
        "yes": True,
        "no": False,
    }
    return series.map(lambda value: mapping.get(str(value).strip().lower(), value) if pd.notna(value) else value)


def _prepare_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    prepared = frame.copy()

    # --- district / borough sanity ----------------------------------------
    # district_name must be "Praha N" (the city zone).
    # borough_name must be a neighbourhood name (Smíchov, Vinohrady …).
    # The old fallback of copying district_name → borough_name caused every
    # "Praha 5" to appear as the borough name, creating the exact confusion
    # reported.  Instead we leave borough_name as None when it is missing and
    # let the data-quality flags surface the gap, rather than masking it.

    # If district_name is present but does not start with "Praha " (i.e. it
    # somehow ended up holding a neighbourhood name), try to recover it from
    # prague_zone which is always the "Praha N" value.
    if "district_name" in prepared.columns and "prague_zone" in prepared.columns:
        bad_district_mask = (
            prepared["district_name"].notna()
            & ~prepared["district_name"].astype(str).str.match(r"^Praha\s+\d+$", na=False)
            & (prepared["district_name"].astype(str) != "Praha - Ostatní")
        )
        if bad_district_mask.any():
            # Before overwriting, rescue the neighbourhood name (e.g. "Smíchov")
            # that the scraper stored in district_name.
            if "borough_name" not in prepared.columns:
                prepared["borough_name"] = None
            rescue_mask = bad_district_mask & prepared["borough_name"].isna()
            prepared.loc[rescue_mask, "borough_name"] = prepared.loc[rescue_mask, "district_name"]
            prepared.loc[bad_district_mask, "district_name"] = prepared.loc[bad_district_mask, "prague_zone"]

    # Ensure borough_name column always exists (may be None/NaN — that is
    # correct and honest; do NOT fall back to copying district_name).
    if "borough_name" not in prepared.columns:
        prepared["borough_name"] = None
    if "district_name" not in prepared.columns:
        prepared["district_name"] = None
    if "prague_zone" not in prepared.columns:
        prepared["prague_zone"] = None
    if "region_name" not in prepared.columns:
        prepared["region_name"] = None

    if "location_quality" not in prepared.columns:
        prepared["location_quality"] = "ok"
    for column in NUMERIC_COLUMNS:
        if column in prepared.columns:
            prepared[column] = pd.to_numeric(prepared[column], errors="coerce")
    for column in DATETIME_COLUMNS:
        if column in prepared.columns:
            prepared[column] = pd.to_datetime(prepared[column], errors="coerce")
    for column in BOOLEAN_COLUMNS:
        if column in prepared.columns:
            prepared[column] = _normalize_bool(prepared[column])
    if "snapshot_date" in prepared.columns:
        prepared["snapshot_date"] = pd.to_datetime(prepared["snapshot_date"], errors="coerce").dt.date
    elif "scraped_at" in prepared.columns:
        prepared["snapshot_date"] = pd.to_datetime(prepared["scraped_at"], errors="coerce").dt.date
    elif "last_seen_at" in prepared.columns:
        prepared["snapshot_date"] = pd.to_datetime(prepared["last_seen_at"], errors="coerce").dt.date
    prepared = _repair_borough_placeholders(prepared)
    return prepared


def load_market_data(
    current_path: str = "data/listings_processed.csv",
    history_path: str = "data/listing_history.csv",
    removed_path: str = "data/removed_listings.csv",
) -> MarketDataBundle:
    db_current = _prepare_frame(read_postgres_current_state_df())
    db_history = _prepare_frame(read_postgres_history_df())
    if not db_current.empty or not db_history.empty:
        removed_df = db_current[db_current.get("is_removed", False) == True].copy() if "is_removed" in db_current.columns else pd.DataFrame()
        return MarketDataBundle(
            current_df=db_current,
            history_df=db_history,
            removed_df=removed_df,
        )
    return MarketDataBundle(
        current_df=_prepare_frame(_read_csv(current_path)),
        history_df=_prepare_frame(_read_csv(history_path)),
        removed_df=_prepare_frame(_read_csv(removed_path)),
    )


def _is_invalid_borough(value) -> bool:
    if pd.isna(value):
        return True
    return str(value).strip() in INVALID_BOROUGH_VALUES


def _repair_borough_placeholders(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "borough_name" not in frame.columns:
        return frame

    prepared = frame.copy()
    if "region_name" not in prepared.columns:
        prepared["region_name"] = None

    prague_mask = prepared["region_name"].fillna("Praha").astype(str).str.contains("Praha", case=False, na=False)
    unresolved_mask = prepared["borough_name"].map(_is_invalid_borough)
    repair_mask = prague_mask & unresolved_mask
    if not repair_mask.any():
        prepared.loc[unresolved_mask, "borough_name"] = pd.NA
        return prepared

    for idx, row in prepared.loc[repair_mask].iterrows():
        borough_name, district_name, prague_zone, _ = deduce_district_and_zone(
            row.get("full_address") or row.get("street_address"),
            row.get("title"),
        )
        if borough_name and borough_name not in INVALID_BOROUGH_VALUES:
            prepared.at[idx, "borough_name"] = borough_name
            if _is_invalid_borough(row.get("district_name")) or pd.isna(row.get("district_name")):
                prepared.at[idx, "district_name"] = district_name
            if _is_invalid_borough(row.get("prague_zone")) or pd.isna(row.get("prague_zone")):
                prepared.at[idx, "prague_zone"] = prague_zone
        else:
            prepared.at[idx, "borough_name"] = pd.NA

    prague_rows = prepared["region_name"].fillna("Praha").astype(str).str.contains("Praha", case=False, na=False)
    for idx, row in prepared.loc[prague_rows].iterrows():
        borough_name = row.get("borough_name")
        expected_zone = NEIGHBOURHOOD_TO_ZONE.get(borough_name) if pd.notna(borough_name) else None
        district_name = row.get("district_name")
        prague_zone = row.get("prague_zone")

        if expected_zone:
            if district_name != expected_zone:
                prepared.at[idx, "district_name"] = expected_zone
            if prague_zone != expected_zone:
                prepared.at[idx, "prague_zone"] = expected_zone
            continue

        if pd.notna(district_name) and district_name != "Praha - Ostatní" and not is_valid_prague_zone(district_name):
            prepared.at[idx, "district_name"] = pd.NA
        if pd.notna(prague_zone) and prague_zone != "Praha - Ostatní" and not is_valid_prague_zone(prague_zone):
            prepared.at[idx, "prague_zone"] = pd.NA
    return prepared


def _apply_common_filters(frame: pd.DataFrame, filters: Optional[Dict] = None) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    filters = filters or {}
    out = frame.copy()
    if filters.get("sources") and "source" in out.columns:
        out = out[out["source"].isin(filters["sources"])]
    if filters.get("property_types") and "property_type" in out.columns:
        out = out[out["property_type"].isin(filters["property_types"])]
    if filters.get("districts") and "district_name" in out.columns:
        out = out[out["district_name"].isin(filters["districts"])]
    if filters.get("boroughs") and "borough_name" in out.columns:
        out = out[out["borough_name"].isin(filters["boroughs"])]
    if filters.get("seller_types") and "seller_type" in out.columns:
        out = out[out["seller_type"].isin(filters["seller_types"])]
    if filters.get("regions") and "region_name" in out.columns:
        out = out[out["region_name"].isin(filters["regions"])]
    if filters.get("search"):
        query = str(filters["search"]).strip().lower()
        if query:
            title = out.get("title", pd.Series("", index=out.index)).fillna("").str.lower()
            address = out.get("full_address", pd.Series("", index=out.index)).fillna("").str.lower()
            out = out[title.str.contains(query, na=False) | address.str.contains(query, na=False)]
    if "price_czk" in out.columns:
        minimum, maximum = filters.get("price_range", (None, None))
        if minimum is not None:
            out = out[out["price_czk"].fillna(0) >= minimum]
        if maximum is not None:
            out = out[out["price_czk"].fillna(0) <= maximum]
    if "area_m2" in out.columns:
        minimum, maximum = filters.get("size_range", (None, None))
        if minimum is not None:
            out = out[out["area_m2"].fillna(0) >= minimum]
        if maximum is not None:
            out = out[out["area_m2"].fillna(0) <= maximum]
    return out


def _group_by_geo(frame: pd.DataFrame, grain: str = "borough") -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame()
    if grain == "district":
        group_cols = ["district_name", "region_name"]
        label_col = "district_name"
    else:
        group_cols = ["borough_name", "district_name", "region_name"]
        label_col = "borough_name"
    valid = frame.dropna(subset=[label_col]).copy()
    if label_col == "borough_name":
        valid = valid[~valid["borough_name"].map(_is_invalid_borough)]
    if valid.empty:
        return pd.DataFrame()
    return (
        valid.groupby(group_cols, as_index=False, dropna=False)
        .agg(
            active_listings=("composite_id", "count"),
            median_price_czk=("price_czk", "median"),
            average_price_czk=("price_czk", "mean"),
            median_price_per_m2_czk=("price_per_m2_czk", "median"),
            total_market_value_czk=("price_czk", "sum"),
            average_days_on_market=("listing_duration_days", "mean"),
            latitude=("latitude", "median"),
            longitude=("longitude", "median"),
        )
    )


def _apply_date_range(frame: pd.DataFrame, filters: Optional[Dict] = None) -> pd.DataFrame:
    if frame.empty or "snapshot_date" not in frame.columns:
        return frame.copy()
    filters = filters or {}
    out = frame.copy()
    date_range = filters.get("date_range")
    if date_range:
        start_date, end_date = date_range
        if start_date:
            out = out[out["snapshot_date"] >= start_date]
        if end_date:
            out = out[out["snapshot_date"] <= end_date]
    return out


def _safe_percent_change(current_value, previous_value):
    if previous_value in (None, 0) or pd.isna(previous_value):
        return None
    return round(((current_value - previous_value) / previous_value) * 100.0, 2)


def _summary_delta(current_value, previous_value):
    if previous_value is None or pd.isna(previous_value):
        return {"current": current_value, "previous": previous_value, "delta": None, "pct_change": None}
    return {
        "current": current_value,
        "previous": previous_value,
        "delta": current_value - previous_value,
        "pct_change": _safe_percent_change(current_value, previous_value),
    }


def _get_filtered_context(bundle: MarketDataBundle, filters: Optional[Dict] = None) -> Dict[str, pd.DataFrame]:
    current_df = _apply_common_filters(bundle.current_df, filters)
    history_df = _apply_date_range(_apply_common_filters(bundle.history_df, filters), filters)
    removed_df = _apply_common_filters(bundle.removed_df, filters)

    current_active = current_df[current_df.get("is_active", False) == True].copy() if "is_active" in current_df.columns else current_df.copy()
    distinct_dates = sorted(date for date in history_df.get("snapshot_date", pd.Series(dtype="object")).dropna().unique().tolist())
    latest_date = distinct_dates[-1] if distinct_dates else None
    previous_date = distinct_dates[-2] if len(distinct_dates) >= 2 else None

    current_snapshot = pd.DataFrame()
    previous_snapshot = pd.DataFrame()
    if latest_date is not None and "snapshot_date" in history_df.columns:
        current_snapshot = history_df[(history_df["snapshot_date"] == latest_date) & (history_df["exists_on_source"] == True)].copy()
    if previous_date is not None and "snapshot_date" in history_df.columns:
        previous_snapshot = history_df[(history_df["snapshot_date"] == previous_date) & (history_df["exists_on_source"] == True)].copy()

    return {
        "current_df": current_df,
        "current_active": current_active,
        "history_df": history_df,
        "removed_df": removed_df,
        "latest_snapshot": current_snapshot,
        "previous_snapshot": previous_snapshot,
        "latest_date": latest_date,
        "previous_date": previous_date,
    }


def _movement_frame(current_snapshot: pd.DataFrame, previous_snapshot: pd.DataFrame) -> pd.DataFrame:
    if current_snapshot.empty and previous_snapshot.empty:
        return pd.DataFrame()
    prev = previous_snapshot.set_index("composite_id", drop=False) if not previous_snapshot.empty else pd.DataFrame()
    curr = current_snapshot.set_index("composite_id", drop=False) if not current_snapshot.empty else pd.DataFrame()
    prev_ids = set(prev.index.tolist()) if not prev.empty else set()
    curr_ids = set(curr.index.tolist()) if not curr.empty else set()

    records = []
    for listing_id in sorted(curr_ids - prev_ids):
        row = curr.loc[listing_id].to_dict()
        row["movement"] = "new"
        row["price_change_amount"] = None
        row["price_change_percentage"] = None
        records.append(row)
    for listing_id in sorted(prev_ids - curr_ids):
        row = prev.loc[listing_id].to_dict()
        row["movement"] = "removed"
        row["price_change_amount"] = None
        row["price_change_percentage"] = None
        records.append(row)
    for listing_id in sorted(curr_ids & prev_ids):
        current_row = curr.loc[listing_id].to_dict()
        previous_row = prev.loc[listing_id].to_dict()
        current_price = current_row.get("price_czk")
        previous_price = previous_row.get("price_czk")
        if pd.notna(current_price) and pd.notna(previous_price) and float(current_price) != float(previous_price):
            change_amount = float(current_price) - float(previous_price)
            current_row["movement"] = "price_increase" if change_amount > 0 else "price_reduction"
            current_row["price_change_amount"] = change_amount
            current_row["price_change_percentage"] = _safe_percent_change(float(current_price), float(previous_price))
            records.append(current_row)
    return pd.DataFrame(records)


def get_market_overview(bundle: MarketDataBundle, filters: Optional[Dict] = None) -> Dict:
    context = _get_filtered_context(bundle, filters)
    current_active = context["current_active"]
    previous_snapshot = context["previous_snapshot"]
    movement_df = _movement_frame(context["latest_snapshot"], previous_snapshot) if not previous_snapshot.empty else pd.DataFrame()

    previous_active_count = len(previous_snapshot) if not previous_snapshot.empty else None
    previous_total_value = previous_snapshot["price_czk"].sum() if "price_czk" in previous_snapshot.columns and not previous_snapshot.empty else None
    previous_median_price = previous_snapshot["price_czk"].median() if "price_czk" in previous_snapshot.columns and not previous_snapshot.empty else None
    previous_average_price = previous_snapshot["price_czk"].mean() if "price_czk" in previous_snapshot.columns and not previous_snapshot.empty else None
    previous_median_ppsqm = previous_snapshot["price_per_m2_czk"].median() if "price_per_m2_czk" in previous_snapshot.columns and not previous_snapshot.empty else None

    return {
        "latest_snapshot_date": context["latest_date"],
        "previous_snapshot_date": context["previous_date"],
        "active_listings": _summary_delta(len(current_active), previous_active_count),
        "total_market_value": _summary_delta(current_active["price_czk"].sum() if "price_czk" in current_active.columns else None, previous_total_value),
        "median_listing_price": _summary_delta(current_active["price_czk"].median() if "price_czk" in current_active.columns else None, previous_median_price),
        "average_listing_price": _summary_delta(current_active["price_czk"].mean() if "price_czk" in current_active.columns else None, previous_average_price),
        "median_price_per_sqm": _summary_delta(current_active["price_per_m2_czk"].median() if "price_per_m2_czk" in current_active.columns else None, previous_median_ppsqm),
        "new_listings": int((movement_df.get("movement") == "new").sum()) if not movement_df.empty else 0,
        "removed_listings": int((movement_df.get("movement") == "removed").sum()) if not movement_df.empty else 0,
        "price_increases": int((movement_df.get("movement") == "price_increase").sum()) if not movement_df.empty else 0,
        "price_reductions": int((movement_df.get("movement") == "price_reduction").sum()) if not movement_df.empty else 0,
        "days_on_market_avg": current_active["listing_duration_days"].mean() if "listing_duration_days" in current_active.columns else None,
        "days_on_market_median": current_active["listing_duration_days"].median() if "listing_duration_days" in current_active.columns else None,
    }


def get_market_timeseries(bundle: MarketDataBundle, filters: Optional[Dict] = None) -> pd.DataFrame:
    context = _get_filtered_context(bundle, filters)
    history_df = context["history_df"]
    if history_df.empty:
        return pd.DataFrame()
    active = history_df[history_df["exists_on_source"] == True].copy()
    if active.empty:
        return pd.DataFrame()

    trend = (
        active.groupby("snapshot_date", as_index=False)
        .agg(
            active_listings=("composite_id", "count"),
            total_market_value_czk=("price_czk", "sum"),
            median_price_czk=("price_czk", "median"),
            average_price_czk=("price_czk", "mean"),
            median_price_per_m2_czk=("price_per_m2_czk", "median"),
        )
        .sort_values("snapshot_date")
    )

    movement_rows = []
    ordered_dates = trend["snapshot_date"].tolist()
    for index, snapshot_date in enumerate(ordered_dates):
        current_snapshot = active[active["snapshot_date"] == snapshot_date].copy()
        previous_snapshot = active[active["snapshot_date"] == ordered_dates[index - 1]].copy() if index > 0 else pd.DataFrame()
        movement_df = _movement_frame(current_snapshot, previous_snapshot) if index > 0 else pd.DataFrame()
        movement_rows.append(
            {
                "snapshot_date": snapshot_date,
                "new_listings": int((movement_df.get("movement") == "new").sum()) if not movement_df.empty else 0,
                "removed_listings": int((movement_df.get("movement") == "removed").sum()) if not movement_df.empty else 0,
                "price_increases": int((movement_df.get("movement") == "price_increase").sum()) if not movement_df.empty else 0,
                "price_reductions": int((movement_df.get("movement") == "price_reduction").sum()) if not movement_df.empty else 0,
            }
        )
    trend = trend.merge(pd.DataFrame(movement_rows), on="snapshot_date", how="left")
    return trend


def get_market_districts(bundle: MarketDataBundle, filters: Optional[Dict] = None) -> pd.DataFrame:
    context = _get_filtered_context(bundle, filters)
    current_active = context["current_active"]
    previous_snapshot = context["previous_snapshot"]
    if current_active.empty or "district_name" not in current_active.columns:
        return pd.DataFrame()

    current_grouped = (
        current_active.groupby(["district_name", "borough_name", "region_name"], as_index=False, dropna=False)
        .agg(
            active_listings=("composite_id", "count"),
            total_market_value_czk=("price_czk", "sum"),
            median_price_czk=("price_czk", "median"),
            median_price_per_m2_czk=("price_per_m2_czk", "median"),
            average_days_on_market=("listing_duration_days", "mean"),
        )
    )
    if previous_snapshot.empty:
        current_grouped["active_listings_delta"] = None
        return current_grouped.sort_values(["active_listings", "total_market_value_czk"], ascending=False)

    prev_grouped = previous_snapshot.groupby(["district_name", "region_name"], as_index=False).agg(previous_active_listings=("composite_id", "count"))
    merged = current_grouped.merge(prev_grouped, on=["district_name", "region_name"], how="left")
    merged["active_listings_delta"] = merged["active_listings"] - merged["previous_active_listings"]
    return merged.sort_values(["active_listings", "total_market_value_czk"], ascending=False)


def get_market_boroughs(bundle: MarketDataBundle, filters: Optional[Dict] = None) -> pd.DataFrame:
    context = _get_filtered_context(bundle, filters)
    current_active = context["current_active"]
    grouped = _group_by_geo(current_active, "borough")
    if grouped.empty:
        return grouped
    return grouped.sort_values(["median_price_czk", "active_listings"], ascending=False)


def get_market_map_data(bundle: MarketDataBundle, filters: Optional[Dict] = None, grain: str = "borough") -> pd.DataFrame:
    context = _get_filtered_context(bundle, filters)
    current_active = context["current_active"]
    grouped = _group_by_geo(current_active, grain)
    if grouped.empty:
        return grouped
    grouped = grouped.dropna(subset=["latitude", "longitude"])
    return grouped.sort_values(["median_price_czk", "active_listings"], ascending=False)


def _project_mercator(lat: float, lon: float):
    # Local equirectangular projection so Prague retains a sensible x/y aspect
    # for hex aggregation without depending on a GIS library.
    scale = math.cos(math.radians(PRAGUE_LATITUDE_REFERENCE))
    return float(lon) * scale, float(lat)


def _inverse_mercator(x: float, y: float):
    scale = math.cos(math.radians(PRAGUE_LATITUDE_REFERENCE))
    return float(y), float(x) / scale


def _hex_round(q: float, r: float):
    x = q
    z = r
    y = -x - z
    rx = round(x)
    ry = round(y)
    rz = round(z)
    x_diff = abs(rx - x)
    y_diff = abs(ry - y)
    z_diff = abs(rz - z)
    if x_diff > y_diff and x_diff > z_diff:
        rx = -ry - rz
    elif y_diff > z_diff:
        ry = -rx - rz
    else:
        rz = -rx - ry
    return int(rx), int(rz)


def _mode_or_none(series: pd.Series):
    cleaned = series.dropna()
    if cleaned.empty:
        return None
    modes = cleaned.mode()
    if modes.empty:
        return None
    return modes.iloc[0]


def get_market_hexagons(bundle: MarketDataBundle, filters: Optional[Dict] = None, grid_size: int = 18):
    context = _get_filtered_context(bundle, filters)
    current_active = context["current_active"]
    required_columns = {"latitude", "longitude"}
    if current_active.empty or not required_columns.issubset(current_active.columns):
        return pd.DataFrame(), {"type": "FeatureCollection", "features": []}

    frame = current_active.dropna(subset=["latitude", "longitude"]).copy()
    if frame.empty:
        return pd.DataFrame(), {"type": "FeatureCollection", "features": []}

    projected = frame.apply(lambda row: _project_mercator(row["latitude"], row["longitude"]), axis=1, result_type="expand")
    frame["hex_x"] = projected[0]
    frame["hex_y"] = projected[1]

    width = max(frame["hex_x"].max() - frame["hex_x"].min(), 1e-6)
    height = max(frame["hex_y"].max() - frame["hex_y"].min(), 1e-6)
    size = max(width, height) / max(int(grid_size), 1)
    size = max(size, 1e-4)
    sqrt3 = math.sqrt(3)

    def _assign_hex(row):
        q = ((sqrt3 / 3.0) * row["hex_x"] - (1.0 / 3.0) * row["hex_y"]) / size
        r = ((2.0 / 3.0) * row["hex_y"]) / size
        return _hex_round(q, r)

    axial = frame.apply(_assign_hex, axis=1, result_type="expand")
    frame["hex_q"] = axial[0]
    frame["hex_r"] = axial[1]
    frame["hex_id"] = frame["hex_q"].astype(str) + ":" + frame["hex_r"].astype(str)

    grouped = (
        frame.groupby("hex_id", as_index=False)
        .agg(
            active_listings=("composite_id", "count"),
            median_price_czk=("price_czk", "median"),
            average_price_czk=("price_czk", "mean"),
            median_price_per_m2_czk=("price_per_m2_czk", "median"),
            total_market_value_czk=("price_czk", "sum"),
            average_days_on_market=("listing_duration_days", "mean"),
            district_name=("district_name", _mode_or_none),
            borough_name=("borough_name", _mode_or_none),
            region_name=("region_name", _mode_or_none),
            latitude=("latitude", "median"),
            longitude=("longitude", "median"),
            hex_q=("hex_q", "first"),
            hex_r=("hex_r", "first"),
        )
        .sort_values(["active_listings", "median_price_czk"], ascending=False)
        .reset_index(drop=True)
    )

    features = []
    for row in grouped.itertuples(index=False):
        center_x = size * sqrt3 * (row.hex_q + (row.hex_r / 2.0))
        center_y = size * 1.5 * row.hex_r
        corners = []
        for corner_index in range(6):
            angle = math.radians((60 * corner_index) - 30)
            corner_x = center_x + (size * math.cos(angle))
            corner_y = center_y + (size * math.sin(angle))
            corner_lat, corner_lon = _inverse_mercator(corner_x, corner_y)
            corners.append([corner_lon, corner_lat])
        corners.append(corners[0])
        features.append(
            {
                "type": "Feature",
                "properties": {
                    "hex_id": row.hex_id,
                    "district_name": row.district_name,
                    "borough_name": row.borough_name,
                    "region_name": row.region_name,
                },
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [corners],
                },
            }
        )

    return grouped, {"type": "FeatureCollection", "features": features}


def get_source_inventory(bundle: MarketDataBundle, filters: Optional[Dict] = None) -> pd.DataFrame:
    context = _get_filtered_context(bundle, filters)
    current_active = context["current_active"]
    if current_active.empty or "source" not in current_active.columns:
        return pd.DataFrame()
    grouped = (
        current_active.groupby("source", as_index=False)
        .agg(
            active_listings=("composite_id", "count"),
            total_market_value_czk=("price_czk", "sum"),
            median_price_czk=("price_czk", "median"),
            median_price_per_m2_czk=("price_per_m2_czk", "median"),
        )
        .sort_values("active_listings", ascending=False)
    )
    return grouped


def get_market_price_movements(bundle: MarketDataBundle, filters: Optional[Dict] = None) -> pd.DataFrame:
    context = _get_filtered_context(bundle, filters)
    if context["previous_snapshot"].empty:
        return pd.DataFrame()
    movement_df = _movement_frame(context["latest_snapshot"], context["previous_snapshot"])
    if movement_df.empty:
        return movement_df
    columns = [
        "movement",
        "composite_id",
        "source",
        "property_type",
        "district_name",
        "borough_name",
        "title",
        "price_czk",
        "price_change_amount",
        "price_change_percentage",
        "property_link",
    ]
    return movement_df[[column for column in columns if column in movement_df.columns]].sort_values(["movement", "price_change_amount"], ascending=[True, False])


def get_active_listings(bundle: MarketDataBundle, filters: Optional[Dict] = None) -> pd.DataFrame:
    context = _get_filtered_context(bundle, filters)
    current_active = context["current_active"]
    if current_active.empty:
        return current_active
    sort_columns = [column for column in ["price_czk", "listing_duration_days"] if column in current_active.columns]
    if sort_columns:
        current_active = current_active.sort_values(sort_columns, ascending=[False, False][: len(sort_columns)])
    return current_active


def get_listing_history(bundle: MarketDataBundle, listing_id: str) -> pd.DataFrame:
    history_df = bundle.history_df.copy()
    if history_df.empty or "composite_id" not in history_df.columns:
        return pd.DataFrame()
    filtered = history_df[history_df["composite_id"] == listing_id].copy()
    if filtered.empty:
        return filtered
    return filtered.sort_values(["scraped_at", "exists_on_source"])


def get_data_quality(bundle: MarketDataBundle, filters: Optional[Dict] = None) -> Dict:
    context = _get_filtered_context(bundle, filters)
    current_df = context["current_df"]
    current_active = context["current_active"]
    if current_df.empty:
        return {
            "total_records": 0,
            "active_records": 0,
            "missing_price": 0,
            "missing_area": 0,
            "missing_price_per_sqm": 0,
            "missing_coordinates": 0,
            "location_issues": 0,
            "location_issue_examples": [],
        }

    issue_mask = current_df.get("location_quality", pd.Series("ok", index=current_df.index)).fillna("ok") != "ok"
    examples = current_df.loc[issue_mask, [col for col in ["title", "district_name", "borough_name", "location_quality"] if col in current_df.columns]].head(10)
    return {
        "total_records": int(len(current_df)),
        "active_records": int(len(current_active)),
        "missing_price": int(current_df.get("price_czk", pd.Series(index=current_df.index)).isna().sum()),
        "missing_area": int(current_df.get("area_m2", pd.Series(index=current_df.index)).isna().sum()),
        "missing_price_per_sqm": int(current_df.get("price_per_m2_czk", pd.Series(index=current_df.index)).isna().sum()),
        "missing_coordinates": int(
            pd.concat(
                [
                    current_df.get("latitude", pd.Series(index=current_df.index)).isna(),
                    current_df.get("longitude", pd.Series(index=current_df.index)).isna(),
                ],
                axis=1,
            ).any(axis=1).sum()
        ),
        "location_issues": int(issue_mask.sum()),
        "location_issue_examples": examples.to_dict(orient="records"),
    }
