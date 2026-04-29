from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Optional

import pandas as pd


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
            prepared.loc[bad_district_mask, "district_name"] = prepared.loc[bad_district_mask, "prague_zone"]

    # Ensure borough_name column always exists (may be None/NaN — that is
    # correct and honest; do NOT fall back to copying district_name).
    if "borough_name" not in prepared.columns:
        prepared["borough_name"] = None

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
    return prepared


def load_market_data(
    current_path: str = "data/listings_processed.csv",
    history_path: str = "data/listing_history.csv",
    removed_path: str = "data/removed_listings.csv",
) -> MarketDataBundle:
    return MarketDataBundle(
        current_df=_prepare_frame(_read_csv(current_path)),
        history_df=_prepare_frame(_read_csv(history_path)),
        removed_df=_prepare_frame(_read_csv(removed_path)),
    )


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
        current_active.groupby(["district_name", "borough_name"], as_index=False, dropna=False)
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

    prev_grouped = previous_snapshot.groupby("district_name", as_index=False).agg(previous_active_listings=("composite_id", "count"))
    merged = current_grouped.merge(prev_grouped, on="district_name", how="left")
    merged["active_listings_delta"] = merged["active_listings"] - merged["previous_active_listings"]
    return merged.sort_values(["active_listings", "total_market_value_czk"], ascending=False)


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
