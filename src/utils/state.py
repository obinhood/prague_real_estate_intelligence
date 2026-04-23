import pandas as pd
from src.utils.logger import get_logger

logger = get_logger("state")

CURRENT_COLS = [
    "composite_id", "url_id", "source", "property_search_type", "property_type_code", "property_type",
    "url", "property_link", "title", "layout_type", "area_m2", "price_czk", "price_per_m2_czk",
    "full_address", "street_address", "district_name", "prague_zone", "city_name", "region_name", "country_name",
    "latitude", "longitude", "seller_type", "floor", "ownership_type", "energy_class",
    "description", "details_json", "has_balcony", "has_parking", "has_terrace", "has_elevator", "has_cellar",
    "first_seen_at", "last_seen_at", "is_active", "is_removed", "removed_at",
    "price_changed", "previous_price_czk", "price_change_czk", "listing_duration_days", "removed_duration_days"
]

HISTORY_COLS = [
    "composite_id", "url_id", "source", "property_search_type", "property_type_code", "property_type",
    "url", "property_link", "title", "layout_type", "area_m2", "price_czk", "price_per_m2_czk",
    "full_address", "street_address", "district_name", "prague_zone", "city_name", "region_name", "country_name",
    "latitude", "longitude", "seller_type", "floor", "ownership_type", "energy_class",
    "description", "details_json", "has_balcony", "has_parking", "has_terrace", "has_elevator", "has_cellar",
    "scraped_at", "exists_on_source"
]


def _ensure(df, cols):
    out = df.copy()
    for c in cols:
        if c not in out.columns:
            out[c] = None
    return out[cols]


def _ensure_composite_id(df):
    if df.empty:
        return df
    if "composite_id" not in df.columns:
        req = {"source", "property_search_type", "url_id"}
        if req.issubset(df.columns):
            df = df.copy()
            df["composite_id"] = df["source"].astype(str) + "_" + df["property_search_type"].astype(str) + "_" + df["url_id"].astype(str)
        else:
            return pd.DataFrame()
    return df


def reconcile_current_with_previous(current_df, previous_df, now):
    logger.info("STAGE: State reconciliation started")
    current_df = _ensure_composite_id(current_df.copy())
    previous_df = _ensure_composite_id(previous_df.copy())

    if previous_df.empty:
        current_df["first_seen_at"] = now
        current_df["last_seen_at"] = now
        current_df["is_active"] = True
        current_df["is_removed"] = False
        current_df["removed_at"] = pd.NaT
        current_df["price_changed"] = False
        current_df["previous_price_czk"] = None
        current_df["price_change_czk"] = None
        current_df["listing_duration_days"] = 0.0
        current_df["removed_duration_days"] = None
        logger.info("STAGE: No previous state found | initializing fresh state")
        return _ensure(current_df, CURRENT_COLS), {
            "new_listings": len(current_df),
            "removed_listings": 0,
            "active_listings": len(current_df),
            "price_changes": 0
        }

    prev_map = previous_df.set_index("composite_id", drop=False).to_dict("index")
    prev_ids = set(previous_df["composite_id"].astype(str))
    curr_ids = set(current_df["composite_id"].astype(str))
    new_ids = curr_ids - prev_ids
    removed_ids = prev_ids - curr_ids
    rows = []
    price_changes = 0

    for _, row in current_df.iterrows():
        row = row.to_dict()
        rid = str(row["composite_id"])
        row["last_seen_at"] = now
        row["is_active"] = True
        row["is_removed"] = False
        row["removed_at"] = pd.NaT
        row["removed_duration_days"] = None

        if rid in prev_map:
            prev = prev_map[rid]
            row["first_seen_at"] = prev.get("first_seen_at")
            prev_price = prev.get("price_czk")
            curr_price = row.get("price_czk")
            if pd.notna(prev_price) and pd.notna(curr_price) and float(prev_price) != float(curr_price):
                row["price_changed"] = True
                row["previous_price_czk"] = prev_price
                row["price_change_czk"] = float(curr_price) - float(prev_price)
                price_changes += 1
            else:
                row["price_changed"] = False
                row["previous_price_czk"] = prev_price
                row["price_change_czk"] = 0 if pd.notna(prev_price) and pd.notna(curr_price) else None
        else:
            row["first_seen_at"] = now
            row["price_changed"] = False
            row["previous_price_czk"] = None
            row["price_change_czk"] = None

        try:
            row["listing_duration_days"] = round((pd.Timestamp(now) - pd.Timestamp(row["first_seen_at"])).total_seconds() / 86400.0, 2)
        except Exception:
            row["listing_duration_days"] = None
        rows.append(row)

    for rid in removed_ids:
        prev = dict(prev_map[rid])
        prev["is_active"] = False
        prev["is_removed"] = True
        prev["removed_at"] = now
        try:
            prev["removed_duration_days"] = round((pd.Timestamp(now) - pd.Timestamp(prev["first_seen_at"])).total_seconds() / 86400.0, 2)
            prev["listing_duration_days"] = prev["removed_duration_days"]
        except Exception:
            prev["removed_duration_days"] = None
        rows.append(prev)

    current_state = _ensure(pd.DataFrame(rows), CURRENT_COLS)
    logger.info(f"STAGE: State reconciliation finished | new: {len(new_ids)} | removed: {len(removed_ids)} | price changes: {price_changes}")
    return current_state, {
        "new_listings": len(new_ids),
        "removed_listings": len(removed_ids),
        "active_listings": int((current_state['is_active'] == True).sum()),
        "price_changes": price_changes
    }


def build_history_snapshot(current_df, previous_df, now):
    logger.info("STAGE: History snapshot build started")
    cur = _ensure_composite_id(current_df.copy())
    previous_df = _ensure_composite_id(previous_df.copy())
    cur["scraped_at"] = now
    cur["exists_on_source"] = True

    if not previous_df.empty:
        prev_ids = set(previous_df["composite_id"].astype(str))
        curr_ids = set(cur["composite_id"].astype(str))
        removed_ids = prev_ids - curr_ids
        if removed_ids:
            removed_rows = previous_df[previous_df["composite_id"].astype(str).isin(removed_ids)].copy()
            removed_rows["scraped_at"] = now
            removed_rows["exists_on_source"] = False
            cur = pd.concat([cur, removed_rows], ignore_index=True)

    out = _ensure(cur, HISTORY_COLS)
    logger.info(f"STAGE: History snapshot build finished | rows: {len(out)}")
    return out
