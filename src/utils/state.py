import pandas as pd
from src.utils.logger import get_logger

logger = get_logger("state")

CURRENT_COLS = [
    "composite_id", "url_id", "source", "property_search_type", "property_type_code", "property_type",
    "url", "property_link", "title",
    "layout_type", "bedroom_count", "is_studio",
    "area_m2", "size_band", "price_czk", "price_per_m2_czk", "price_tier",
    "full_address", "street_address", "borough_name", "district_name", "prague_zone", "prague_ring",
    "location_quality", "city_name", "region_name", "country_name",
    "latitude", "longitude", "seller_type", "floor", "floor_category",
    "ownership_type", "energy_class", "is_new_build",
    "description", "details_json",
    "has_balcony", "has_parking", "has_terrace", "has_elevator", "has_cellar", "amenity_score",
    "first_seen_at", "last_seen_at", "snapshot_date", "is_active", "is_removed", "removed_at",
    "relisted", "relisted_at",
    "price_changed", "previous_price_czk", "price_change_czk", "price_change_pct",
    "listing_duration_days", "listing_age_bucket", "removed_duration_days",
    "price_vs_district_median_pct",
]

HISTORY_COLS = [
    "composite_id", "url_id", "source", "property_search_type", "property_type_code", "property_type",
    "url", "property_link", "title",
    "layout_type", "bedroom_count", "is_studio",
    "area_m2", "size_band", "price_czk", "price_per_m2_czk", "price_tier",
    "full_address", "street_address", "borough_name", "district_name", "prague_zone", "prague_ring",
    "location_quality", "city_name", "region_name", "country_name",
    "latitude", "longitude", "seller_type", "floor", "floor_category",
    "ownership_type", "energy_class", "is_new_build",
    "description", "details_json",
    "has_balcony", "has_parking", "has_terrace", "has_elevator", "has_cellar", "amenity_score",
    "scraped_at", "snapshot_date", "exists_on_source",
]


def _ensure(df, cols):
    out = df.copy()
    for c in cols:
        if c not in out.columns:
            out[c] = None
    return out[cols]


def _ensure_composite_id(df):
    if "composite_id" not in df.columns:
        req = {"source", "property_search_type", "url_id"}
        if req.issubset(df.columns):
            df = df.copy()
            df["composite_id"] = (
                df["source"].astype(str) + "_" +
                df["property_search_type"].astype(str) + "_" +
                df["url_id"].astype(str)
            )
        else:
            return pd.DataFrame()
    return df


def _safe_bool(value, default: bool = False) -> bool:
    """
    Convert *value* to bool without raising on NaN / None / non-standard types.

    pandas stores boolean columns as proper Python bools when the table comes
    back from PostgreSQL via SQLAlchemy, but defensive handling here prevents a
    ``ValueError: The truth value of a Series is ambiguous`` or
    ``ValueError: cannot convert float NaN to bool`` crash if anything slips
    through as a float NaN or a nullable boolean.
    """
    if value is None:
        return default
    try:
        if pd.isna(value):
            return default
    except (TypeError, ValueError):
        pass
    if isinstance(value, bool):
        return value
    try:
        return bool(value)
    except Exception:
        return default


def _status_subset(df, column_name, expected=True):
    if df.empty or column_name not in df.columns:
        return pd.DataFrame(columns=df.columns)
    return df[df[column_name] == expected].copy()


def _listing_age_bucket(duration_days) -> str:
    """
    Classify listing tenure into human-readable buckets.

    fresh       0 – 6 days    Just appeared; likely still attracting viewings.
    active      7 – 29 days   Healthy on-market period for Prague.
    established 30 – 89 days  Slower to shift; may see price negotiation.
    stale       90+ days      Long-running; possible overpricing or niche property.
    """
    try:
        d = float(duration_days)
    except (TypeError, ValueError):
        return "unknown"
    if d < 7:
        return "fresh"
    if d < 30:
        return "active"
    if d < 90:
        return "established"
    return "stale"


def _price_change_pct(price_change_czk, previous_price_czk):
    """Return percentage price change, rounded to 2 dp. None when inputs are missing."""
    try:
        prev = float(previous_price_czk)
        delta = float(price_change_czk)
        if prev == 0:
            return None
        return round((delta / prev) * 100, 2)
    except (TypeError, ValueError):
        return None


def reconcile_current_with_previous(current_df, previous_df, now):
    """
    Compare today's scrape against the previous saved state and produce a
    reconciled ``listings`` snapshot.

    Re-listed properties
    --------------------
    If a composite_id is present in today's scrape AND existed in the previous
    state with ``is_removed=True``, the listing is treated as **re-listed**:

    * ``first_seen_at``  is carried forward from the original first appearance
      (so days-on-market accumulates across listing periods).
    * ``relisted=True``  and ``relisted_at=<now>`` are set.
    * The newest scraped price is used; a price delta vs the last-known price
      is recorded so price changes between listing periods are visible.

    Price tracking
    --------------
    The freshest price always comes from ``current_df`` (the latest scrape).
    ``previous_price_czk`` and ``price_change_czk`` record only the delta
    against the immediately preceding snapshot, regardless of whether the
    listing was continuously active or was re-listed.
    """
    logger.info("STAGE: State reconciliation started")
    current_df = _ensure_composite_id(current_df.copy())
    previous_df = _ensure_composite_id(previous_df.copy())
    snapshot_date = pd.Timestamp(now).date()

    # ── First run: no previous state at all ──────────────────────────────────
    if previous_df.empty:
        current_df["first_seen_at"]       = now
        current_df["last_seen_at"]        = now
        current_df["snapshot_date"]       = snapshot_date
        current_df["is_active"]           = True
        current_df["is_removed"]          = False
        current_df["removed_at"]          = pd.NaT
        current_df["relisted"]            = False
        current_df["relisted_at"]         = pd.NaT
        current_df["price_changed"]          = False
        current_df["previous_price_czk"]     = None
        current_df["price_change_czk"]       = None
        current_df["price_change_pct"]       = None
        current_df["listing_duration_days"]  = 0.0
        current_df["listing_age_bucket"]     = "fresh"
        current_df["removed_duration_days"]  = None
        logger.info("STAGE: No previous state found | initializing fresh state")
        return _ensure(current_df, CURRENT_COLS), {
            "new_listings":      len(current_df),
            "removed_listings":  0,
            "relisted_listings": 0,
            "active_listings":   len(current_df),
            "price_changes":     0,
        }

    # ── Build lookup structures ───────────────────────────────────────────────
    previous_active_df  = _status_subset(previous_df, "is_active",  True)
    previous_removed_df = _status_subset(previous_df, "is_removed", True)
    prev_map            = previous_df.set_index("composite_id", drop=False).to_dict("index")
    prev_active_ids     = set(previous_active_df["composite_id"].astype(str))
    curr_ids            = set(current_df["composite_id"].astype(str))
    new_ids             = curr_ids - prev_active_ids
    removed_ids         = prev_active_ids - curr_ids
    rows: list          = []
    price_changes       = 0
    relisted_count      = 0

    # ── Process every listing present in today's scrape ──────────────────────
    for _, row in current_df.iterrows():
        row = row.to_dict()
        rid = str(row["composite_id"])

        # Defaults for all rows
        row["last_seen_at"]         = now
        row["snapshot_date"]        = snapshot_date
        row["is_active"]            = True
        row["is_removed"]           = False
        row["removed_at"]           = pd.NaT
        row["removed_duration_days"] = None
        row["relisted"]             = False
        row["relisted_at"]          = pd.NaT

        prev_record = prev_map.get(rid)

        if prev_record is not None:
            was_active  = _safe_bool(prev_record.get("is_active"),  default=True)
            was_removed = _safe_bool(prev_record.get("is_removed"), default=False)

            if was_active and not was_removed:
                # ── Normal continuation ──────────────────────────────────────
                # Listing was active last run; preserve its first_seen_at.
                row["first_seen_at"] = prev_record.get("first_seen_at")

                prev_price = prev_record.get("price_czk")
                curr_price = row.get("price_czk")
                if pd.notna(prev_price) and pd.notna(curr_price) and float(prev_price) != float(curr_price):
                    row["price_changed"]      = True
                    row["previous_price_czk"] = prev_price
                    row["price_change_czk"]   = float(curr_price) - float(prev_price)
                    price_changes += 1
                else:
                    row["price_changed"]      = False
                    row["previous_price_czk"] = prev_price
                    row["price_change_czk"]   = (
                        0.0 if pd.notna(prev_price) and pd.notna(curr_price) else None
                    )

            else:
                # ── Re-listed ────────────────────────────────────────────────
                # Listing was previously removed (or in an ambiguous removed
                # state) and has now reappeared.  Carry forward the ORIGINAL
                # first_seen_at so days-on-market accumulates correctly across
                # both listing periods.
                original_first_seen = prev_record.get("first_seen_at")
                row["first_seen_at"] = (
                    original_first_seen
                    if original_first_seen is not None and pd.notna(original_first_seen)
                    else now
                )

                if was_removed:
                    row["relisted"]    = True
                    row["relisted_at"] = now
                    relisted_count    += 1
                    logger.debug(
                        f"Re-listed: {rid} | original first_seen_at={row['first_seen_at']}"
                    )

                # Compare latest scraped price vs last-known price (even if the
                # listing was off market for a while — useful for price intel).
                prev_price = prev_record.get("price_czk")
                curr_price = row.get("price_czk")
                row["previous_price_czk"] = prev_price
                if pd.notna(prev_price) and pd.notna(curr_price) and float(prev_price) != float(curr_price):
                    row["price_changed"]    = True
                    row["price_change_czk"] = float(curr_price) - float(prev_price)
                    price_changes += 1
                else:
                    row["price_changed"]    = False
                    row["price_change_czk"] = None

        else:
            # ── Brand new listing, never seen before ─────────────────────────
            row["first_seen_at"]      = now
            row["price_changed"]      = False
            row["previous_price_czk"] = None
            row["price_change_czk"]   = None

        # Days on market accumulates from first_seen_at regardless of re-lists
        try:
            row["listing_duration_days"] = round(
                (pd.Timestamp(now) - pd.Timestamp(row["first_seen_at"])).total_seconds() / 86400.0, 2
            )
        except Exception:
            row["listing_duration_days"] = None

        row["listing_age_bucket"] = _listing_age_bucket(row.get("listing_duration_days"))
        row["price_change_pct"]   = _price_change_pct(
            row.get("price_change_czk"), row.get("previous_price_czk")
        )
        rows.append(row)

    # ── Listings that were active last run but are gone today ─────────────────
    previous_active_map = previous_active_df.set_index("composite_id", drop=False).to_dict("index")
    for rid in removed_ids:
        prev = dict(previous_active_map[rid])
        prev["is_active"]    = False
        prev["is_removed"]   = True
        prev["removed_at"]   = now
        prev["snapshot_date"] = snapshot_date
        # relisted fields carry whatever was already stored (False / NaT)
        try:
            prev["removed_duration_days"] = round(
                (pd.Timestamp(now) - pd.Timestamp(prev["first_seen_at"])).total_seconds() / 86400.0, 2
            )
            prev["listing_duration_days"] = prev["removed_duration_days"]
        except Exception:
            prev["removed_duration_days"] = None
        prev["listing_age_bucket"] = _listing_age_bucket(prev.get("listing_duration_days"))
        # price_change_pct carries forward whatever was already stored
        if "price_change_pct" not in prev or prev.get("price_change_pct") is None:
            prev["price_change_pct"] = _price_change_pct(
                prev.get("price_change_czk"), prev.get("previous_price_czk")
            )
        rows.append(prev)

    # ── Carry forward previously-removed listings ─────────────────────────────
    if not previous_removed_df.empty:
        rows.extend(previous_removed_df.to_dict("records"))

    current_state = _ensure(pd.DataFrame(rows), CURRENT_COLS)
    # Active records are appended first, so keep="first" gives the active
    # version priority over any stale removed duplicate.
    current_state = current_state.drop_duplicates(subset=["composite_id"], keep="first")

    logger.info(
        f"STAGE: State reconciliation finished | "
        f"new: {len(new_ids)} | removed: {len(removed_ids)} | "
        f"relisted: {relisted_count} | price changes: {price_changes}"
    )
    return current_state, {
        "new_listings":      len(new_ids),
        "removed_listings":  len(removed_ids),
        "relisted_listings": relisted_count,
        "active_listings":   int((current_state["is_active"] == True).sum()),
        "price_changes":     price_changes,
    }


def build_history_snapshot(current_df, previous_df, now):
    logger.info("STAGE: History snapshot build started")
    cur         = _ensure_composite_id(current_df.copy())
    previous_df = _ensure_composite_id(previous_df.copy())
    cur["scraped_at"]       = now
    cur["snapshot_date"]    = pd.Timestamp(now).date()
    cur["exists_on_source"] = True

    if not previous_df.empty:
        previous_active_df = _status_subset(previous_df, "is_active", True)
        prev_ids = set(previous_active_df["composite_id"].astype(str))
        curr_ids = set(cur["composite_id"].astype(str))
        removed_ids = prev_ids - curr_ids
        if removed_ids:
            removed_rows = previous_active_df[
                previous_active_df["composite_id"].astype(str).isin(removed_ids)
            ].copy()
            removed_rows["scraped_at"]       = now
            removed_rows["snapshot_date"]    = pd.Timestamp(now).date()
            removed_rows["exists_on_source"] = False
            cur = pd.concat([cur, removed_rows], ignore_index=True)

    out = _ensure(cur, HISTORY_COLS)
    out = out.drop_duplicates(
        subset=["composite_id", "scraped_at", "exists_on_source"], keep="last"
    )
    logger.info(f"STAGE: History snapshot build finished | rows: {len(out)}")
    return out
