"""
description_enrichment.py
─────────────────────────
Extracts missing or incorrect address / location data from listing
descriptions and backfills borough_name, district_name, full_address,
street_address, and location_quality.

Sreality meta-descriptions reliably follow the pattern:
  "PropertyType N+kk AREA m² k prodeji ADDRESS; PRICE, features…"

So extracting the text between "k prodeji" and the first ";" gives us a clean
address string, which we can then feed into the existing deduce_district_and_zone
and stredocesky_kraj locality logic.
"""

import re
from typing import Optional, Tuple

import pandas as pd

from src.utils.helpers import clean_text
from src.utils.logger import get_logger
from src.utils.process_csv import (
    BAD_DISTRICT_PHRASES,
    deduce_district_and_zone,
    infer_market_scope,
)

logger = get_logger("description_enrichment")

# ── Regex patterns ────────────────────────────────────────────────────────────

# Matches "k prodeji ADDRESS;" or "k prodeji ADDRESS," (fallback)
_K_PRODEJI_RE = re.compile(
    r"k\s+prodeji\s+([^;,\n]{3,120}?)(?:[;,]|\s+\d{1,3}(?:\s+\d{3})*\s*Kč|$)",
    re.IGNORECASE,
)

# Matches "k pronájmu ADDRESS;"
_K_PRONAJEM_RE = re.compile(
    r"k\s+pron[áa]jmu\s+([^;,\n]{3,120}?)(?:[;,]|\s+\d{1,3}(?:\s+\d{3})*\s*Kč|$)",
    re.IGNORECASE,
)

# Strip leading "street, " to find the last locality part (for _sc)
_LOCALITY_LAST_RE = re.compile(r"^[^,]+,\s*")


def _extract_address_from_description(description: str) -> Optional[str]:
    """
    Return the raw address string found in the description, or None.

    Tries "k prodeji" first, then "k pronájmu" as a fallback.
    """
    if not description:
        return None
    text = clean_text(description) or ""

    for pattern in (_K_PRODEJI_RE, _K_PRONAJEM_RE):
        m = pattern.search(text)
        if m:
            candidate = clean_text(m.group(1))
            if candidate and len(candidate) >= 3:
                # Remove trailing noise phrases
                for bad in BAD_DISTRICT_PHRASES:
                    candidate = candidate.replace(bad, "").replace(bad.lower(), "")
                candidate = clean_text(candidate).strip(",; ") or None
                if candidate:
                    return candidate
    return None


def _is_bad_locality(value) -> bool:
    """Return True when a borough/district value is missing or clearly wrong."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return True
    s = str(value).strip()
    if not s or s in ("Praha - Ostatní", "nan"):
        return True
    # "Středočeský kraj" is a valid district name — never flag as bad
    if s == "Středočeský kraj":
        return False
    # Contains area measurement → it's a title fragment, not a place name
    if re.search(r"\d+\s*m²", s):
        return True
    return False


def enrich_from_descriptions(df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
    """
    Scan descriptions for listings that are missing a valid address or have a
    corrupted borough/district name, and backfill the location fields.

    Returns
    -------
    df_enriched : DataFrame
        A copy of *df* with updated location columns.
    n_updated : int
        Number of rows that had at least one field updated.
    """
    if df.empty:
        return df.copy(), 0

    out = df.copy()
    n_updated = 0

    # Identify rows that need enrichment:
    #   • full_address is missing, OR
    #   • borough_name/district_name is bad (None, sentinel, or contains "m²")
    needs_enrichment = (
        out["full_address"].isna()
        | out["borough_name"].apply(_is_bad_locality)
        | out["district_name"].apply(_is_bad_locality)
    )

    logger.info(
        f"Description enrichment: {needs_enrichment.sum()} rows need address backfill "
        f"(out of {len(out)} total)"
    )

    description_col = out.get("description", pd.Series([None] * len(out)))

    for idx in out.index[needs_enrichment]:
        desc = description_col.get(idx) if hasattr(description_col, "get") else description_col.iloc[idx]
        if pd.isna(desc) or not desc:
            continue

        address_from_desc = _extract_address_from_description(str(desc))
        if not address_from_desc:
            continue

        row = out.loc[idx]
        pst = str(row.get("property_search_type") or "")
        scope = infer_market_scope(pst)
        changed = False

        # Always update full_address if it was missing
        if pd.isna(row.get("full_address")) or not row.get("full_address"):
            out.at[idx, "full_address"] = address_from_desc
            if "," in address_from_desc:
                street_part = clean_text(address_from_desc.split(",", 1)[0])
                out.at[idx, "street_address"] = street_part
            else:
                out.at[idx, "street_address"] = address_from_desc
            changed = True

        # Update borough / district from the extracted address
        if scope == "praha":
            borough_name, district_name, prague_zone, location_quality = deduce_district_and_zone(
                address_from_desc, str(row.get("title") or "")
            )
            if _is_bad_locality(row.get("borough_name")) and not _is_bad_locality(borough_name):
                out.at[idx, "borough_name"] = borough_name
                changed = True
            if _is_bad_locality(row.get("district_name")) and not _is_bad_locality(district_name):
                out.at[idx, "district_name"] = district_name
                out.at[idx, "prague_zone"] = prague_zone
                out.at[idx, "location_quality"] = location_quality
                changed = True
        else:
            # Středočeský kraj: last comma-separated part is the municipality.
            # borough_name = municipality; district_name = "Středočeský kraj".
            parts = [clean_text(p) for p in re.split(r",", address_from_desc) if clean_text(p)]
            locality = parts[-1] if len(parts) >= 2 else (parts[0] if parts else None)
            if locality and locality.lower().startswith("praha"):
                locality = None
            if locality and _is_bad_locality(row.get("borough_name")):
                out.at[idx, "borough_name"] = locality
                out.at[idx, "district_name"] = "Středočeský kraj"
                out.at[idx, "location_quality"] = "ok"
                changed = True
            elif not _is_bad_locality(row.get("borough_name")):
                # borough already good — ensure district is set to the region
                if str(row.get("district_name") or "") != "Středočeský kraj":
                    out.at[idx, "district_name"] = "Středočeský kraj"
                    changed = True

        if changed:
            n_updated += 1

    logger.info(f"Description enrichment complete: {n_updated} rows updated")
    return out, n_updated
