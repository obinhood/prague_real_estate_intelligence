"""
Prague Real Estate Intelligence — FastAPI Backend
Serves all dashboard metrics, time-series, district analytics, and listing data.
Falls back to CSV data if no live database connection is configured.
"""
from __future__ import annotations

import math
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------
app = FastAPI(
    title="Prague Real Estate Intelligence API",
    description="Daily market snapshots, listing analytics, and district breakdowns for the Prague property market.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Analytics service bootstrap
# ---------------------------------------------------------------------------
_bundle = None


def _get_bundle():
    global _bundle
    if _bundle is None:
        try:
            from src.analytics.service import load_market_data
            _bundle = load_market_data()
        except Exception as exc:
            return None
    return _bundle


def _reload_bundle():
    global _bundle
    _bundle = None
    return _get_bundle()


# ---------------------------------------------------------------------------
# Serialisation helpers
# ---------------------------------------------------------------------------
def _v(val: Any) -> Any:
    """Convert pandas / numpy scalars to plain Python types for JSON."""
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except Exception:
        pass
    if hasattr(val, "item"):          # numpy scalar
        return val.item()
    if isinstance(val, float) and math.isnan(val):
        return None
    return val


def _clean_dict(d: Dict) -> Dict:
    return {k: _v(v) for k, v in d.items()}


def _clean_records(df: pd.DataFrame) -> List[Dict]:
    if df is None or df.empty:
        return []
    return [_clean_dict(r) for r in df.to_dict("records")]


# ---------------------------------------------------------------------------
# Filter helpers
# ---------------------------------------------------------------------------
def _build_filters(
    source: Optional[List[str]],
    property_type: Optional[List[str]],
    district: Optional[List[str]],
    borough: Optional[List[str]],
    seller_type: Optional[List[str]],
    search: Optional[str],
    price_min: Optional[float],
    price_max: Optional[float],
    size_min: Optional[float],
    size_max: Optional[float],
    date_from: Optional[str],
    date_to: Optional[str],
) -> Dict:
    f: Dict = {}
    if source:
        f["sources"] = source
    if property_type:
        f["property_types"] = property_type
    if district:
        f["districts"] = district
    if borough:
        f["boroughs"] = borough
    if seller_type:
        f["seller_types"] = seller_type
    if search and search.strip():
        f["search"] = search.strip()
    if price_min is not None or price_max is not None:
        f["price_range"] = (price_min or 0, price_max or float("inf"))
    if size_min is not None or size_max is not None:
        f["size_range"] = (size_min or 0, size_max or float("inf"))
    if date_from or date_to:
        try:
            start = pd.to_datetime(date_from).date() if date_from else None
            end = pd.to_datetime(date_to).date() if date_to else None
            f["date_range"] = (start, end)
        except Exception:
            pass
    return f


_COMMON_PARAMS = dict(
    source=Query(None, description="Filter by source (sreality, bezrealitky)"),
    property_type=Query(None, description="Filter by property type code"),
    district=Query(None, description="Filter by district name (Praha 5, Praha 2 …)"),
    borough=Query(None, description="Filter by borough/neighbourhood name"),
    seller_type=Query(None, description="Filter by seller type (owner, agency)"),
    search=Query(None, description="Free-text search in title / address"),
    price_min=Query(None, description="Minimum price in CZK"),
    price_max=Query(None, description="Maximum price in CZK"),
    size_min=Query(None, description="Minimum area in m²"),
    size_max=Query(None, description="Maximum area in m²"),
    date_from=Query(None, description="Start of date range (YYYY-MM-DD)"),
    date_to=Query(None, description="End of date range (YYYY-MM-DD)"),
)


# ---------------------------------------------------------------------------
# Static dashboard
# ---------------------------------------------------------------------------
_DASHBOARD_PATHS = [
    Path("dashboard.html"),
    Path(__file__).parents[2] / "dashboard.html",
]


@app.get("/", include_in_schema=False)
def serve_dashboard():
    for p in _DASHBOARD_PATHS:
        if p.exists():
            return FileResponse(str(p), media_type="text/html")
    return JSONResponse(
        {"error": "dashboard.html not found — place it alongside src/ or run from the project root"},
        status_code=404,
    )


@app.post("/api/reload", summary="Reload market data from disk")
def reload_data():
    b = _reload_bundle()
    if b is None:
        return {"status": "no_data", "message": "No CSV data found. Run run_pipeline.py first."}
    return {"status": "ok", "message": "Market data reloaded."}


# ---------------------------------------------------------------------------
# /api/market/overview
# ---------------------------------------------------------------------------
@app.get("/api/market/overview", summary="KPI metrics with day-over-day deltas")
def market_overview(
    source: Optional[List[str]] = Query(None),
    property_type: Optional[List[str]] = Query(None),
    district: Optional[List[str]] = Query(None),
    borough: Optional[List[str]] = Query(None),
    seller_type: Optional[List[str]] = Query(None),
    search: Optional[str] = Query(None),
    price_min: Optional[float] = Query(None),
    price_max: Optional[float] = Query(None),
    size_min: Optional[float] = Query(None),
    size_max: Optional[float] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
):
    bundle = _get_bundle()
    if bundle is None:
        raise HTTPException(503, "No market data available. Run run_pipeline.py first.")

    from src.analytics.service import get_market_overview
    filters = _build_filters(source, property_type, district, borough, seller_type, search, price_min, price_max, size_min, size_max, date_from, date_to)
    overview = get_market_overview(bundle, filters)

    def _metric(m):
        if isinstance(m, dict):
            return {k: _v(v) for k, v in m.items()}
        return _v(m)

    return {k: _metric(v) for k, v in overview.items()}


# ---------------------------------------------------------------------------
# /api/market/timeseries
# ---------------------------------------------------------------------------
@app.get("/api/market/timeseries", summary="Daily snapshot trend data")
def market_timeseries(
    source: Optional[List[str]] = Query(None),
    property_type: Optional[List[str]] = Query(None),
    district: Optional[List[str]] = Query(None),
    borough: Optional[List[str]] = Query(None),
    seller_type: Optional[List[str]] = Query(None),
    search: Optional[str] = Query(None),
    price_min: Optional[float] = Query(None),
    price_max: Optional[float] = Query(None),
    size_min: Optional[float] = Query(None),
    size_max: Optional[float] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
):
    bundle = _get_bundle()
    if bundle is None:
        raise HTTPException(503, "No market data available.")

    from src.analytics.service import get_market_timeseries
    filters = _build_filters(source, property_type, district, borough, seller_type, search, price_min, price_max, size_min, size_max, date_from, date_to)
    df = get_market_timeseries(bundle, filters)
    if df.empty:
        return {"data": []}
    df["snapshot_date"] = df["snapshot_date"].astype(str)
    return {"data": _clean_records(df)}


# ---------------------------------------------------------------------------
# /api/market/districts
# ---------------------------------------------------------------------------
@app.get("/api/market/districts", summary="District-level analytics")
def market_districts(
    source: Optional[List[str]] = Query(None),
    property_type: Optional[List[str]] = Query(None),
    district: Optional[List[str]] = Query(None),
    borough: Optional[List[str]] = Query(None),
    seller_type: Optional[List[str]] = Query(None),
    search: Optional[str] = Query(None),
    price_min: Optional[float] = Query(None),
    price_max: Optional[float] = Query(None),
    size_min: Optional[float] = Query(None),
    size_max: Optional[float] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
):
    bundle = _get_bundle()
    if bundle is None:
        raise HTTPException(503, "No market data available.")

    from src.analytics.service import get_market_districts
    filters = _build_filters(source, property_type, district, borough, seller_type, search, price_min, price_max, size_min, size_max, date_from, date_to)
    df = get_market_districts(bundle, filters)
    return {"data": _clean_records(df)}


# ---------------------------------------------------------------------------
# /api/market/price-movements
# ---------------------------------------------------------------------------
@app.get("/api/market/price-movements", summary="New, removed, and price-changed listings since previous snapshot")
def market_price_movements(
    source: Optional[List[str]] = Query(None),
    property_type: Optional[List[str]] = Query(None),
    district: Optional[List[str]] = Query(None),
    borough: Optional[List[str]] = Query(None),
    seller_type: Optional[List[str]] = Query(None),
    search: Optional[str] = Query(None),
    price_min: Optional[float] = Query(None),
    price_max: Optional[float] = Query(None),
    size_min: Optional[float] = Query(None),
    size_max: Optional[float] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
):
    bundle = _get_bundle()
    if bundle is None:
        raise HTTPException(503, "No market data available.")

    from src.analytics.service import get_market_price_movements
    filters = _build_filters(source, property_type, district, borough, seller_type, search, price_min, price_max, size_min, size_max, date_from, date_to)
    df = get_market_price_movements(bundle, filters)
    return {"data": _clean_records(df)}


# ---------------------------------------------------------------------------
# /api/listings
# ---------------------------------------------------------------------------
@app.get("/api/listings", summary="Paginated active listings with full filter set")
def get_listings(
    source: Optional[List[str]] = Query(None),
    property_type: Optional[List[str]] = Query(None),
    district: Optional[List[str]] = Query(None),
    borough: Optional[List[str]] = Query(None),
    seller_type: Optional[List[str]] = Query(None),
    search: Optional[str] = Query(None),
    price_min: Optional[float] = Query(None),
    price_max: Optional[float] = Query(None),
    size_min: Optional[float] = Query(None),
    size_max: Optional[float] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    sort_by: Optional[str] = Query("price_czk", description="Column to sort by"),
    sort_order: Optional[str] = Query("desc", description="asc or desc"),
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=500),
):
    bundle = _get_bundle()
    if bundle is None:
        raise HTTPException(503, "No market data available.")

    from src.analytics.service import get_active_listings
    filters = _build_filters(source, property_type, district, borough, seller_type, search, price_min, price_max, size_min, size_max, date_from, date_to)
    df = get_active_listings(bundle, filters)

    if not df.empty and sort_by and sort_by in df.columns:
        ascending = sort_order == "asc"
        df = df.sort_values(sort_by, ascending=ascending, na_position="last")

    total = len(df)
    start = (page - 1) * per_page
    end = start + per_page
    page_df = df.iloc[start:end]

    columns = [c for c in [
        "composite_id", "source", "property_type", "district_name", "borough_name",
        "title", "price_czk", "price_per_m2_czk", "area_m2", "layout_type",
        "seller_type", "listing_duration_days", "first_seen_at", "last_seen_at",
        "property_link", "location_quality", "has_balcony", "has_parking",
        "has_terrace", "has_elevator", "energy_class", "floor",
    ] if c in page_df.columns]

    for col in ["first_seen_at", "last_seen_at"]:
        if col in page_df.columns:
            page_df = page_df.copy()
            page_df[col] = page_df[col].astype(str)

    return {
        "total": total,
        "page": page,
        "per_page": per_page,
        "pages": math.ceil(total / per_page),
        "data": _clean_records(page_df[columns]),
    }


# ---------------------------------------------------------------------------
# /api/listings/{composite_id}/history
# ---------------------------------------------------------------------------
@app.get("/api/listings/{composite_id}/history", summary="Price and availability history for a single listing")
def listing_history(composite_id: str):
    bundle = _get_bundle()
    if bundle is None:
        raise HTTPException(503, "No market data available.")

    from src.analytics.service import get_listing_history
    df = get_listing_history(bundle, composite_id)
    if df.empty:
        raise HTTPException(404, f"No history found for listing '{composite_id}'.")

    for col in ["scraped_at", "snapshot_date"]:
        if col in df.columns:
            df[col] = df[col].astype(str)
    return {"composite_id": composite_id, "data": _clean_records(df)}


# ---------------------------------------------------------------------------
# /api/market/data-quality
# ---------------------------------------------------------------------------
@app.get("/api/market/data-quality", summary="Data coverage and quality metrics")
def data_quality(
    source: Optional[List[str]] = Query(None),
    property_type: Optional[List[str]] = Query(None),
    district: Optional[List[str]] = Query(None),
    borough: Optional[List[str]] = Query(None),
    seller_type: Optional[List[str]] = Query(None),
    search: Optional[str] = Query(None),
    price_min: Optional[float] = Query(None),
    price_max: Optional[float] = Query(None),
    size_min: Optional[float] = Query(None),
    size_max: Optional[float] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
):
    bundle = _get_bundle()
    if bundle is None:
        raise HTTPException(503, "No market data available.")

    from src.analytics.service import get_data_quality
    filters = _build_filters(source, property_type, district, borough, seller_type, search, price_min, price_max, size_min, size_max, date_from, date_to)
    quality = get_data_quality(bundle, filters)
    return _clean_dict(quality)


# ---------------------------------------------------------------------------
# /api/market/filter-options
# ---------------------------------------------------------------------------
@app.get("/api/market/filter-options", summary="Available filter values for the UI")
def filter_options():
    bundle = _get_bundle()
    if bundle is None:
        return {"sources": [], "property_types": [], "districts": [], "boroughs": [], "seller_types": []}

    from src.analytics.service import _apply_common_filters
    df = bundle.current_df
    if "is_active" in df.columns:
        df = df[df["is_active"] == True].copy()

    def _uniq(col):
        if col not in df.columns:
            return []
        return sorted([str(v) for v in df[col].dropna().unique() if str(v).strip()])

    price_vals = df["price_czk"].dropna() if "price_czk" in df.columns else pd.Series(dtype=float)
    size_vals = df["area_m2"].dropna() if "area_m2" in df.columns else pd.Series(dtype=float)

    return {
        "sources": _uniq("source"),
        "property_types": _uniq("property_type"),
        "districts": _uniq("district_name"),
        "boroughs": _uniq("borough_name"),
        "seller_types": _uniq("seller_type"),
        "price_min": _v(price_vals.min()) if not price_vals.empty else None,
        "price_max": _v(price_vals.max()) if not price_vals.empty else None,
        "size_min": _v(size_vals.min()) if not size_vals.empty else None,
        "size_max": _v(size_vals.max()) if not size_vals.empty else None,
    }


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------
@app.get("/api/health", include_in_schema=False)
def health():
    bundle = _get_bundle()
    has_data = bundle is not None and not bundle.current_df.empty
    return {
        "status": "ok",
        "data_loaded": has_data,
        "active_listings": int(len(bundle.current_df[bundle.current_df.get("is_active", False) == True])) if has_data else 0,
    }
