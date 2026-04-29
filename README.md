# Prague Real Estate Intelligence

A daily market-intelligence platform for the Prague property market. Tracks listings from Sreality (and optionally Bezrealitky), stores day-over-day state in PostgreSQL, and serves analytics through a FastAPI backend and React dashboard.

---

## What it does

- Scrapes active listings every day and compares them against the previous snapshot
- Detects new listings, removed listings, price changes, and re-listed properties
- Preserves full listing history so you can track how long any property stayed on the market
- Derives analyst-ready fields from raw scraped data (price tier, size band, Prague ring, amenity score, etc.)
- Computes cross-listing metrics like price deviation from district median
- Serves all of this via a REST API and a live React dashboard

---

## Architecture

```
Scrapers  →  pipeline.py  →  PostgreSQL (listings + listing_history)
                         →  CSV mirrors  (data/)
                         →  FastAPI (src/api/main.py)  →  dashboard.html
```

| Layer | Location | Notes |
|---|---|---|
| Scrapers | `src/adapters/` | Sreality + Bezrealitky (HTML scraping) |
| Pipeline | `src/pipeline.py` | Orchestrates scrape → clean → reconcile → persist |
| CSV processing | `src/utils/process_csv.py` | Parses titles, resolves locations, enriches derived fields |
| State reconciliation | `src/utils/state.py` | Day-over-day comparison, re-listing detection |
| Database I/O | `src/db/io.py` | SQLAlchemy, `write_dataframe_replace` |
| Normalized import | `src/db/import_clean_csvs_to_postgres.py` | Optional: upserts into a fully normalized schema |
| Analytics service | `src/analytics/service.py` | Pandas-based, reads from CSV mirrors |
| REST API | `src/api/main.py` | FastAPI, all endpoints under `/api/` |
| Dashboard | `dashboard.html` | Single-file React + Recharts, CDN imports |
| Streamlit dashboard | `src/dashboard/app.py` | Alternative UI |

---

## Quick start

### 1. Prerequisites

- Python 3.9+
- PostgreSQL running locally (or set `DATABASE_URL` to point elsewhere)

### 2. Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

### 3. Configure database

```bash
export DATABASE_URL="postgresql+psycopg2://user:password@localhost:5432/prague_real_estate"
```

### 4. Run the pipeline

```bash
python run_pipeline.py
```

To also scrape Bezrealitky:

```bash
python run_pipeline.py --bezrealitky
```

### 5. Start the API + dashboard

```bash
python start_api.py
```

Then open [http://localhost:8000](http://localhost:8000).

### 6. Run tests

```bash
python -m pytest tests/
```

---

## Data model

### `listings` table — one row per listing, latest known state

| Column | Type | Description |
|---|---|---|
| `composite_id` | str | Unique key: `{source}_{search_type}_{url_id}` |
| `layout_type` | str | e.g. `2+kk`, `3+1` |
| `bedroom_count` | int | Parsed from layout |
| `is_studio` | bool | True for 1+kk / garsonier |
| `area_m2` | float | Floor area |
| `size_band` | str | micro / studio / small / medium / large / xlarge |
| `price_czk` | int | Latest asking price |
| `price_per_m2_czk` | float | Price per m² |
| `price_tier` | str | budget / mid / premium / luxury |
| `district_name` | str | Administrative district, e.g. `Praha 5` |
| `borough_name` | str | Neighbourhood, e.g. `Smíchov` |
| `prague_ring` | str | inner / central / middle / outer |
| `floor` | int | Floor number |
| `floor_category` | str | ground / low / mid / high / penthouse |
| `energy_class` | str | A–G energy label |
| `is_new_build` | bool | Energy class A* or title contains "novostavba" |
| `amenity_score` | int | 0–5 count of balcony, parking, terrace, elevator, cellar |
| `first_seen_at` | datetime | First time ever scraped (preserved across re-lists) |
| `listing_duration_days` | float | Days since first seen |
| `listing_age_bucket` | str | fresh / active / established / stale |
| `is_active` | bool | Present in latest scrape |
| `is_removed` | bool | Absent from latest scrape |
| `relisted` | bool | Previously removed, now back |
| `price_changed` | bool | Price differed from previous scrape |
| `price_change_czk` | float | Absolute price delta |
| `price_change_pct` | float | Percentage price delta |
| `price_vs_district_median_pct` | float | % above/below district median price-per-m² |

### `listing_history` table — one row per listing per scrape

Appends a snapshot each run with `scraped_at`, `snapshot_date`, and `exists_on_source` so you can reconstruct full price and availability history for any listing.

---

## API endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/` | Serves `dashboard.html` |
| GET | `/api/health` | Health check |
| GET | `/api/market/overview` | KPI summary (active, new, removed, median price, etc.) |
| GET | `/api/market/timeseries` | Daily trend data |
| GET | `/api/market/districts` | Per-district breakdown |
| GET | `/api/market/price-movements` | New / removed / repriced listings |
| GET | `/api/listings` | Paginated active listings (filterable) |
| GET | `/api/listings/{id}/history` | Full price + availability history for one listing |
| GET | `/api/market/data-quality` | Location coverage and quality flags |
| GET | `/api/market/filter-options` | Available filter values for UI dropdowns |
| POST | `/api/reload` | Re-reads CSV data without restarting |

---

## Location resolution

When scraper data lacks an explicit district, the pipeline resolves location through a 5-step chain:

1. Neighbourhood keyword match (e.g. "Vinohrady" → Praha 2)
2. Explicit "Praha N" regex in address
3. "Praha – Name" pattern
4. Street-name lookup (~150 Prague streets mapped to borough + district)
5. Postal code (PSČ) prefix lookup

---

## Re-listing detection

If a listing disappears and then reappears:

- `first_seen_at` is carried forward from the original listing period, so days-on-market accumulates correctly across both periods
- `relisted = True` and `relisted_at` are stamped with the reappearance date
- The newest scraped price is used; `price_change_czk` and `price_change_pct` compare against the last known price before removal

---

## Notes

- `district_name` always holds the administrative zone (Praha 1–22). `borough_name` holds the neighbourhood (Smíchov, Vinohrady, etc.). They are never conflated.
- `location_quality` flags partial or suspicious parses (`missing_borough`, `zone_without_known_borough`, etc.).
- The pipeline deduplicates history rows so re-running on the same day is safe.
- `write_dataframe_replace` recreates the `listings` and `listing_history` tables on every run — fast but drops PostgreSQL indexes. Switch to an upsert strategy for production.
