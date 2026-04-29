# Setup Guide

Step-by-step instructions for getting the Prague Real Estate Intelligence platform running locally.

---

## Requirements

- Python 3.9 or higher
- PostgreSQL 14+ (local or remote)
- Git

---

## 1. Clone the repo

```bash
git clone <repo-url>
cd prague_real_estate_intelligence
```

---

## 2. Create and activate a virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate       # macOS / Linux
# .venv\Scripts\activate        # Windows
```

---

## 3. Install dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

Key packages installed:

| Package | Version | Purpose |
|---|---|---|
| pandas | ≥2.1, <2.3 | Data processing |
| sqlalchemy | ≥2.0, <2.1 | Database ORM |
| psycopg2-binary | ≥2.9 | PostgreSQL driver |
| fastapi | ≥0.111 | REST API |
| uvicorn | ≥0.29 | ASGI server |
| requests | ≥2.31 | HTTP scraping |
| beautifulsoup4 | ≥4.12 | HTML parsing |
| streamlit | ≥1.32 | Alternative dashboard UI |
| plotly | ≥5.18 | Charts (Streamlit dashboard) |

---

## 4. Configure the database

Set the `DATABASE_URL` environment variable before running anything:

```bash
export DATABASE_URL="postgresql+psycopg2://user:password@localhost:5432/prague_real_estate"
```

To make this permanent, add it to your shell profile (`.zshrc`, `.bashrc`) or a `.env` file in the project root. The `.env` file is gitignored.

**Create the database** if it doesn't exist yet:

```bash
psql -U postgres -c "CREATE DATABASE prague_real_estate;"
```

The pipeline creates tables automatically on first run using `pandas.DataFrame.to_sql`.

---

## 5. (Optional) Apply the normalized PostgreSQL schema

For a production-grade normalized schema with indexes, foreign keys, and analytics views:

```bash
python -m src.db.postgres_schema
```

Then import existing CSV data into the normalized tables:

```bash
python -m src.db.import_clean_csvs_to_postgres
```

---

## 6. Run the pipeline

```bash
python run_pipeline.py
```

This will:

1. Scrape listings from Sreality
2. Clean and parse titles, resolve locations, compute derived fields
3. Compare against the previous state (detect new, removed, re-listed, repriced)
4. Write the reconciled state and full history to PostgreSQL
5. Write CSV mirrors to `data/`
6. Generate reports: `data/market_report.html`, `data/daily_price_report.csv`

To also scrape Bezrealitky:

```bash
python run_pipeline.py --bezrealitky
```

---

## 7. Start the API and dashboard

```bash
python start_api.py
```

Open [http://localhost:8000](http://localhost:8000) for the React dashboard.

The API is available at `http://localhost:8000/api/` — see README for endpoint reference.

---

## 8. (Optional) Run the Streamlit dashboard

```bash
streamlit run src/dashboard/app.py
```

---

## 9. Run the tests

```bash
python -m pytest tests/
```

---

## Data directory layout

```
data/
  listings_master.csv       Raw scrape output (overwritten each run)
  listings_processed.csv    Cleaned + enriched state (latest snapshot)
  listing_history.csv       Full append-only history
  removed_listings.csv      Listings marked as removed
  daily_price_report.csv    Daily median price time series
  market_report.html        Static HTML market summary
```

All files in `data/` are gitignored — they are regenerated on each pipeline run.

---

## Environment variables reference

| Variable | Required | Example | Description |
|---|---|---|---|
| `DATABASE_URL` | Yes | `postgresql+psycopg2://user:pw@localhost/db` | SQLAlchemy database URL |

---

## Troubleshooting

**`ModuleNotFoundError: No module named 'sqlalchemy'`**
Run `pip install -r requirements.txt` inside the activated virtual environment.

**`psycopg2.OperationalError: could not connect to server`**
Check that PostgreSQL is running and that `DATABASE_URL` is set correctly.

**`KeyError: 'composite_id'`**
The master CSV is missing required columns. Check that the scraper returned data — re-run `python run_pipeline.py` once scrapers are reachable.

**Dashboard shows mock data only**
The dashboard falls back to embedded mock data when the API is unreachable. Start `python start_api.py` and reload.

**Re-running the pipeline on the same day**
Safe — the pipeline deduplicates history rows by `(composite_id, scraped_at, exists_on_source)` so duplicate rows are not accumulated.
