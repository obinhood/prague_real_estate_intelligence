# Prague Real Estate Intelligence v8

Complete rebuild of the Prague real estate tracker.

Includes:
- Sreality scraper
- Optional Bezrealitky scraper
- PostgreSQL + CSV outputs
- History snapshots
- Removed listings CSV
- Listing duration metrics
- Price change tracking
- Professional dashboard
- HTML report
- Fixed SQLAlchemy / pandas DB I/O
- NumPy pinned below 2 to avoid compatibility issues

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

## Run

```bash
python run_pipeline.py
streamlit run src/dashboard/app.py
```

## Optional reset

```bash
python reset_db.py
```
