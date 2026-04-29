# Pipeline Diagnostics Report
_Generated: 2026-04-29_

---

## Summary

| Severity | Count |
|----------|-------|
| 🔴 Critical | 4 |
| 🟠 Medium | 4 |
| 🟡 Minor | 3 |

---

## 🔴 Critical Bugs

### 1. `detect_max_pages` in Sreality can return a price as a page count
**File:** `src/adapters/sreality.py` — `detect_max_pages()`

```python
for a in soup.find_all("a"):
    txt = clean_text(a.get_text())
    if txt and txt.isdigit():
        numbers.append(int(txt))
detected = max(numbers) if numbers else 1
```

The logic collects ALL digit-only text from every anchor tag on the page — including prices like `4990000` or square metres like `85` that happen to be inside `<a>` tags. It then takes the **maximum** value, which could return something like `4,990,000` as the page count. This causes the scraper to attempt millions of page fetches that all 404. **Bezrealitky's `detect_max_pages` is safer** — it correctly checks for `page=` or `/strana-` in the `href` before treating the text as a page number.

**Fix:** Filter by pagination-specific hrefs (e.g. `?strana=` or `/strana-`), just like Bezrealitky does:
```python
if ("strana=" in href or "/strana-" in href) and txt and txt.isdigit():
    numbers.append(int(txt))
```

---

### 2. `write_dataframe_replace` drops and recreates tables on every run
**File:** `src/db/io.py` — `write_dataframe_replace()`

```python
df.to_sql(table_name, conn, if_exists="replace", index=False)
```

`if_exists="replace"` drops the entire table and recreates it with pandas-inferred types on every single pipeline run. Consequences:
- All PostgreSQL indexes are destroyed each run.
- Any schema improvements made via `postgres_schema.sql` get wiped after the first run.
- Column data types are non-deterministic (pandas guesses them from the current batch's data).
- No proper `BIGINT`/`NUMERIC` types — everything becomes pandas defaults.

**Fix:** Use `if_exists="append"` with a pre-cleared table, or do a proper upsert. The `import_clean_csvs_to_postgres.py` already does this correctly — the main pipeline should use the same approach, or at minimum use `if_exists="append"` after a manual `TRUNCATE`.

---

### 3. `full_history` grows with duplicate rows on repeated runs
**File:** `src/pipeline.py` — `run_pipeline()`

```python
full_history = pd.concat([previous_history, history_snapshot], ignore_index=True) \
    if not previous_history.empty else history_snapshot.copy()
write_dataframe_replace(full_history, "listing_history")
```

`build_history_snapshot()` deduplicates the new snapshot before returning, but the `pd.concat` of previous history + new snapshot is **never deduplicated** before writing to the DB. Each pipeline run appends a new snapshot, but if the pipeline is run twice in the same day with the same data, you'll get duplicate `(composite_id, scraped_at, exists_on_source)` rows accumulating in the history table.

**Fix:** Add dedup immediately after the concat:
```python
full_history = full_history.drop_duplicates(
    subset=["composite_id", "scraped_at", "exists_on_source"], keep="last"
)
```

---

### 4. `_upsert_listings` will `KeyError` if a listing's source is missing from `source_map`
**File:** `src/db/import_clean_csvs_to_postgres.py` — `_upsert_listings()`

```python
source_code = row.get("source")
...
"source_id": source_map[source_code],
```

If `source_code` is `None` (missing data), an empty string, or any value not in the sources config, this raises an unhandled `KeyError` and aborts the entire import. Given that missing data is expected (especially from Bezrealitky), this is a silent data-quality timebomb.

**Fix:** Add a guard:
```python
source_id = source_map.get(source_code)
if source_id is None:
    logger.warning(f"Unknown source '{source_code}' for composite_id {row.get('composite_id')} — skipping")
    continue
```

---

## 🟠 Medium Bugs

### 5. Bezrealitky `ownership_type` is hardcoded to `"owner"`
**File:** `src/adapters/bezrealitky.py` — `parse_detail_page()`

```python
out["seller_type"] = "owner"
out["ownership_type"] = "owner"
```

Both fields are hardcoded to `"owner"` regardless of what the page actually says. The Sreality adapter correctly parses `ownership_type` from the page text (looking for "osobní", "družstevní", etc.), and `seller_type` based on whether agency keywords appear. The Bezrealitky adapter does not do this — every listing from this source will have `ownership_type = "owner"`, which is incorrect for agency listings.

**Fix:** Apply the same text-matching logic as in the Sreality adapter:
```python
out["seller_type"] = "owner"  # bezrealitky is owner-only by design, so this is actually correct
out["ownership_type"] = None
for val in ["osobní", "družstevní", "státní/obecní"]:
    if val in lower:
        out["ownership_type"] = val
        break
```

---

### 6. No rate limiting / delay between page fetches
**Files:** `src/adapters/sreality.py`, `src/adapters/bezrealitky.py` — `scrape()`

The page loop fetches pages back-to-back with no sleep:
```python
for page in range(1, max_pages + 1):
    html = self.fetch(f"{base_url}?strana={page}")
```

Only the retry backoff (`time.sleep(backoff * attempt)`) introduces any delay, and only on failures. This will trigger rate limiting or IP bans on both sites. The detail-page enrichment is threaded (4 workers), making the burst even heavier.

**Fix:** Add a small `time.sleep(0.5)` (or randomized `time.sleep(random.uniform(0.3, 1.0))`) between page fetches inside the loop.

---

### 7. `looks_like_listing_title` used as scrape-time filter is too strict
**Files:** `src/adapters/sreality.py`, `src/adapters/bezrealitky.py` — `parse_listing_cards()`

```python
title = clean_text(a.get_text(" ", strip=True))
if not looks_like_listing_title(title):
    continue
```

`looks_like_listing_title` requires the title to contain both a property keyword AND either a price or an area. However, the anchor text scraped from list pages is often just a property name or address — the price/area appear elsewhere in the card (in separate `<span>` or `<div>` elements). This means **valid listings whose anchor text lacks price or area get silently dropped** before they even reach the detail-scraping stage.

The filter was designed to remove navigation links (which is valid), but it's applied too aggressively on listing-card anchors where the title is not expected to contain full pricing info.

**Fix:** Keep a minimal filter (length > 8, not in known bad titles) for the scrape-time pass. Apply the stricter `has_price or has_area` check only in `process_master_csv`, which runs after detail enrichment has added the price.

---

### 8. `_insert_snapshots_and_events` uses fragile timestamp key lookup
**File:** `src/db/import_clean_csvs_to_postgres.py` — `_insert_snapshots_and_events()`

```python
scrape_run_id = int(
    current_snapshot["scraped_at"].dropna()
    .map(lambda value: scrape_run_map[pd.Timestamp(value)])
    .iloc[0]
)
```

`scrape_run_map` is keyed by `pd.Timestamp(scraped_at)` from `groupby("scraped_at")`. When rows are reconstructed from CSV and then converted back with `pd.Timestamp(value)`, nanosecond-level precision differences can cause the key lookup to fail with a `KeyError`, aborting the entire import silently partway through.

**Fix:** Round/truncate timestamps to second precision consistently when building and looking up the map:
```python
scrape_run_map = {pd.Timestamp(k).floor("s"): v for k, v in scrape_run_map.items()}
# and when looking up:
.map(lambda value: scrape_run_map[pd.Timestamp(value).floor("s")])
```

---

## 🟡 Minor Issues

### 9. `extract_url_id` in Bezrealitky falls back to the full URL
**File:** `src/adapters/bezrealitky.py`

```python
def extract_url_id(self, url):
    m = re.search(r"/(\d+)(?:-|/|$)", url)
    return m.group(1) if m else url  # ← returns full URL if no match
```

If the regex fails (e.g. a URL with no numeric segment), the entire URL becomes the `url_id`, which then becomes part of the `composite_id`. This creates an extremely long, non-deterministic composite key. Sreality has the same pattern.

**Fix:** Return `None` on failure and skip the row, or extract the last path segment as a fallback:
```python
return m.group(1) if m else None
```

---

### 10. `previous_removed_df["snapshot_date"]` assignment is a no-op or silently nulls the column
**File:** `src/utils/state.py` — `reconcile_current_with_previous()`

```python
previous_removed_df["snapshot_date"] = previous_removed_df.get("snapshot_date")
```

`DataFrame.get("snapshot_date")` returns the Series if the column exists (no-op) or `None` if it doesn't (which would set all values to `None`, wiping the column). The intent is likely just to preserve the existing snapshot_date, so this line is at best a no-op and at worst data-destroying. It can be safely removed.

---

### 11. `init_db()` does nothing
**File:** `src/db/io.py`

```python
def init_db():
    logger.info("STAGE: Database init checked")
```

`init_db()` is called at the start of every pipeline run but contains no actual logic — it just logs. Any schema initialization (creating tables, indexes) must be done separately by running `postgres_schema.py`. If the schema doesn't exist and the pipeline runs, `write_dataframe_replace` will auto-create bare tables with no indexes, silently bypassing the intended schema.

**Fix:** Either move the schema application into `init_db()`, or at minimum add a check:
```python
def init_db():
    from src.db.postgres_schema import apply_postgres_schema
    apply_postgres_schema()
    logger.info("STAGE: Database schema initialized")
```

---

## Does Bezrealitky Scraping Work?

**Structurally, yes** — the adapter is architecturally correct. It has retry logic, threading, deduplication, and graceful error handling. However it has two issues that likely cause it to return sparse or no data in practice:

1. **The `looks_like_listing_title` filter** (Bug #7) will silently drop listings whose anchor text doesn't contain price/area, which is common on modern listing cards.
2. **No rate limiting** (Bug #6) makes it likely the site responds with 429/captcha after the first few pages.
3. **`ownership_type` is hardcoded** (Bug #5), so the data it does collect will have incorrect metadata.

The adapter won't crash outright — failures at the page or detail level are caught and logged — but the output dataset will likely be incomplete or empty.

---

## Are Safety Nets in Place for Missing Data?

**Partial.** Here's the breakdown:

| Safety net | Status |
|---|---|
| Missing columns filled with `None` via `_ensure()` | ✅ Yes — applied in `state.py` |
| Missing `composite_id` reconstructed from parts | ✅ Yes — `_ensure_composite_id()` in `state.py` |
| Empty scrape returns empty DataFrame, not crash | ✅ Yes — handled in `pipeline.py` |
| `safe_float` / `safe_int` for numeric fields | ✅ Yes — used throughout |
| Detail page failures skip row, don't abort | ✅ Yes — caught per-row in adapters |
| Missing source in `source_map` during import | ❌ No — `KeyError` (Bug #4) |
| History deduplication after concat | ❌ No — (Bug #3) |
| Schema guarantees on DB write | ❌ No — table is dropped/recreated (Bug #2) |
| Rate limiting / anti-ban protection | ❌ No — (Bug #6) |

---

## Recommended Fix Priority

1. Fix `detect_max_pages` in Sreality (Bug #1) — prevents runaway page loops
2. Add dedup to `full_history` concat in `pipeline.py` (Bug #3) — data integrity
3. Add `source_map.get()` guard in `_upsert_listings` (Bug #4) — prevents import crashes
4. Add rate limiting to both adapters (Bug #6) — prevents bans
5. Fix `write_dataframe_replace` strategy (Bug #2) — schema/index stability
6. Relax `looks_like_listing_title` at scrape time (Bug #7) — yield more listings
7. Fix Bezrealitky `ownership_type` parsing (Bug #5)
8. Fix timestamp precision in `scrape_run_map` (Bug #8)
