import pandas as pd
from src.utils.logger import get_logger

logger = get_logger("reports")


def _normalize_datetime_column(frame: pd.DataFrame, column_name: str) -> pd.DataFrame:
    if column_name not in frame.columns:
        return frame
    out = frame.copy()
    out[column_name] = pd.to_datetime(out[column_name], errors="coerce", utc=True)
    return out


def generate_daily_price_csv(history_df, output_path="data/daily_price_report.csv"):
    logger.info("STAGE: Daily report CSV generation started")
    if history_df.empty:
        pd.DataFrame().to_csv(output_path, index=False)
        return output_path
    active = history_df[history_df["exists_on_source"] == True].copy()
    active = _normalize_datetime_column(active, "scraped_at")
    report = (
        active.groupby(["scraped_at", "source", "property_type", "district_name", "borough_name"], dropna=False)
        .agg(
            listing_count=("composite_id", "count"),
            total_market_value_czk=("price_czk", "sum"),
            average_asking_price_czk=("price_czk", "mean"),
            median_asking_price_czk=("price_czk", "median"),
            median_price_per_m2_czk=("price_per_m2_czk", "median"),
            average_area_m2=("area_m2", "mean"),
        )
        .reset_index()
        .sort_values(["scraped_at", "source", "property_type", "district_name"])
    )
    report.to_csv(output_path, index=False)
    logger.info(f"STAGE: Daily report CSV generation finished | rows: {len(report)}")
    return output_path


def generate_removed_listings_csv(current_df, output_path="data/removed_listings.csv"):
    logger.info("STAGE: Removed listings CSV generation started")
    if current_df.empty:
        pd.DataFrame().to_csv(output_path, index=False)
        return output_path
    removed = current_df[current_df["is_removed"] == True].copy() if "is_removed" in current_df.columns else pd.DataFrame()
    removed = _normalize_datetime_column(removed, "removed_at")
    if removed.empty:
        removed.to_csv(output_path, index=False)
        logger.info("STAGE: Removed listings CSV generation finished | empty")
        return output_path
    keep_cols = [c for c in [
        "composite_id", "source", "property_type", "property_link", "title",
        "full_address", "district_name", "borough_name", "prague_zone", "price_czk",
        "first_seen_at", "removed_at", "removed_duration_days", "seller_type"
    ] if c in removed.columns]
    removed = removed[keep_cols].sort_values(["removed_at", "source", "property_type"], ascending=[False, True, True])
    removed.to_csv(output_path, index=False)
    logger.info(f"STAGE: Removed listings CSV generation finished | rows: {len(removed)}")
    return output_path


def generate_market_report_html(current_df, output_path="data/market_report.html"):
    logger.info("STAGE: HTML report generation started")
    if current_df.empty:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write("<html><body><h1>No data yet</h1></body></html>")
        return output_path
    active = current_df[current_df["is_active"] == True].copy()
    total_market_value = active["price_czk"].sum() if "price_czk" in active.columns else None
    average_price = active["price_czk"].mean() if "price_czk" in active.columns else None
    average_duration = active["listing_duration_days"].mean() if "listing_duration_days" in active.columns else None
    by_source = (
        active.groupby("source", as_index=False)
        .agg(
            listing_count=("composite_id", "count"),
            total_market_value_czk=("price_czk", "sum"),
            average_asking_price_czk=("price_czk", "mean"),
        )
        .sort_values("total_market_value_czk", ascending=False)
    )
    by_district = (
        active.dropna(subset=["district_name"])
        .groupby(["district_name", "borough_name", "property_type"], as_index=False)
        .agg(
            listing_count=("composite_id", "count"),
            total_market_value_czk=("price_czk", "sum"),
            median_price_per_m2_czk=("price_per_m2_czk", "median"),
        )
        .sort_values("total_market_value_czk", ascending=False)
        .head(25)
    )
    def fmt_czk(x):
        if x is None or pd.isna(x):
            return "n/a"
        return f"{int(round(x)):,.0f} Kč".replace(",", " ")
    def fmt_days(x):
        if x is None or pd.isna(x):
            return "n/a"
        return f"{round(float(x),1)} days"
    html = f"""
<html><head><meta charset='utf-8'/>
<style>
body {{ font-family: Inter, Arial, sans-serif; background:#0c1326; color:#edf3ff; margin:0; padding:32px; }}
.container {{ max-width:1500px; margin:0 auto; }}
.grid {{ display:grid; grid-template-columns:repeat(3,1fr); gap:16px; margin-bottom:24px; }}
.card {{ background:#111a31; border:1px solid #24365f; border-radius:18px; padding:18px; }}
.label {{ color:#8ea3ca; font-size:12px; text-transform:uppercase; }}
.value {{ color:#7fe5c0; font-size:26px; font-weight:700; margin-top:8px; }}
.table {{ width:100%; border-collapse:collapse; background:#111a31; }}
.table th,.table td {{ padding:12px 14px; border-bottom:1px solid #223257; text-align:left; }}
.table th {{ background:#0d1429; color:#a7c0ff; }}
</style></head><body>
<div class='container'>
<h1>Prague Real Estate Market Report</h1>
<div class='grid'>
<div class='card'><div class='label'>Active listings</div><div class='value'>{len(active):,}</div></div>
<div class='card'><div class='label'>Total market value</div><div class='value'>{fmt_czk(total_market_value)}</div></div>
<div class='card'><div class='label'>Average asking price</div><div class='value'>{fmt_czk(average_price)}</div></div>
</div>
<p>Average listing duration: {fmt_days(average_duration)}</p>
<h2>Breakdown by source</h2>
{by_source.to_html(index=False, classes='table', border=0)}
<h2>Top district / property-type groups</h2>
{by_district.to_html(index=False, classes='table', border=0)}
</div></body></html>
"""
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)
    logger.info("STAGE: HTML report generation finished")
    return output_path
