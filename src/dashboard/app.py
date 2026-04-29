import math
import os
from datetime import timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from src.analytics.service import (
    get_active_listings,
    get_data_quality,
    get_listing_history,
    get_market_districts,
    get_market_overview,
    get_market_price_movements,
    get_market_timeseries,
    load_market_data,
)


st.set_page_config(page_title="Prague Real Estate Intelligence", layout="wide")

st.markdown(
    """
    <style>
    .stApp { background: radial-gradient(circle at top left, #1a2440 0%, #0a0f1d 48%, #070b14 100%); color: #edf2ff; }
    .block-container { max-width: 1600px; padding-top: 1.2rem; padding-bottom: 2rem; }
    h1, h2, h3, label, .stMarkdown, .stCaption, p, span { color: #edf2ff !important; }
    [data-testid="stSidebar"] { background: rgba(9, 14, 27, 0.98); border-right: 1px solid rgba(116, 148, 255, 0.18); }
    [data-testid="stMetric"], .panel {
        background: linear-gradient(180deg, rgba(17,25,46,0.96) 0%, rgba(10,16,32,0.96) 100%);
        border: 1px solid rgba(101,130,215,0.20);
        border-radius: 20px;
        box-shadow: 0 18px 48px rgba(0, 0, 0, 0.22);
        padding: 16px 18px;
    }
    .hero {
        background: linear-gradient(135deg, rgba(30,48,92,0.92), rgba(11,16,30,0.96));
        border: 1px solid rgba(122,157,255,0.24);
        border-radius: 24px;
        padding: 24px 26px;
        margin-bottom: 18px;
    }
    .hero-kicker { color: #88b6ff !important; letter-spacing: 0.12em; text-transform: uppercase; font-size: 0.76rem; }
    .hero-title { font-size: 2rem; font-weight: 700; margin: 0.3rem 0 0.6rem 0; }
    .hero-copy { color: #a8b6d9 !important; max-width: 880px; }
    .kpi-card {
        background: linear-gradient(180deg, rgba(17,25,46,0.98) 0%, rgba(8,12,22,0.98) 100%);
        border: 1px solid rgba(103,134,212,0.2);
        border-radius: 22px;
        padding: 18px;
        min-height: 148px;
    }
    .kpi-label { color: #8ea4d6 !important; text-transform: uppercase; letter-spacing: 0.08em; font-size: 0.72rem; }
    .kpi-value { font-size: 1.8rem; font-weight: 700; margin-top: 0.45rem; }
    .kpi-delta { font-size: 0.95rem; margin-top: 0.55rem; }
    .kpi-subtle { color: #8fa1c9 !important; font-size: 0.85rem; margin-top: 0.55rem; }
    .delta-up { color: #6ee7b7 !important; }
    .delta-down { color: #ff8f8f !important; }
    .delta-flat { color: #f7d47c !important; }
    .section-title { margin-top: 1.6rem; margin-bottom: 0.7rem; }
    .small-note { color: #92a3cb !important; }
    .dataframe tbody tr:hover { background: rgba(114, 143, 255, 0.08); }
    </style>
    """,
    unsafe_allow_html=True,
)


def fmt_int(value):
    if value is None or pd.isna(value):
        return "n/a"
    return f"{int(round(value)):,}".replace(",", " ")


def fmt_czk(value):
    if value is None or pd.isna(value):
        return "n/a"
    return f"{int(round(value)):,} Kč".replace(",", " ")


def fmt_days(value):
    if value is None or pd.isna(value):
        return "n/a"
    return f"{float(value):.1f} d"


def fmt_pct(value):
    if value is None or pd.isna(value):
        return "n/a"
    sign = "+" if value > 0 else ""
    return f"{sign}{value:.2f}%"


def render_kpi_card(title, metric, formatter, subtitle=None):
    current_value = metric.get("current") if isinstance(metric, dict) else metric
    delta_value = metric.get("delta") if isinstance(metric, dict) else None
    pct_value = metric.get("pct_change") if isinstance(metric, dict) else None
    arrow = "→"
    delta_class = "delta-flat"
    if delta_value is not None and not pd.isna(delta_value):
        if delta_value > 0:
            arrow = "↑"
            delta_class = "delta-up"
        elif delta_value < 0:
            arrow = "↓"
            delta_class = "delta-down"
    delta_text = "No comparison snapshot"
    if delta_value is not None and not pd.isna(delta_value):
        delta_text = f"{arrow} {formatter(delta_value)}"
        if pct_value is not None and not pd.isna(pct_value):
            delta_text = f"{delta_text} ({fmt_pct(pct_value)})"
    subtitle_text = subtitle or ""
    st.markdown(
        f"""
        <div class="kpi-card">
            <div class="kpi-label">{title}</div>
            <div class="kpi-value">{formatter(current_value)}</div>
            <div class="kpi-delta {delta_class}">{delta_text}</div>
            <div class="kpi-subtle">{subtitle_text}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def empty_panel(message):
    st.markdown(f"<div class='panel small-note'>{message}</div>", unsafe_allow_html=True)


data_path = "data/listings_processed.csv"
if not os.path.exists(data_path):
    st.warning("No processed dataset found. Run `python run_pipeline.py` first.")
    st.stop()

with st.spinner("Loading market intelligence datasets..."):
    bundle = load_market_data()

if bundle.current_df.empty:
    st.warning("The processed dataset is empty. Run the pipeline again after scraping completes.")
    st.stop()

history_dates = sorted(date for date in bundle.history_df.get("snapshot_date", pd.Series(dtype="object")).dropna().unique().tolist())
default_start = history_dates[0] if history_dates else None
default_end = history_dates[-1] if history_dates else None
active_df = bundle.current_df[bundle.current_df["is_active"] == True].copy() if "is_active" in bundle.current_df.columns else bundle.current_df.copy()

with st.sidebar:
    st.header("Filters")
    date_range = None
    if history_dates:
        chosen_dates = st.date_input(
            "Snapshot range",
            value=(default_start, default_end) if default_start and default_end and default_start != default_end else default_end,
            min_value=default_start,
            max_value=default_end,
        )
        if isinstance(chosen_dates, tuple):
            date_range = chosen_dates
        else:
            date_range = (chosen_dates - timedelta(days=30), chosen_dates)

    source_options = sorted([value for value in active_df.get("source", pd.Series(dtype=str)).dropna().unique().tolist() if str(value).strip()])
    property_options = sorted([value for value in active_df.get("property_type", pd.Series(dtype=str)).dropna().unique().tolist() if str(value).strip()])
    district_options = sorted([value for value in active_df.get("district_name", pd.Series(dtype=str)).dropna().unique().tolist() if str(value).strip()])
    borough_options = sorted([value for value in active_df.get("borough_name", pd.Series(dtype=str)).dropna().unique().tolist() if str(value).strip()])
    seller_options = sorted([value for value in active_df.get("seller_type", pd.Series(dtype=str)).dropna().unique().tolist() if str(value).strip()])

    selected_sources = st.multiselect("Source", source_options, default=source_options)
    selected_property_types = st.multiselect("Property type", property_options, default=property_options)
    selected_districts = st.multiselect("District", district_options, default=district_options)
    selected_boroughs = st.multiselect("Borough", borough_options, default=borough_options)
    selected_sellers = st.multiselect("Seller type", seller_options, default=seller_options)
    search_term = st.text_input("Search listing title or address")

    price_values = active_df["price_czk"].dropna() if "price_czk" in active_df.columns else pd.Series(dtype=float)
    size_values = active_df["area_m2"].dropna() if "area_m2" in active_df.columns else pd.Series(dtype=float)
    price_min = int(price_values.min()) if not price_values.empty else 0
    price_max = int(price_values.max()) if not price_values.empty else 0
    size_min = int(size_values.min()) if not size_values.empty else 0
    size_max = int(size_values.max()) if not size_values.empty else 0

    selected_price_range = (price_min, price_max)
    selected_size_range = (size_min, size_max)
    if price_max > price_min:
        selected_price_range = st.slider("Price range (CZK)", min_value=price_min, max_value=price_max, value=(price_min, price_max), step=max(100000, (price_max - price_min) // 100))
    if size_max > size_min:
        selected_size_range = st.slider("Size range (m²)", min_value=size_min, max_value=size_max, value=(size_min, size_max))

filters = {
    "date_range": date_range,
    "sources": selected_sources,
    "property_types": selected_property_types,
    "districts": selected_districts,
    "boroughs": selected_boroughs,
    "seller_types": selected_sellers,
    "search": search_term,
    "price_range": selected_price_range,
    "size_range": selected_size_range,
}

overview = get_market_overview(bundle, filters)
timeseries = get_market_timeseries(bundle, filters)
districts_df = get_market_districts(bundle, filters)
movements_df = get_market_price_movements(bundle, filters)
active_listings_df = get_active_listings(bundle, filters)
quality = get_data_quality(bundle, filters)

if active_listings_df.empty:
    st.markdown(
        """
        <div class="hero">
            <div class="hero-kicker">Prague Market Intelligence</div>
            <div class="hero-title">No listings match the current filter set</div>
            <div class="hero-copy">Widen the district, source, or price filters to bring listings back into scope. Time-series history remains filter-aware and compares against the previous available snapshot inside the chosen window.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.stop()

latest_date = overview.get("latest_snapshot_date")
previous_date = overview.get("previous_snapshot_date")
comparison_text = "Latest available snapshot"
if latest_date and previous_date:
    comparison_text = f"Comparing {latest_date} vs previous available snapshot {previous_date}"
elif latest_date:
    comparison_text = f"Only one snapshot available in the current filtered range: {latest_date}"

st.markdown(
    f"""
    <div class="hero">
        <div class="hero-kicker">Prague Market Intelligence</div>
        <div class="hero-title">Daily listing intelligence for analysts, investors, and real-estate operators</div>
        <div class="hero-copy">
            Tracks active inventory, pricing, removals, and district-level movement using scrape snapshots. {comparison_text}.
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

kpi_row_1 = st.columns(4)
with kpi_row_1[0]:
    render_kpi_card("Active Listings", overview["active_listings"], fmt_int, "Current active inventory in filtered scope")
with kpi_row_1[1]:
    render_kpi_card("Total Market Value", overview["total_market_value"], fmt_czk, "Aggregate asking value of current active listings")
with kpi_row_1[2]:
    render_kpi_card("Median Listing Price", overview["median_listing_price"], fmt_czk, "Median asking price across current active listings")
with kpi_row_1[3]:
    render_kpi_card("Average Listing Price", overview["average_listing_price"], fmt_czk, "Average asking price across current active listings")

kpi_row_2 = st.columns(4)
with kpi_row_2[0]:
    render_kpi_card("Median Price / m²", overview["median_price_per_sqm"], fmt_czk, "Median price efficiency for listings with area data")
with kpi_row_2[1]:
    render_kpi_card("New Listings", {"current": overview["new_listings"], "delta": None, "pct_change": None}, fmt_int, "IDs present now but not in previous snapshot")
with kpi_row_2[2]:
    render_kpi_card("Removed Listings", {"current": overview["removed_listings"], "delta": None, "pct_change": None}, fmt_int, "IDs missing now but present in previous snapshot")
with kpi_row_2[3]:
    render_kpi_card("Median Days on Market", {"current": overview["days_on_market_median"], "delta": None, "pct_change": None}, fmt_days, "Median duration of still-active filtered listings")

st.markdown("<h2 class='section-title'>Market Overview</h2>", unsafe_allow_html=True)
trend_left, trend_right = st.columns(2)

if not timeseries.empty:
    with trend_left:
        fig_active = px.line(
            timeseries,
            x="snapshot_date",
            y=["active_listings", "new_listings", "removed_listings"],
            markers=True,
            title="Inventory and churn by snapshot",
            color_discrete_sequence=["#7da0ff", "#6ee7b7", "#ff8f8f"],
        )
        fig_active.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", legend_title_text="")
        st.plotly_chart(fig_active, use_container_width=True)

    with trend_right:
        fig_value = go.Figure()
        fig_value.add_trace(go.Scatter(x=timeseries["snapshot_date"], y=timeseries["total_market_value_czk"], mode="lines+markers", name="Total market value", line=dict(color="#7da0ff", width=3)))
        fig_value.add_trace(go.Scatter(x=timeseries["snapshot_date"], y=timeseries["median_price_czk"], mode="lines+markers", name="Median price", line=dict(color="#f7d47c", width=2)))
        fig_value.update_layout(title="Value and price trend", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_value, use_container_width=True)
else:
    with trend_left:
        empty_panel("No multi-snapshot time-series is available yet. Run the pipeline on more than one day to unlock daily trend charts.")
    with trend_right:
        empty_panel("Price and value trend charts will appear once at least two snapshots exist in history.")

st.markdown("<h2 class='section-title'>District Analysis</h2>", unsafe_allow_html=True)
district_left, district_right = st.columns(2)
if not districts_df.empty:
    with district_left:
        fig_district_value = px.bar(
            districts_df.head(15),
            x="district_name",
            y="total_market_value_czk",
            color="active_listings",
            title="Top districts by current market value",
            hover_data=["borough_name", "median_price_czk", "median_price_per_m2_czk", "active_listings_delta"],
            color_continuous_scale=["#0ea5e9", "#7da0ff", "#8b5cf6"],
        )
        fig_district_value.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_district_value, use_container_width=True)

    with district_right:
        fig_district_psm = px.bar(
            districts_df.dropna(subset=["median_price_per_m2_czk"]).head(15),
            x="district_name",
            y="median_price_per_m2_czk",
            color="average_days_on_market",
            title="Median price per m² by district",
            hover_data=["borough_name", "active_listings", "active_listings_delta"],
            color_continuous_scale=["#14532d", "#22c55e", "#86efac"],
        )
        fig_district_psm.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_district_psm, use_container_width=True)
else:
    with district_left:
        empty_panel("District analytics are unavailable for the current filter set.")
    with district_right:
        empty_panel("Borough and district breakdowns need at least one active listing in scope.")

st.markdown("<h2 class='section-title'>Price and Duration Distribution</h2>", unsafe_allow_html=True)
distribution_left, distribution_right = st.columns(2)
with distribution_left:
    if "price_czk" in active_listings_df.columns and active_listings_df["price_czk"].dropna().any():
        fig_hist_price = px.histogram(active_listings_df.dropna(subset=["price_czk"]), x="price_czk", nbins=30, title="Asking price distribution", color_discrete_sequence=["#7da0ff"])
        fig_hist_price.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_hist_price, use_container_width=True)
    else:
        empty_panel("Price distribution needs valid `price_czk` data.")
with distribution_right:
    if "listing_duration_days" in active_listings_df.columns and active_listings_df["listing_duration_days"].dropna().any():
        fig_hist_days = px.histogram(active_listings_df.dropna(subset=["listing_duration_days"]), x="listing_duration_days", nbins=30, title="Days on market distribution", color_discrete_sequence=["#f7d47c"])
        fig_hist_days.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_hist_days, use_container_width=True)
    else:
        empty_panel("Days-on-market distribution needs valid duration data.")

st.markdown("<h2 class='section-title'>Market Movements</h2>", unsafe_allow_html=True)
if not movements_df.empty:
    movement_counts = movements_df["movement"].value_counts().rename_axis("movement").reset_index(name="count")
    fig_movement = px.bar(movement_counts, x="movement", y="count", color="movement", title="Listing movement since previous available snapshot", color_discrete_map={
        "new": "#6ee7b7",
        "removed": "#ff8f8f",
        "price_increase": "#7da0ff",
        "price_reduction": "#f7d47c",
    })
    fig_movement.update_layout(showlegend=False, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
    st.plotly_chart(fig_movement, use_container_width=True)
    st.dataframe(movements_df, use_container_width=True, hide_index=True)
else:
    empty_panel("No new, removed, or price-changed listings were detected between the latest and previous available snapshots in the current filter scope.")

st.markdown("<h2 class='section-title'>Active Listings</h2>", unsafe_allow_html=True)
rows_per_page = st.selectbox("Rows per page", [25, 50, 100], index=1)
total_rows = len(active_listings_df)
page_count = max(1, math.ceil(total_rows / rows_per_page))
page_number = st.number_input("Page", min_value=1, max_value=page_count, value=1, step=1)
start = (page_number - 1) * rows_per_page
end = start + rows_per_page

listing_columns = [
    column for column in [
        "source", "property_type", "district_name", "borough_name", "title", "price_czk", "price_per_m2_czk",
        "area_m2", "seller_type", "listing_duration_days", "first_seen_at", "property_link", "location_quality"
    ] if column in active_listings_df.columns
]
st.caption(f"Showing rows {start + 1}-{min(end, total_rows)} of {total_rows}")
st.dataframe(active_listings_df.iloc[start:end][listing_columns], use_container_width=True, hide_index=True)

listing_options = active_listings_df.get("composite_id", pd.Series(dtype=str)).dropna().tolist()
if listing_options:
    selected_listing = st.selectbox("Inspect listing history", listing_options)
    history_df = get_listing_history(bundle, selected_listing)
    if not history_df.empty:
        st.dataframe(history_df, use_container_width=True, hide_index=True)
    else:
        empty_panel("No listing history was found for the selected record.")

st.markdown("<h2 class='section-title'>Data Quality and Coverage</h2>", unsafe_allow_html=True)
quality_row = st.columns(5)
quality_row[0].metric("Total rows", fmt_int(quality["total_records"]))
quality_row[1].metric("Active rows", fmt_int(quality["active_records"]))
quality_row[2].metric("Missing prices", fmt_int(quality["missing_price"]))
quality_row[3].metric("Missing area", fmt_int(quality["missing_area"]))
quality_row[4].metric("Location issues", fmt_int(quality["location_issues"]))

if quality["location_issue_examples"]:
    st.caption("Sample records flagged by the new district / borough sanity checks")
    st.dataframe(pd.DataFrame(quality["location_issue_examples"]), use_container_width=True, hide_index=True)
else:
    empty_panel("Location sanity checks passed for the current filtered scope.")
