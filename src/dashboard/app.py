import copy
import json
import math
from pathlib import Path
from datetime import timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from src.analytics.service import (
    get_active_listings,
    get_data_quality,
    get_listing_history,
    get_market_boroughs,
    get_market_districts,
    get_market_map_data,
    get_market_overview,
    get_market_price_movements,
    get_market_timeseries,
    get_source_inventory,
    load_market_data,
)


st.set_page_config(page_title="Prague Real Estate Intelligence", layout="wide")

THEME = {
    "bg_start": "#101722",
    "bg_mid": "#0a111b",
    "bg_end": "#060b12",
    "panel_top": "rgba(17, 24, 39, 0.96)",
    "panel_bottom": "rgba(8, 13, 24, 0.98)",
    "panel_border": "rgba(120, 143, 168, 0.20)",
    "hero_top": "rgba(24, 35, 52, 0.96)",
    "hero_bottom": "rgba(10, 16, 28, 0.98)",
    "hero_border": "rgba(104, 128, 156, 0.24)",
    "text": "#eef3f8",
    "muted_text": "#9aa9bb",
    "muted_text_soft": "#7f91a6",
    "accent": "#5f8fb5",
    "accent_soft": "#88adc6",
    "line_primary": "#6f95b8",
    "line_secondary": "#9fb9c9",
    "positive": "#5fa58f",
    "negative": "#c97777",
    "neutral": "#c4a46b",
    "choropleth": ["#0d1623", "#173247", "#23536d", "#3b7890", "#78a8b8"],
    "bar_teal": ["#173247", "#2c5b71", "#4f7f90"],
    "bar_blue": ["#29405b", "#4d6d8c", "#7f9eb6"],
}

st.markdown(
    """
    <style>
    .stApp { background: radial-gradient(circle at top left, #101722 0%, #0a111b 48%, #060b12 100%); color: #eef3f8; }
    .block-container { max-width: 1600px; padding-top: 1.2rem; padding-bottom: 2rem; }
    h1, h2, h3, label, .stMarkdown, .stCaption, p, span { color: #eef3f8 !important; }
    [data-testid="stSidebar"] { background: rgba(8, 13, 24, 0.98); border-right: 1px solid rgba(120, 143, 168, 0.18); }
    [data-testid="stMetric"], .panel {
        background: linear-gradient(180deg, rgba(17, 24, 39, 0.96) 0%, rgba(8, 13, 24, 0.98) 100%);
        border: 1px solid rgba(120, 143, 168, 0.20);
        border-radius: 20px;
        box-shadow: 0 18px 48px rgba(0, 0, 0, 0.22);
        padding: 16px 18px;
    }
    .hero {
        background: linear-gradient(135deg, rgba(24, 35, 52, 0.96), rgba(10, 16, 28, 0.98));
        border: 1px solid rgba(104, 128, 156, 0.24);
        border-radius: 24px;
        padding: 24px 26px;
        margin-bottom: 18px;
    }
    .hero-kicker { color: #88adc6 !important; letter-spacing: 0.12em; text-transform: uppercase; font-size: 0.76rem; }
    .hero-title { font-size: 2rem; font-weight: 700; margin: 0.3rem 0 0.6rem 0; }
    .hero-copy { color: #9aa9bb !important; max-width: 880px; }
    .kpi-card {
        background: linear-gradient(180deg, rgba(17, 24, 39, 0.98) 0%, rgba(8, 13, 24, 0.98) 100%);
        border: 1px solid rgba(120, 143, 168, 0.2);
        border-radius: 22px;
        padding: 18px;
        min-height: 148px;
    }
    .kpi-label { color: #88adc6 !important; text-transform: uppercase; letter-spacing: 0.08em; font-size: 0.72rem; }
    .kpi-value { font-size: 1.8rem; font-weight: 700; margin-top: 0.45rem; }
    .kpi-delta { font-size: 0.95rem; margin-top: 0.55rem; }
    .kpi-subtle { color: #7f91a6 !important; font-size: 0.85rem; margin-top: 0.55rem; }
    .delta-up { color: #5fa58f !important; }
    .delta-down { color: #c97777 !important; }
    .delta-flat { color: #c4a46b !important; }
    .section-title { margin-top: 1.6rem; margin-bottom: 0.7rem; }
    .small-note { color: #7f91a6 !important; }
    .dataframe tbody tr:hover { background: rgba(95, 143, 181, 0.08); }
    </style>
    """,
    unsafe_allow_html=True,
)


def apply_plot_theme(fig):
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color=THEME["text"]),
        title_font=dict(color=THEME["text"], size=18),
        legend=dict(font=dict(color=THEME["muted_text"])),
        margin=dict(l=24, r=24, t=48, b=24),
    )
    fig.update_xaxes(showgrid=True, gridcolor="rgba(127, 145, 166, 0.16)", zeroline=False, color=THEME["muted_text"])
    fig.update_yaxes(showgrid=True, gridcolor="rgba(127, 145, 166, 0.16)", zeroline=False, color=THEME["muted_text"])
    return fig


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


@st.cache_data(show_spinner=False)
def load_prague_boundary_geojson():
    path = Path("data/reference/prague_city_parts.geojson")
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def build_choropleth_frame(geojson, borough_frame, district_frame):
    if not geojson:
        return pd.DataFrame()

    def _collapse_lookup(frame, key_column):
        if frame is None or frame.empty or key_column not in frame.columns:
            return {}
        aggregations = {}
        if "borough_name" in frame.columns:
            aggregations["borough_name"] = ("borough_name", "first")
        if "district_name" in frame.columns:
            aggregations["district_name"] = ("district_name", "first")
        if "region_name" in frame.columns:
            aggregations["region_name"] = ("region_name", "first")
        if "active_listings" in frame.columns:
            aggregations["active_listings"] = ("active_listings", "sum")
        if "median_price_czk" in frame.columns:
            aggregations["median_price_czk"] = ("median_price_czk", "median")
        if "median_price_per_m2_czk" in frame.columns:
            aggregations["median_price_per_m2_czk"] = ("median_price_per_m2_czk", "median")
        if "total_market_value_czk" in frame.columns:
            aggregations["total_market_value_czk"] = ("total_market_value_czk", "sum")
        if "average_days_on_market" in frame.columns:
            aggregations["average_days_on_market"] = ("average_days_on_market", "mean")
        collapsed = frame.dropna(subset=[key_column]).groupby(key_column, as_index=False, dropna=False).agg(**aggregations)
        return collapsed.set_index(key_column).to_dict(orient="index")

    borough_lookup = _collapse_lookup(borough_frame, "borough_name")
    district_lookup = _collapse_lookup(district_frame, "district_name")

    rows = []
    for feature in geojson.get("features", []):
        props = feature.get("properties", {})
        primary_name = props.get("NAZEV_1")
        secondary_name = props.get("NAZEV_MC")
        feature_name = primary_name or secondary_name
        if not feature_name:
            continue

        aliases = [value for value in [feature_name, primary_name, secondary_name] if value]
        match = None
        matched_alias = feature_name
        match_level = "borough"
        for alias in aliases:
            match = borough_lookup.get(alias)
            if match is not None:
                matched_alias = alias
                break
        if match is None:
            match_level = "district"
            for alias in aliases:
                match = district_lookup.get(alias)
                if match is not None:
                    matched_alias = alias
                    break
        if match is None:
            continue

        rows.append(
            {
                "feature_name": feature_name,
                "feature_key": matched_alias,
                "match_level": match_level,
                "borough_name": match.get("borough_name"),
                "district_name": match.get("district_name"),
                "region_name": match.get("region_name"),
                "active_listings": match.get("active_listings"),
                "median_price_czk": match.get("median_price_czk"),
                "median_price_per_m2_czk": match.get("median_price_per_m2_czk"),
                "total_market_value_czk": match.get("total_market_value_czk"),
                "average_days_on_market": match.get("average_days_on_market"),
            }
        )
    return pd.DataFrame(rows)


def get_selection_points(event):
    if event is None:
        return []
    if isinstance(event, dict):
        selection = event.get("selection", event)
        return selection.get("points", [])
    selection = getattr(event, "selection", None)
    if selection is None:
        return []
    if isinstance(selection, dict):
        return selection.get("points", [])
    return getattr(selection, "points", []) or []


def update_map_filters_from_selection(selection_points):
    if not selection_points:
        return False
    selected_boroughs = set()
    selected_districts = set()
    for point in selection_points:
        custom_data = point.get("customdata") if isinstance(point, dict) else None
        if custom_data:
            borough_name = custom_data[0] if len(custom_data) > 0 else None
            district_name = custom_data[1] if len(custom_data) > 1 else None
            match_level = custom_data[2] if len(custom_data) > 2 else None
            if borough_name and match_level == "borough":
                selected_boroughs.add(borough_name)
            if district_name:
                selected_districts.add(district_name)
            continue
        location = point.get("location") if isinstance(point, dict) else None
        if location:
            selected_districts.add(location)
    selected_boroughs = sorted(selected_boroughs)
    selected_districts = sorted(selected_districts)
    if (
        selected_boroughs != st.session_state.get("map_borough_filter", [])
        or selected_districts != st.session_state.get("map_district_filter", [])
    ):
        st.session_state["map_borough_filter"] = selected_boroughs
        st.session_state["map_district_filter"] = selected_districts
        return True
    return False


with st.spinner("Loading market intelligence datasets..."):
    bundle = load_market_data()

if bundle.current_df.empty:
    st.warning("The processed dataset is empty. Run the pipeline again after scraping completes.")
    st.stop()

if "map_borough_filter" not in st.session_state:
    st.session_state["map_borough_filter"] = []
if "map_district_filter" not in st.session_state:
    st.session_state["map_district_filter"] = []

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
    region_options = sorted([value for value in active_df.get("region_name", pd.Series(dtype=str)).dropna().unique().tolist() if str(value).strip()])

    selected_source = st.selectbox(
        "Reporting source",
        source_options,
        help="Use one source at a time for KPIs and charts so portal overlap does not inflate totals.",
    )
    selected_regions = st.multiselect("Region", region_options, default=region_options)
    selected_property_types = st.multiselect("Property type", property_options, default=property_options)
    selected_districts = st.multiselect("District", district_options, default=district_options)
    selected_boroughs = st.multiselect("Borough", borough_options, default=borough_options)
    selected_sellers = st.multiselect("Seller type", seller_options, default=seller_options)
    search_term = st.text_input("Search listing title or address")

    if st.session_state["map_borough_filter"] or st.session_state["map_district_filter"]:
        st.caption("Map selection filter is active")
        st.write("Boroughs:", ", ".join(st.session_state["map_borough_filter"]) or "none")
        st.write("Districts:", ", ".join(st.session_state["map_district_filter"]) or "none")
        if st.button("Clear map selection"):
            st.session_state["map_borough_filter"] = []
            st.session_state["map_district_filter"] = []
            st.rerun()

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
    "sources": [selected_source] if selected_source else [],
    "regions": selected_regions,
    "property_types": selected_property_types,
    "districts": selected_districts,
    "boroughs": selected_boroughs,
    "seller_types": selected_sellers,
    "search": search_term,
    "price_range": selected_price_range,
    "size_range": selected_size_range,
}

if st.session_state["map_borough_filter"]:
    filters["boroughs"] = (
        sorted(set(filters["boroughs"]).intersection(st.session_state["map_borough_filter"]))
        if filters["boroughs"]
        else st.session_state["map_borough_filter"]
    )
if st.session_state["map_district_filter"]:
    filters["districts"] = (
        sorted(set(filters["districts"]).intersection(st.session_state["map_district_filter"]))
        if filters["districts"]
        else st.session_state["map_district_filter"]
    )

comparison_filters = dict(filters)
comparison_filters["sources"] = []

overview = get_market_overview(bundle, filters)
timeseries = get_market_timeseries(bundle, filters)
districts_df = get_market_districts(bundle, filters)
boroughs_df = get_market_boroughs(bundle, filters)
map_df = get_market_map_data(bundle, filters, grain="borough")
district_map_df = get_market_map_data(bundle, filters, grain="district")
movements_df = get_market_price_movements(bundle, filters)
active_listings_df = get_active_listings(bundle, filters)
quality = get_data_quality(bundle, filters)
source_inventory_df = get_source_inventory(bundle, comparison_filters)
prague_boundary_geojson = load_prague_boundary_geojson()
choropleth_df = build_choropleth_frame(prague_boundary_geojson, boroughs_df, district_map_df)

if active_listings_df.empty:
    st.markdown(
        """
        <div class="hero">
            <div class="hero-kicker">Prague Market Intelligence</div>
            <div class="hero-title">No listings match the current filter set</div>
            <div class="hero-copy">Widen the source, region, district, or price filters to bring listings back into scope.</div>
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
        <div class="hero-title">Portal-specific housing intelligence for Prague and the surrounding region</div>
        <div class="hero-copy">
            Tracks active inventory, pricing, removals, and borough-level movement using scrape snapshots. {comparison_text}. KPI totals below are currently shown for <strong>{selected_source}</strong> only to avoid duplicate counts across portals.
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown("<h2 class='section-title'>Source Coverage</h2>", unsafe_allow_html=True)
source_left, source_right = st.columns([1.05, 1.45])
with source_left:
    st.caption("This panel compares source coverage under the non-source filters. The dashboard itself stays on one source at a time.")
    if not source_inventory_df.empty:
        st.dataframe(source_inventory_df, use_container_width=True, hide_index=True)
    else:
        empty_panel("No source comparison data is available for the current filters.")
with source_right:
    if not source_inventory_df.empty:
        fig_source = px.bar(
            source_inventory_df,
            x="source",
            y="active_listings",
            color="median_price_czk",
            title="Active inventory available by source",
            hover_data=["total_market_value_czk", "median_price_per_m2_czk"],
            color_continuous_scale=THEME["bar_blue"],
        )
        apply_plot_theme(fig_source)
        st.plotly_chart(fig_source, use_container_width=True)
    else:
        empty_panel("Source comparison will appear once multiple portals have data inside the current non-source filter scope.")

kpi_row_1 = st.columns(4)
with kpi_row_1[0]:
    render_kpi_card("Active Listings", overview["active_listings"], fmt_int, "Current active inventory in selected source")
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

st.markdown("<h2 class='section-title'>Market Trends</h2>", unsafe_allow_html=True)
trend_left, trend_right = st.columns(2)
if not timeseries.empty:
    with trend_left:
        fig_active = px.line(
            timeseries,
            x="snapshot_date",
            y=["active_listings", "new_listings", "removed_listings"],
            markers=True,
            title="Inventory and churn by snapshot",
            color_discrete_sequence=[THEME["line_primary"], THEME["positive"], THEME["negative"]],
        )
        apply_plot_theme(fig_active)
        fig_active.update_layout(legend_title_text="")
        st.plotly_chart(fig_active, use_container_width=True)
    with trend_right:
        fig_value = go.Figure()
        fig_value.add_trace(go.Scatter(x=timeseries["snapshot_date"], y=timeseries["total_market_value_czk"], mode="lines+markers", name="Total market value", line=dict(color=THEME["line_primary"], width=3)))
        fig_value.add_trace(go.Scatter(x=timeseries["snapshot_date"], y=timeseries["median_price_czk"], mode="lines+markers", name="Median price", line=dict(color=THEME["neutral"], width=2)))
        fig_value.update_layout(title="Value and price trend")
        apply_plot_theme(fig_value)
        st.plotly_chart(fig_value, use_container_width=True)
else:
    with trend_left:
        empty_panel("No multi-snapshot time-series is available yet. Run the pipeline on more than one day to unlock daily trend charts.")
    with trend_right:
        empty_panel("Price and value trend charts will appear once at least two snapshots exist in history.")

st.markdown("<h2 class='section-title'>Location Intelligence</h2>", unsafe_allow_html=True)
location_left, location_right = st.columns([1.35, 1.0])

with location_left:
    if not choropleth_df.empty and prague_boundary_geojson:
        choropleth_geojson = copy.deepcopy(prague_boundary_geojson)
        for feature in choropleth_geojson.get("features", []):
            props = feature.setdefault("properties", {})
            props["__map_key"] = props.get("NAZEV_1") or props.get("NAZEV_MC")
        color_series = choropleth_df["median_price_per_m2_czk"].fillna(choropleth_df["median_price_czk"])
        zmin = float(color_series.quantile(0.08))
        zmax = float(color_series.quantile(0.92))
        if zmax <= zmin:
            zmin = float(color_series.min())
            zmax = float(color_series.max())
        plot_df = choropleth_df.assign(color_value=color_series)
        fig_map = px.choropleth(
            plot_df,
            geojson=choropleth_geojson,
            locations="feature_key",
            featureidkey="properties.__map_key",
            color="color_value",
            hover_name="feature_name",
            hover_data={
                "borough_name": True,
                "district_name": True,
                "region_name": True,
                "active_listings": True,
                "median_price_czk": ":,.0f",
                "median_price_per_m2_czk": ":,.0f",
                "average_days_on_market": ":.1f",
                "match_level": True,
                "feature_name": False,
                "feature_key": False,
                "color_value": False,
            },
            custom_data=["borough_name", "district_name", "match_level"],
            title="Median price choropleth by Prague borough / district",
            color_continuous_scale=THEME["choropleth"],
            opacity=0.76,
            height=520,
            range_color=(zmin, zmax),
        )
        fig_map.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=0, r=0, t=48, b=0),
            font=dict(color=THEME["text"]),
            geo=dict(
                fitbounds="locations",
                visible=False,
                bgcolor="rgba(0,0,0,0)",
                projection=dict(type="mercator"),
            ),
            coloraxis_colorbar=dict(
                title="Median price / m²",
                tickfont=dict(color=THEME["muted_text"]),
                titlefont=dict(color=THEME["text"]),
                bgcolor="rgba(8, 13, 24, 0.82)",
            ),
        )
        map_event = st.plotly_chart(
            fig_map,
            use_container_width=True,
            key="borough_price_choropleth",
            on_select="rerun",
            selection_mode="points",
        )
        if update_map_filters_from_selection(get_selection_points(map_event)):
            st.rerun()
    else:
        empty_panel("Choropleth view needs the local Prague boundary GeoJSON plus matching borough or district metrics.")

with location_right:
    if not boroughs_df.empty:
        fig_borough = px.bar(
            boroughs_df.head(15),
            x="median_price_czk",
            y="borough_name",
            color="active_listings",
            orientation="h",
            title="Top boroughs by median price",
            hover_data=["district_name", "region_name", "median_price_per_m2_czk", "average_days_on_market"],
            color_continuous_scale=THEME["bar_teal"],
        )
        apply_plot_theme(fig_borough)
        fig_borough.update_layout(margin=dict(l=170, r=24, t=48, b=24))
        fig_borough.update_yaxes(categoryorder="total ascending", automargin=True, tickfont={"size": 12})
        st.plotly_chart(fig_borough, use_container_width=True)
    else:
        empty_panel("No borough-level breakdown is available for the current source and filters.")

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
            hover_data=["borough_name", "region_name", "median_price_czk", "median_price_per_m2_czk", "active_listings_delta"],
            color_continuous_scale=THEME["bar_blue"],
        )
        apply_plot_theme(fig_district_value)
        st.plotly_chart(fig_district_value, use_container_width=True)
    with district_right:
        fig_district_psm = px.bar(
            districts_df.dropna(subset=["median_price_per_m2_czk"]).head(15),
            x="district_name",
            y="median_price_per_m2_czk",
            color="average_days_on_market",
            title="Median price per m² by district",
            hover_data=["borough_name", "region_name", "active_listings", "active_listings_delta"],
            color_continuous_scale=THEME["bar_teal"],
        )
        apply_plot_theme(fig_district_psm)
        st.plotly_chart(fig_district_psm, use_container_width=True)
else:
    with district_left:
        empty_panel("District analytics are unavailable for the current filter set.")
    with district_right:
        empty_panel("District and borough charts require at least one active listing in scope.")

st.markdown("<h2 class='section-title'>Price and Duration Distribution</h2>", unsafe_allow_html=True)
distribution_left, distribution_right = st.columns(2)
with distribution_left:
    if "price_czk" in active_listings_df.columns and active_listings_df["price_czk"].dropna().any():
        fig_hist_price = px.histogram(active_listings_df.dropna(subset=["price_czk"]), x="price_czk", nbins=30, title="Asking price distribution", color_discrete_sequence=[THEME["line_primary"]])
        apply_plot_theme(fig_hist_price)
        st.plotly_chart(fig_hist_price, use_container_width=True)
    else:
        empty_panel("Price distribution needs valid `price_czk` data.")
with distribution_right:
    if "listing_duration_days" in active_listings_df.columns and active_listings_df["listing_duration_days"].dropna().any():
        fig_hist_days = px.histogram(active_listings_df.dropna(subset=["listing_duration_days"]), x="listing_duration_days", nbins=30, title="Days on market distribution", color_discrete_sequence=[THEME["neutral"]])
        apply_plot_theme(fig_hist_days)
        st.plotly_chart(fig_hist_days, use_container_width=True)
    else:
        empty_panel("Days-on-market distribution needs valid duration data.")

st.markdown("<h2 class='section-title'>Market Movements</h2>", unsafe_allow_html=True)
if not movements_df.empty:
    movement_counts = movements_df["movement"].value_counts().rename_axis("movement").reset_index(name="count")
    fig_movement = px.bar(
        movement_counts,
        x="movement",
        y="count",
        color="movement",
        title="Listing movement since previous available snapshot",
        color_discrete_map={
            "new": THEME["positive"],
            "removed": THEME["negative"],
            "price_increase": THEME["line_primary"],
            "price_reduction": THEME["neutral"],
        },
    )
    apply_plot_theme(fig_movement)
    fig_movement.update_layout(showlegend=False)
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
        "source",
        "region_name",
        "property_type",
        "district_name",
        "borough_name",
        "title",
        "price_czk",
        "price_per_m2_czk",
        "area_m2",
        "seller_type",
        "listing_duration_days",
        "first_seen_at",
        "property_link",
        "location_quality",
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
    st.caption("Sample records flagged by the district / borough sanity checks")
    st.dataframe(pd.DataFrame(quality["location_issue_examples"]), use_container_width=True, hide_index=True)
else:
    empty_panel("Location sanity checks passed for the current filtered scope.")
