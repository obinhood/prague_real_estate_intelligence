import os
import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="Prague Real Estate Intelligence", layout="wide")

st.markdown(
    """
    <style>
    .stApp { background: #0c1326; color: #edf3ff; }
    .block-container { max-width: 1600px; padding-top: 1rem; padding-bottom: 2rem; }
    h1, h2, h3, label, .stMarkdown, .stCaption, p { color: #edf3ff !important; }
    [data-testid="stMetric"] {
        background: #111a31;
        border: 1px solid #24365f;
        padding: 14px 18px;
        border-radius: 18px;
        box-shadow: 0 8px 18px rgba(0,0,0,0.18);
    }
    [data-testid="stMetricValue"] { color: #7fe5c0; }
    [data-testid="stSidebar"] { background: #0a1020; border-right: 1px solid #24365f; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("Prague Real Estate Intelligence")
st.caption("Professional dashboard for Prague real estate market movement by scrape date, district, property type and source.")

data_path = "data/listings_processed.csv"
history_path = "data/listing_history.csv"
removed_path = "data/removed_listings.csv"

if not os.path.exists(data_path):
    st.warning("No processed dataset found. Run `python run_pipeline.py` first.")
    st.stop()

df = pd.read_csv(data_path)
history_df = pd.read_csv(history_path) if os.path.exists(history_path) else pd.DataFrame()
removed_df = pd.read_csv(removed_path) if os.path.exists(removed_path) else pd.DataFrame()

for frame in [df, history_df, removed_df]:
    for col in ["price_czk", "price_per_m2_czk", "area_m2", "listing_duration_days", "removed_duration_days"]:
        if col in frame.columns:
            frame[col] = pd.to_numeric(frame[col], errors="coerce")

for col in ["first_seen_at", "last_seen_at", "removed_at"]:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce")

if not history_df.empty and "scraped_at" in history_df.columns:
    history_df["scraped_at"] = pd.to_datetime(history_df["scraped_at"], errors="coerce")

active_df = df[df["is_active"] == True].copy() if "is_active" in df.columns else df.copy()

with st.sidebar:
    st.header("Filters")
    source_options = sorted([x for x in active_df.get("source", pd.Series(dtype=str)).dropna().unique().tolist() if str(x).strip()])
    selected_sources = st.multiselect("Website", source_options, default=source_options)
    property_options = sorted([x for x in active_df.get("property_type", pd.Series(dtype=str)).dropna().unique().tolist() if str(x).strip()])
    selected_property_types = st.multiselect("Property type", property_options, default=property_options)
    district_options = sorted([x for x in active_df.get("district_name", pd.Series(dtype=str)).dropna().unique().tolist() if str(x).strip()])
    selected_districts = st.multiselect("District", district_options, default=district_options)

filtered = active_df.copy()
if selected_sources and "source" in filtered.columns:
    filtered = filtered[filtered["source"].isin(selected_sources)]
if selected_property_types and "property_type" in filtered.columns:
    filtered = filtered[filtered["property_type"].isin(selected_property_types)]
if selected_districts and "district_name" in filtered.columns:
    filtered = filtered[filtered["district_name"].isin(selected_districts)]

if filtered.empty:
    st.warning("No listings match the selected filters.")
    st.stop()

hist_filtered = history_df.copy()
if not hist_filtered.empty:
    if selected_sources and "source" in hist_filtered.columns:
        hist_filtered = hist_filtered[hist_filtered["source"].isin(selected_sources)]
    if selected_property_types and "property_type" in hist_filtered.columns:
        hist_filtered = hist_filtered[hist_filtered["property_type"].isin(selected_property_types)]
    if selected_districts and "district_name" in hist_filtered.columns:
        hist_filtered = hist_filtered[hist_filtered["district_name"].isin(selected_districts)]


def fmt_czk(x):
    if x is None or pd.isna(x):
        return "n/a"
    return f"{int(round(x)):,.0f} Kč".replace(",", " ")


def fmt_days(x):
    if x is None or pd.isna(x):
        return "n/a"
    return f"{round(float(x),1)} days"


def delta_str(curr, prev, mode="currency"):
    if prev is None or pd.isna(prev):
        return "n/a"
    diff = curr - prev
    if mode == "currency":
        return fmt_czk(diff)
    if mode == "count":
        return f"{int(diff):,}"
    return f"{round(float(diff),1)}"

listing_count_curr = len(filtered)
listing_count_prev = None
total_market_curr = filtered["price_czk"].sum() if "price_czk" in filtered.columns else None
avg_price_curr = filtered["price_czk"].mean() if "price_czk" in filtered.columns else None
avg_duration_curr = filtered["listing_duration_days"].mean() if "listing_duration_days" in filtered.columns else None

total_market_prev = None
avg_price_prev = None
if not hist_filtered.empty and "scraped_at" in hist_filtered.columns:
    scrape_dates = sorted(hist_filtered["scraped_at"].dropna().unique().tolist())
    if len(scrape_dates) >= 2:
        prev_scrape = scrape_dates[-2]
        prev_df = hist_filtered[(hist_filtered["scraped_at"] == prev_scrape) & (hist_filtered["exists_on_source"] == True)].copy()
        listing_count_prev = len(prev_df)
        total_market_prev = prev_df["price_czk"].sum() if "price_czk" in prev_df.columns else None
        avg_price_prev = prev_df["price_czk"].mean() if "price_czk" in prev_df.columns else None

avg_removed_duration = removed_df["removed_duration_days"].mean() if (not removed_df.empty and "removed_duration_days" in removed_df.columns) else None

m1, m2, m3, m4 = st.columns(4)
m1.metric("Latest active listings", f"{listing_count_curr:,}", delta_str(listing_count_curr, listing_count_prev, "count"))
m2.metric("Total market value", fmt_czk(total_market_curr), delta_str(total_market_curr, total_market_prev, "currency"))
m3.metric("Average asking price", fmt_czk(avg_price_curr), delta_str(avg_price_curr, avg_price_prev, "currency"))
m4.metric("Average listing duration", fmt_days(avg_duration_curr), fmt_days(avg_removed_duration) if avg_removed_duration is not None else "n/a")

st.caption("The first three KPI deltas compare the latest scrape to the previous scrape. The final KPI subtitle shows average duration before removal for removed listings.")

left, right = st.columns(2)

with left:
    if {"district_name", "price_czk"}.issubset(filtered.columns):
        district_value = (
            filtered.dropna(subset=["district_name", "price_czk"])
            .groupby("district_name", as_index=False)
            .agg(listing_count=("composite_id", "count"), total_market_value_czk=("price_czk", "sum"), average_asking_price_czk=("price_czk", "mean"))
            .sort_values("total_market_value_czk", ascending=False)
        )
        fig1 = px.bar(district_value.head(20), x="district_name", y="total_market_value_czk", title="Total market value by district", hover_data=["listing_count", "average_asking_price_czk"])
        fig1.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig1, use_container_width=True)

    if {"property_type", "price_czk"}.issubset(filtered.columns):
        type_value = (
            filtered.groupby("property_type", as_index=False)
            .agg(listing_count=("composite_id", "count"), total_market_value_czk=("price_czk", "sum"), average_asking_price_czk=("price_czk", "mean"))
            .sort_values("total_market_value_czk", ascending=False)
        )
        fig2 = px.bar(type_value, x="property_type", y="listing_count", title="Listing count by property type", hover_data=["total_market_value_czk", "average_asking_price_czk"])
        fig2.update_layout(showlegend=False, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig2, use_container_width=True)

with right:
    if {"source", "price_czk"}.issubset(filtered.columns):
        source_value = filtered.groupby("source", as_index=False).agg(listing_count=("composite_id", "count"), total_market_value_czk=("price_czk", "sum"))
        fig3 = px.pie(source_value, names="source", values="listing_count", title="Listing share by website", hole=0.45)
        fig3.update_layout(paper_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig3, use_container_width=True)

    if {"district_name", "price_per_m2_czk"}.issubset(filtered.columns):
        district_ppm = (
            filtered.dropna(subset=["district_name", "price_per_m2_czk"])
            .groupby("district_name", as_index=False)
            .agg(median_price_per_m2_czk=("price_per_m2_czk", "median"), listing_count=("composite_id", "count"))
            .sort_values("median_price_per_m2_czk", ascending=False)
        )
        fig4 = px.bar(district_ppm.head(20), x="district_name", y="median_price_per_m2_czk", title="Median price per m² by district", hover_data=["listing_count"])
        fig4.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig4, use_container_width=True)

st.subheader("Scrape-to-scrape trend")
if not hist_filtered.empty and {"scraped_at", "source", "property_type", "price_czk"}.issubset(hist_filtered.columns):
    trend = (
        hist_filtered[hist_filtered["exists_on_source"] == True]
        .groupby(["scraped_at", "source", "property_type"], as_index=False)
        .agg(listing_count=("composite_id", "count"), total_market_value_czk=("price_czk", "sum"), average_asking_price_czk=("price_czk", "mean"))
        .sort_values("scraped_at")
    )
    t1, t2 = st.columns(2)
    with t1:
        fig5 = px.line(trend, x="scraped_at", y="listing_count", color="property_type", line_dash="source", markers=True, title="Listing count trend by scrape")
        fig5.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig5, use_container_width=True)
    with t2:
        fig6 = px.line(trend, x="scraped_at", y="average_asking_price_czk", color="property_type", line_dash="source", markers=True, title="Average asking price trend by scrape")
        fig6.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig6, use_container_width=True)

st.subheader("Current listings table")
show_cols = [c for c in ["source", "property_type", "property_link", "title", "district_name", "prague_zone", "layout_type", "area_m2", "price_czk", "price_per_m2_czk", "seller_type", "energy_class", "listing_duration_days"] if c in filtered.columns]
st.dataframe(filtered[show_cols], use_container_width=True)

if not removed_df.empty:
    st.subheader("Removed listings")
    removed_show_cols = [c for c in ["source", "property_type", "property_link", "title", "district_name", "prague_zone", "price_czk", "first_seen_at", "removed_at", "removed_duration_days"] if c in removed_df.columns]
    st.dataframe(removed_df[removed_show_cols], use_container_width=True)
