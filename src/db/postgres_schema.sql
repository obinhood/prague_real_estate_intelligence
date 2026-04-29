CREATE TABLE IF NOT EXISTS sources (
    source_id BIGSERIAL PRIMARY KEY,
    source_code TEXT NOT NULL UNIQUE,
    source_name TEXT NOT NULL,
    source_domain TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS scrape_runs (
    scrape_run_id BIGSERIAL PRIMARY KEY,
    snapshot_date DATE NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    run_status TEXT NOT NULL DEFAULT 'completed' CHECK (run_status IN ('started', 'completed', 'failed', 'partial')),
    include_bezrealitky BOOLEAN NOT NULL DEFAULT FALSE,
    scraped_rows INTEGER NOT NULL DEFAULT 0,
    active_rows INTEGER NOT NULL DEFAULT 0,
    new_listings INTEGER NOT NULL DEFAULT 0,
    removed_listings INTEGER NOT NULL DEFAULT 0,
    price_changes INTEGER NOT NULL DEFAULT 0,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (started_at)
);

CREATE TABLE IF NOT EXISTS listings (
    listing_id BIGSERIAL PRIMARY KEY,
    composite_id TEXT NOT NULL UNIQUE,
    source_id BIGINT NOT NULL REFERENCES sources(source_id),
    source_listing_key TEXT,
    url_id TEXT,
    property_search_type TEXT,
    property_type_code TEXT,
    property_type TEXT,
    transaction_type TEXT NOT NULL DEFAULT 'sale',
    listing_url TEXT,
    latest_title TEXT,
    first_seen_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL,
    latest_snapshot_date DATE NOT NULL,
    current_status TEXT NOT NULL CHECK (current_status IN ('active', 'removed', 'unknown')),
    last_known_price_czk NUMERIC(14, 2),
    last_known_price_per_m2_czk NUMERIC(14, 2),
    last_known_area_m2 NUMERIC(10, 2),
    latest_district_name TEXT,
    latest_borough_name TEXT,
    latest_prague_zone TEXT,
    location_quality TEXT NOT NULL DEFAULT 'ok',
    latest_source_payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS listing_snapshots (
    listing_snapshot_id BIGSERIAL PRIMARY KEY,
    listing_id BIGINT NOT NULL REFERENCES listings(listing_id) ON DELETE CASCADE,
    scrape_run_id BIGINT NOT NULL REFERENCES scrape_runs(scrape_run_id) ON DELETE CASCADE,
    snapshot_date DATE NOT NULL,
    scraped_at TIMESTAMPTZ NOT NULL,
    exists_on_source BOOLEAN NOT NULL,
    source_id BIGINT NOT NULL REFERENCES sources(source_id),
    title TEXT,
    property_type_code TEXT,
    property_type TEXT,
    transaction_type TEXT NOT NULL DEFAULT 'sale',
    layout_type TEXT,
    area_m2 NUMERIC(10, 2),
    price_czk NUMERIC(14, 2),
    price_per_m2_czk NUMERIC(14, 2),
    previous_price_czk NUMERIC(14, 2),
    price_change_czk NUMERIC(14, 2),
    full_address TEXT,
    street_address TEXT,
    district_name TEXT,
    borough_name TEXT,
    prague_zone TEXT,
    location_quality TEXT NOT NULL DEFAULT 'ok',
    city_name TEXT,
    region_name TEXT,
    country_name TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    seller_type TEXT,
    agency_name TEXT,
    ownership_type TEXT,
    floor TEXT,
    total_floors INTEGER,
    energy_class TEXT,
    has_balcony BOOLEAN,
    has_terrace BOOLEAN,
    has_parking BOOLEAN,
    has_elevator BOOLEAN,
    has_cellar BOOLEAN,
    description TEXT,
    details_json JSONB,
    listing_duration_days NUMERIC(10, 2),
    removed_duration_days NUMERIC(10, 2),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (listing_id, scrape_run_id)
);

CREATE TABLE IF NOT EXISTS listing_status_events (
    listing_status_event_id BIGSERIAL PRIMARY KEY,
    listing_id BIGINT NOT NULL REFERENCES listings(listing_id) ON DELETE CASCADE,
    scrape_run_id BIGINT NOT NULL REFERENCES scrape_runs(scrape_run_id) ON DELETE CASCADE,
    snapshot_date DATE NOT NULL,
    previous_snapshot_date DATE,
    event_type TEXT NOT NULL CHECK (event_type IN ('new', 'removed', 'price_increase', 'price_reduction', 'reappeared')),
    event_at TIMESTAMPTZ NOT NULL,
    source_id BIGINT REFERENCES sources(source_id),
    previous_price_czk NUMERIC(14, 2),
    current_price_czk NUMERIC(14, 2),
    price_change_czk NUMERIC(14, 2),
    price_change_pct NUMERIC(8, 4),
    details JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS district_reference (
    district_reference_id BIGSERIAL PRIMARY KEY,
    district_name TEXT NOT NULL,
    borough_name TEXT NOT NULL,
    prague_zone TEXT NOT NULL,
    is_core_mapping BOOLEAN NOT NULL DEFAULT TRUE,
    UNIQUE (district_name, borough_name, prague_zone)
);

CREATE INDEX IF NOT EXISTS idx_scrape_runs_snapshot_date
    ON scrape_runs (snapshot_date DESC, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_listings_source_id
    ON listings (source_id, current_status, latest_snapshot_date DESC);

CREATE INDEX IF NOT EXISTS idx_listings_location
    ON listings (latest_district_name, latest_borough_name, latest_prague_zone);

CREATE INDEX IF NOT EXISTS idx_listing_snapshots_snapshot_date
    ON listing_snapshots (snapshot_date DESC, exists_on_source, source_id);

CREATE INDEX IF NOT EXISTS idx_listing_snapshots_listing_date
    ON listing_snapshots (listing_id, snapshot_date DESC);

CREATE INDEX IF NOT EXISTS idx_listing_snapshots_district
    ON listing_snapshots (district_name, borough_name, prague_zone, snapshot_date DESC);

CREATE INDEX IF NOT EXISTS idx_listing_snapshots_price
    ON listing_snapshots (snapshot_date DESC, price_czk, price_per_m2_czk);

CREATE INDEX IF NOT EXISTS idx_listing_status_events_snapshot_date
    ON listing_status_events (snapshot_date DESC, event_type);

CREATE INDEX IF NOT EXISTS idx_listing_status_events_listing_id
    ON listing_status_events (listing_id, event_at DESC);

CREATE OR REPLACE VIEW latest_active_listings AS
SELECT
    l.listing_id,
    l.composite_id,
    s.source_code,
    s.source_name,
    l.property_search_type,
    l.property_type_code,
    l.property_type,
    l.transaction_type,
    l.listing_url,
    l.latest_title AS title,
    l.first_seen_at,
    l.last_seen_at,
    l.latest_snapshot_date AS snapshot_date,
    l.current_status,
    l.last_known_price_czk AS price_czk,
    l.last_known_price_per_m2_czk AS price_per_m2_czk,
    l.last_known_area_m2 AS area_m2,
    l.latest_district_name AS district_name,
    l.latest_borough_name AS borough_name,
    l.latest_prague_zone AS prague_zone,
    l.location_quality
FROM listings l
JOIN sources s ON s.source_id = l.source_id
WHERE l.current_status = 'active';

CREATE OR REPLACE VIEW daily_market_metrics AS
SELECT
    ls.snapshot_date,
    ls.source_id,
    src.source_code,
    ls.property_type,
    ls.transaction_type,
    ls.district_name,
    ls.borough_name,
    ls.prague_zone,
    COUNT(*) FILTER (WHERE ls.exists_on_source) AS active_listings,
    SUM(ls.price_czk) FILTER (WHERE ls.exists_on_source) AS total_market_value_czk,
    AVG(ls.price_czk) FILTER (WHERE ls.exists_on_source) AS average_listing_price_czk,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ls.price_czk) FILTER (WHERE ls.exists_on_source AND ls.price_czk IS NOT NULL) AS median_listing_price_czk,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ls.price_per_m2_czk) FILTER (WHERE ls.exists_on_source AND ls.price_per_m2_czk IS NOT NULL) AS median_price_per_m2_czk,
    AVG(ls.area_m2) FILTER (WHERE ls.exists_on_source) AS average_area_m2,
    AVG(ls.listing_duration_days) FILTER (WHERE ls.exists_on_source) AS average_days_on_market,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ls.listing_duration_days) FILTER (WHERE ls.exists_on_source AND ls.listing_duration_days IS NOT NULL) AS median_days_on_market
FROM listing_snapshots ls
JOIN sources src ON src.source_id = ls.source_id
GROUP BY
    ls.snapshot_date,
    ls.source_id,
    src.source_code,
    ls.property_type,
    ls.transaction_type,
    ls.district_name,
    ls.borough_name,
    ls.prague_zone;

CREATE OR REPLACE VIEW daily_listing_movements AS
SELECT
    e.snapshot_date,
    e.previous_snapshot_date,
    e.event_type,
    e.source_id,
    src.source_code,
    l.property_type,
    l.transaction_type,
    l.latest_district_name AS district_name,
    l.latest_borough_name AS borough_name,
    l.latest_prague_zone AS prague_zone,
    COUNT(*) AS listing_count,
    SUM(e.price_change_czk) AS total_price_change_czk,
    AVG(e.price_change_czk) AS average_price_change_czk
FROM listing_status_events e
JOIN listings l ON l.listing_id = e.listing_id
LEFT JOIN sources src ON src.source_id = e.source_id
GROUP BY
    e.snapshot_date,
    e.previous_snapshot_date,
    e.event_type,
    e.source_id,
    src.source_code,
    l.property_type,
    l.transaction_type,
    l.latest_district_name,
    l.latest_borough_name,
    l.latest_prague_zone;
