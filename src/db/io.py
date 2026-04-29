import pandas as pd
from sqlalchemy import inspect, text
from src.db.database import engine
from src.utils.logger import get_logger

logger = get_logger("db")


POSTGRES_CURRENT_STATE_QUERY = """
SELECT
    l.composite_id,
    l.url_id,
    src.source_code AS source,
    l.property_search_type,
    l.property_type_code,
    l.property_type,
    l.listing_url AS property_link,
    l.latest_title AS title,
    l.first_seen_at,
    l.last_seen_at,
    l.latest_snapshot_date AS snapshot_date,
    (l.current_status = 'active') AS is_active,
    (l.current_status = 'removed') AS is_removed,
    ls.exists_on_source,
    ls.scraped_at,
    ls.layout_type,
    ls.area_m2,
    ls.price_czk,
    ls.price_per_m2_czk,
    ls.previous_price_czk,
    ls.price_change_czk,
    ls.full_address,
    ls.street_address,
    COALESCE(ls.borough_name, l.latest_borough_name) AS borough_name,
    COALESCE(ls.district_name, l.latest_district_name) AS district_name,
    COALESCE(ls.prague_zone, l.latest_prague_zone) AS prague_zone,
    COALESCE(ls.location_quality, l.location_quality) AS location_quality,
    ls.city_name,
    ls.region_name,
    ls.country_name,
    ls.latitude,
    ls.longitude,
    ls.seller_type,
    ls.floor,
    ls.ownership_type,
    ls.energy_class,
    ls.has_balcony,
    ls.has_parking,
    ls.has_terrace,
    ls.has_elevator,
    ls.has_cellar,
    ls.description,
    ls.details_json,
    ls.listing_duration_days,
    ls.removed_duration_days
FROM listings l
JOIN sources src ON src.source_id = l.source_id
LEFT JOIN listing_snapshots ls
    ON ls.listing_id = l.listing_id
    AND ls.snapshot_date = l.latest_snapshot_date
WHERE l.current_status IN ('active', 'removed')
"""

POSTGRES_HISTORY_QUERY = """
SELECT
    l.composite_id,
    l.url_id,
    src.source_code AS source,
    l.property_search_type,
    l.property_type_code,
    l.property_type,
    l.listing_url AS property_link,
    ls.title,
    l.first_seen_at,
    l.last_seen_at,
    ls.snapshot_date,
    (l.current_status = 'active') AS is_active,
    (l.current_status = 'removed') AS is_removed,
    ls.exists_on_source,
    ls.scraped_at,
    ls.layout_type,
    ls.area_m2,
    ls.price_czk,
    ls.price_per_m2_czk,
    ls.previous_price_czk,
    ls.price_change_czk,
    ls.full_address,
    ls.street_address,
    ls.borough_name,
    ls.district_name,
    ls.prague_zone,
    ls.location_quality,
    ls.city_name,
    ls.region_name,
    ls.country_name,
    ls.latitude,
    ls.longitude,
    ls.seller_type,
    ls.floor,
    ls.ownership_type,
    ls.energy_class,
    ls.has_balcony,
    ls.has_parking,
    ls.has_terrace,
    ls.has_elevator,
    ls.has_cellar,
    ls.description,
    ls.details_json,
    ls.listing_duration_days,
    ls.removed_duration_days
FROM listing_snapshots ls
JOIN listings l ON l.listing_id = ls.listing_id
JOIN sources src ON src.source_id = ls.source_id
"""


def init_db():
    logger.info("STAGE: Database init checked")


def read_table_df(table_name: str) -> pd.DataFrame:
    try:
        inspector = inspect(engine)
        if not inspector.has_table(table_name):
            logger.warning(f"STAGE: Table {table_name} does not exist yet | returning empty DataFrame")
            return pd.DataFrame()
        with engine.connect() as conn:
            df = pd.read_sql_query(text(f"SELECT * FROM {table_name}"), conn)
            logger.info(f"STAGE: Loaded previous table {table_name} | rows: {len(df)}")
            return df
    except Exception as e:
        logger.exception(f"STAGE: Failed reading table {table_name}: {e}")
        return pd.DataFrame()


def write_dataframe_replace(df: pd.DataFrame, table_name: str):
    try:
        with engine.begin() as conn:
            df.to_sql(table_name, conn, if_exists="replace", index=False)
        logger.info(f"STAGE: Wrote table {table_name} | rows: {len(df)}")
    except Exception as e:
        logger.exception(f"STAGE: Failed writing table {table_name}: {e}")
        raise


def has_normalized_postgres_schema() -> bool:
    if engine.dialect.name != "postgresql":
        return False
    try:
        inspector = inspect(engine)
        required_tables = {"listings", "listing_snapshots", "listing_status_events", "sources"}
        return required_tables.issubset(set(inspector.get_table_names()))
    except Exception as e:
        logger.warning("STAGE: Could not inspect normalized PostgreSQL schema: %s", e)
        return False


def read_postgres_current_state_df() -> pd.DataFrame:
    if not has_normalized_postgres_schema():
        return pd.DataFrame()
    try:
        with engine.connect() as conn:
            df = pd.read_sql_query(text(POSTGRES_CURRENT_STATE_QUERY), conn)
        logger.info("STAGE: Loaded normalized current state from PostgreSQL | rows: %s", len(df))
        return df
    except Exception as e:
        logger.exception("STAGE: Failed loading normalized current state: %s", e)
        return pd.DataFrame()


def read_postgres_history_df() -> pd.DataFrame:
    if not has_normalized_postgres_schema():
        return pd.DataFrame()
    try:
        with engine.connect() as conn:
            df = pd.read_sql_query(text(POSTGRES_HISTORY_QUERY), conn)
        logger.info("STAGE: Loaded normalized history from PostgreSQL | rows: %s", len(df))
        return df
    except Exception as e:
        logger.exception("STAGE: Failed loading normalized history: %s", e)
        return pd.DataFrame()
