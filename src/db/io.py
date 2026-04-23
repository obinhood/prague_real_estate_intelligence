import pandas as pd
from sqlalchemy import inspect, text
from src.db.database import engine
from src.utils.logger import get_logger

logger = get_logger("db")


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
