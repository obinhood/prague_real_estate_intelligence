from pathlib import Path

from sqlalchemy import text

from src.db.database import engine
from src.utils.logger import get_logger

logger = get_logger("postgres-schema")


def load_postgres_schema_sql() -> str:
    schema_path = Path(__file__).with_name("postgres_schema.sql")
    return schema_path.read_text(encoding="utf-8")


def apply_postgres_schema() -> None:
    if engine.dialect.name != "postgresql":
        raise RuntimeError(
            f"Postgres schema bootstrap requires a PostgreSQL engine, got {engine.dialect.name!r}."
        )

    raw_sql = load_postgres_schema_sql()
    statements = [statement.strip() for statement in raw_sql.split(";\n") if statement.strip()]
    with engine.begin() as conn:
        for statement in statements:
            conn.execute(text(statement))
    logger.info("Applied PostgreSQL schema successfully")


if __name__ == "__main__":
    apply_postgres_schema()
