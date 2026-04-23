from sqlalchemy import text
from src.db.database import engine

if __name__ == "__main__":
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS listing_history"))
        conn.execute(text("DROP TABLE IF EXISTS listings"))
    print("Dropped tables: listing_history, listings")
