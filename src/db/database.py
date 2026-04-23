from sqlalchemy import create_engine
from src.config import CONFIG

engine = create_engine(CONFIG["database_url"])
