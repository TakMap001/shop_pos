# app/core.py
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from config import DATABASE_URL

# -------------------- Logging Setup --------------------
logger = logging.getLogger("core")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# -------------------- Engine & Session --------------------
engine = create_engine(DATABASE_URL, echo=False, future=True, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine, future=True)
