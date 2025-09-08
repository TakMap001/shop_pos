import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from config import DATABASE_URL

# -------------------- Logging Setup --------------------
logger = logging.getLogger("database")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# -------------------- Engine & Session --------------------
engine = create_engine(DATABASE_URL, echo=False, future=True)  # echo=False avoids clutter
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine, future=True)
Base = declarative_base()

# -------------------- Import Central Models --------------------
from app.models.central_models import User  # Add other central models here if needed

# -------------------- Create Tables Safely --------------------
def init_db():
    """Initialize the main database if tables do not exist."""
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("✅ Main database tables created / verified successfully.")
    except Exception as e:
        logger.error(f"❌ Failed to initialize main database: {e}")

# Call once at import time
init_db()

# -------------------- Dependency --------------------
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
