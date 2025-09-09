import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from config import DATABASE_URL
from app.models.central_models import Base as CentralBase  # Central DB Base
from app.models.models import User  # Users live in main models.py

# -------------------- Logging Setup --------------------
logger = logging.getLogger("database")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# -------------------- Engine & Session --------------------
engine = create_engine(DATABASE_URL, echo=False, future=True, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine, future=True)

# -------------------- Create Tables Safely --------------------
def init_db():
    """Initialize the central database tables (Tenant, User, etc.)."""
    try:
        CentralBase.metadata.create_all(bind=engine)
        logger.info("✅ Central database tables created / verified successfully.")
    except Exception as e:
        logger.error(f"❌ Failed to initialize central database: {e}")

# -------------------- Dependency --------------------
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
