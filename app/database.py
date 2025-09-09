# app/database.py
from app.core import engine, SessionLocal
from app.models.central_models import Base as CentralBase  # Only Base
import logging

logger = logging.getLogger("database")

# -------------------- Create Tables Safely --------------------
def init_db():
    """Initialize the central database tables (Tenant, etc.)."""
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
