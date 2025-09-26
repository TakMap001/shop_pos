# app/database.py
import logging
from app.core import engine, SessionLocal
from app.models.central_models import Base as CentralBase  # Only central DB Base

logger = logging.getLogger("database")


# -------------------- Initialize Central DB --------------------
def init_db():
    """Initialize the central database tables (Tenant, User, etc.)."""
    try:
        CentralBase.metadata.create_all(bind=engine)
        logger.info("✅ Central database tables created or verified successfully.")
    except Exception as e:
        logger.exception(f"❌ Failed to initialize central database: {e}")


# -------------------- Dependency for FastAPI --------------------
def get_db():
    """Yield a central DB session for FastAPI routes."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
