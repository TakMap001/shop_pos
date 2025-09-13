# app/tenant_db.py
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.models.models import Base as TenantBase  # Tenant-specific tables
import logging

logger = logging.getLogger("tenant_db")

# -------------------- Create Tenant DB --------------------
def create_tenant_db(chat_id: int) -> str:
    """
    Create a dedicated PostgreSQL database for the tenant if it doesn't exist.
    Returns the database URL for SQLAlchemy session creation.
    """
    base_url = os.getenv("DATABASE_URL")  # Railway public DB
    db_name = f"tenant_{chat_id}"

    # Construct tenant DB URL (replace database name only)
    if base_url.endswith("/public"):
        tenant_db_url = base_url.replace("/public", f"/{db_name}")
    else:
        tenant_db_url = f"{base_url.rsplit('/',1)[0]}/{db_name}"

    # Connect to default 'postgres' DB to create new tenant DB
    default_engine = create_engine(base_url.rsplit("/",1)[0] + "/postgres", isolation_level="AUTOCOMMIT")
    conn = default_engine.connect()
    try:
        # Check if database exists before creating
        result = conn.execute(f"SELECT 1 FROM pg_database WHERE datname='{db_name}';").fetchone()
        if not result:
            conn.execute(f"CREATE DATABASE {db_name};")
            logger.info(f"✅ Tenant DB '{db_name}' created successfully.")
        else:
            logger.info(f"ℹ️ Tenant DB '{db_name}' already exists.")
    except Exception as e:
        logger.warning(f"⚠️ Could not create tenant DB '{db_name}': {e}")
    finally:
        conn.close()

    # Create tables in tenant DB
    logger.info(f"Tenant DB URL: {tenant_db_url}")
    engine = create_engine(tenant_db_url)
    TenantBase.metadata.create_all(bind=engine)
    logger.info(f"✅ Tenant tables created in DB '{db_name}'.")

    return tenant_db_url

# -------------------- Tenant Session --------------------
def get_session_for_tenant(tenant_db_url: str):
    """Return a SQLAlchemy session for a given tenant DB URL."""
    engine = create_engine(tenant_db_url)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return SessionLocal()  # returns a session instance

# -------------------- Get tenant session safely --------------------
def get_tenant_session(db_url: str):
    """Return an active tenant Session. Returns None if db_url is missing."""
    if not db_url:
        return None
    try:
        return get_session_for_tenant(db_url)
    except Exception as e:
        logger.error(f"❌ Failed to create tenant session: {e}")
        return None
