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
    Create a dedicated PostgreSQL database for the tenant.
    Returns the database URL for SQLAlchemy session creation.
    """
    # Example: DATABASE_URL like: postgres://user:pass@host:port/publicdb
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
        conn.execute(f"CREATE DATABASE {db_name};")
        logger.info(f"✅ Tenant DB '{db_name}' created successfully.")
    except Exception as e:
        logger.warning(f"⚠️ Tenant DB '{db_name}' might already exist: {e}")
    finally:
        conn.close()

    # Create tables in tenant DB
    engine = create_engine(tenant_db_url)
    TenantBase.metadata.create_all(bind=engine)
    logger.info(f"✅ Tenant tables created in DB '{db_name}'.")

    return tenant_db_url

# -------------------- Tenant Session --------------------
def get_session_for_tenant(tenant_db_url: str):
    engine = create_engine(tenant_db_url)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return SessionLocal()
