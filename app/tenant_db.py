import os
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from app.models.models import Base as TenantBase

logger = logging.getLogger("tenant_db")


# -------------------- Create or Ensure Tenant DB --------------------
def create_tenant_db(chat_id: int) -> str:
    """
    Ensure tenant DB exists (for new tenants).
    Returns tenant_db_url.
    """
    base_url = os.getenv("DATABASE_URL")
    if not base_url:
        raise RuntimeError("❌ DATABASE_URL environment variable is missing")

    db_name = f"tenant_{chat_id}"
    tenant_db_url = f"{base_url.rsplit('/', 1)[0]}/{db_name}"

    logger.info(f"📌 Preparing tenant DB: {db_name}")

    # Connect to default DB
    default_engine = create_engine(
        base_url.rsplit("/", 1)[0] + "/postgres",
        execution_options={"isolation_level": "AUTOCOMMIT"}
    )

    with default_engine.connect() as conn:
        result = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname=:dbname"),
            {"dbname": db_name}
        ).fetchone()

        if not result:
            try:
                conn.execute(text(f'CREATE DATABASE "{db_name}"'))
                logger.info(f"✅ Tenant DB '{db_name}' created successfully.")
            except Exception as e:
                logger.warning(f"⚠️ Could not create tenant DB '{db_name}': {e}")
        else:
            logger.info(f"ℹ️ Tenant DB '{db_name}' already exists.")

    # Ensure tables inside it
    ensure_tenant_tables(tenant_db_url)

    return tenant_db_url


# -------------------- Ensure Tables in Existing Tenant DB --------------------
def ensure_tenant_tables(tenant_db_url: str):
    """
    Ensure tenant tables exist inside a tenant DB.
    Run this every time you open a tenant connection.
    """
    engine = create_engine(tenant_db_url)
    TenantBase.metadata.create_all(bind=engine)   # <-- this creates products table etc.
    logger.info(f"✅ Tenant tables ensured in {tenant_db_url}")


# -------------------- Tenant Session --------------------
def get_session_for_tenant(tenant_db_url: str):
    engine = create_engine(tenant_db_url)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return SessionLocal()


# -------------------- Get tenant session safely --------------------
def get_tenant_session(db_url: str):
    if not db_url:
        return None
    try:
        # Always ensure tables before returning a session
        ensure_tenant_tables(db_url)
        return get_session_for_tenant(db_url)
    except Exception as e:
        logger.error(f"❌ Failed to create tenant session: {e}")
        return None
