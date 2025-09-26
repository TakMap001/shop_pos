import os
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from app.models.tenant_base import TenantBase  # tenant DB Base

logger = logging.getLogger("tenant_db")


# -------------------- Create Tenant DB --------------------
def create_tenant_db(chat_id: int) -> str:
    """
    Create a dedicated PostgreSQL database for the tenant if it doesn't exist.
    Returns the database URL for SQLAlchemy session creation.
    """
    if not chat_id:
        raise ValueError("❌ Cannot create tenant DB: chat_id is None or invalid")

    base_url = os.getenv("DATABASE_URL")
    if not base_url:
        raise RuntimeError("❌ DATABASE_URL environment variable is missing")

    db_name = f"tenant_{chat_id}"
    tenant_db_url = f"{base_url.rsplit('/', 1)[0]}/{db_name}"

    logger.info(f"📌 Preparing tenant DB: {db_name}")

    # Connect to default 'postgres' DB to create new tenant DB
    default_engine = create_engine(
        base_url.rsplit("/", 1)[0] + "/postgres",
        execution_options={"isolation_level": "AUTOCOMMIT"}
    )

    with default_engine.connect() as conn:
        # Check if database exists
        try:
            result = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname=:dbname"),
                {"dbname": db_name}
            ).fetchone()
        except Exception as e:
            logger.error(f"❌ Failed to check DB existence: {e}")
            raise

        if not result:
            try:
                conn.execute(text(f'CREATE DATABASE "{db_name}"'))
                logger.info(f"✅ Tenant DB '{db_name}' created successfully.")
            except Exception as e:
                logger.warning(
                    f"⚠️ Could not create tenant DB '{db_name}' (likely permissions issue): {e}"
                )
        else:
            logger.info(f"ℹ️ Tenant DB '{db_name}' already exists.")

    # Ensure tables exist
    ensure_tenant_tables(tenant_db_url)

    return tenant_db_url


# -------------------- Ensure tenant tables exist --------------------
def ensure_tenant_tables(tenant_db_url: str):
    """Ensure all tenant tables exist in the given DB."""
    if not tenant_db_url:
        raise ValueError("Tenant DB URL is missing")

    try:
        engine = create_engine(tenant_db_url, future=True, pool_pre_ping=True)
        TenantBase.metadata.create_all(bind=engine)
        logger.info(f"✅ Tenant tables created/verified in DB '{tenant_db_url}'.")
    except Exception as e:
        logger.error(f"❌ Failed to create tenant tables in '{tenant_db_url}': {e}")
        raise RuntimeError(f"Cannot initialize tenant tables for DB '{tenant_db_url}'") from e


# -------------------- Tenant Session --------------------
def get_session_for_tenant(tenant_db_url: str):
    """Return a SQLAlchemy session for a given tenant DB URL."""
    engine = create_engine(tenant_db_url, future=True, pool_pre_ping=True)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine, future=True)
    return SessionLocal()


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
