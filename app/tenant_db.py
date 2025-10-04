import os
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from app.models.tenant_base import TenantBase  # tenant DB Base

logger = logging.getLogger("tenant_db")


# -------------------- Create Tenant Schema --------------------
def create_tenant_db(chat_id: int) -> str:
    """
    Create a dedicated SCHEMA for the tenant if it doesn't exist.
    Returns the schema-specific database URL for SQLAlchemy session creation.
    """
    if not chat_id:
        raise ValueError("❌ Cannot create tenant schema: chat_id is None or invalid")

    base_url = os.getenv("DATABASE_URL")
    if not base_url:
        raise RuntimeError("❌ DATABASE_URL environment variable is missing")

    schema_name = f"tenant_{chat_id}"

    logger.info(f"📌 Preparing tenant schema: {schema_name}")

    # Connect to main Railway database
    engine = create_engine(base_url, execution_options={"isolation_level": "AUTOCOMMIT"})

    with engine.connect() as conn:
        # Check if schema exists
        try:
            result = conn.execute(
                text("SELECT schema_name FROM information_schema.schemata WHERE schema_name=:s"),
                {"s": schema_name}
            ).fetchone()
        except Exception as e:
            logger.error(f"❌ Failed to check schema existence: {e}")
            raise

        if not result:
            try:
                conn.execute(text(f'CREATE SCHEMA "{schema_name}"'))
                logger.info(f"✅ Tenant schema '{schema_name}' created successfully.")
            except Exception as e:
                logger.warning(
                    f"⚠️ Could not create tenant schema '{schema_name}': {e}"
                )
        else:
            logger.info(f"ℹ️ Tenant schema '{schema_name}' already exists.")

    # Ensure tables exist in this schema
    ensure_tenant_tables(base_url, schema_name)

    # Return URL with schema hint
    tenant_db_url = f"{base_url}#{schema_name}"
    return tenant_db_url


# -------------------- Ensure tenant tables exist --------------------
def ensure_tenant_tables(base_url: str, schema_name: str):
    """Ensure all tenant tables exist in the given schema."""
    if not base_url or not schema_name:
        raise ValueError("Base URL or schema name is missing")

    try:
        engine = create_engine(
            base_url, future=True, pool_pre_ping=True,
            connect_args={"options": f"-csearch_path={schema_name}"}
        )
        TenantBase.metadata.create_all(bind=engine)
        logger.info(f"✅ Tenant tables created/verified in schema '{schema_name}'.")
    except Exception as e:
        logger.error(f"❌ Failed to create tenant tables in schema '{schema_name}': {e}")
        raise RuntimeError(f"Cannot initialize tenant tables for schema '{schema_name}'") from e


# -------------------- Tenant Session --------------------
def get_session_for_tenant(tenant_db_url: str):
    """
    Return a SQLAlchemy session for a given tenant schema DB URL.
    URL format: postgresql://.../railway#schema_name
    """
    if "#" in tenant_db_url:
        base_url, schema_name = tenant_db_url.split("#", 1)
        engine = create_engine(
            base_url, future=True, pool_pre_ping=True,
            connect_args={"options": f"-csearch_path={schema_name}"}
        )
    else:
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
