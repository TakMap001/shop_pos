import os
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from app.models.tenant_base import TenantBase  # tenant DB Base

logger = logging.getLogger("tenant_db")


# -------------------- Create Tenant Schema --------------------
def create_tenant_db(chat_id: int) -> str:
    """
    Create a dedicated SCHEMA for the tenant if it doesn't exist.
    Also ensures a tenant record exists in the central tenants table.
    Returns the schema-specific database URL for SQLAlchemy session creation.
    """
    if not chat_id:
        raise ValueError("❌ Cannot create tenant schema: chat_id is None or invalid")

    base_url = os.getenv("DATABASE_URL")
    if not base_url:
        raise RuntimeError("❌ DATABASE_URL environment variable is missing")

    schema_name = f"tenant_{chat_id}"
    tenant_db_url = f"{base_url}#{schema_name}"

    logger.info(f"📌 Preparing tenant schema: {schema_name}")

    # Connect to main database
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

        # Create schema if missing
        if not result:
            try:
                conn.execute(text(f'CREATE SCHEMA "{schema_name}"'))
                logger.info(f"✅ Tenant schema '{schema_name}' created successfully.")
            except Exception as e:
                logger.warning(f"⚠️ Could not create tenant schema '{schema_name}': {e}")
        else:
            logger.info(f"ℹ️ Tenant schema '{schema_name}' already exists.")

        # -------------------- Ensure tenant record in central DB --------------------
        try:
            existing = conn.execute(
                text("SELECT tenant_id FROM tenants WHERE telegram_owner_id = :oid"),
                {"oid": chat_id}
            ).fetchone()

            if not existing:
                conn.execute(
                    text("""
                        INSERT INTO tenants (tenant_id, store_name, telegram_owner_id, database_url, created_at)
                        VALUES (gen_random_uuid(), :store, :oid, :url, :created)
                    """),
                    {
                        "store": f"Store_{chat_id}",
                        "oid": chat_id,
                        "url": tenant_db_url,
                        "created": datetime.utcnow(),
                    },
                )
                logger.info(f"✅ Tenant record created for owner {chat_id}")
            else:
                conn.execute(
                    text("UPDATE tenants SET database_url = :url WHERE telegram_owner_id = :oid"),
                    {"url": tenant_db_url, "oid": chat_id},
                )
                logger.info(f"ℹ️ Tenant record updated for owner {chat_id}")
        except Exception as e:
            logger.error(f"❌ Failed to ensure tenant record: {e}")

    # Ensure tenant tables exist
    ensure_tenant_tables(base_url, schema_name)

    return tenant_db_url


# -------------------- Ensure tenant tables exist --------------------
def ensure_tenant_tables(base_url: str, schema_name: str):
    """Ensure all tenant tables exist in the given schema."""
    if not base_url or not schema_name:
        raise ValueError("Base URL or schema name is missing")

    try:
        engine = create_engine(
            base_url,
            future=True,
            pool_pre_ping=True,
            connect_args={"options": f"-csearch_path={schema_name}"}
        )
        TenantBase.metadata.create_all(bind=engine)
        logger.info(f"✅ Tenant tables created/verified in schema '{schema_name}'.")
    except Exception as e:
        logger.error(f"❌ Failed to create tenant tables in schema '{schema_name}': {e}")
        raise RuntimeError(f"Cannot initialize tenant tables for schema '{schema_name}'") from e


# -------------------- Tenant Session (simple) --------------------
def get_session_for_tenant(tenant_db_url: str):
    """
    Return a SQLAlchemy session for a given tenant schema DB URL.
    URL format: postgresql://.../railway#schema_name
    """
    if "#" in tenant_db_url:
        base_url, schema_name = tenant_db_url.split("#", 1)
        engine = create_engine(
            base_url,
            future=True,
            pool_pre_ping=True,
            connect_args={"options": f"-csearch_path={schema_name}"}
        )
    else:
        engine = create_engine(tenant_db_url, future=True, pool_pre_ping=True)

    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine, future=True)
    return SessionLocal()


# -------------------- Get tenant session safely --------------------
def get_tenant_session(tenant_db_url: str, chat_id: int = None):
    """
    Return a SQLAlchemy session for a given tenant schema DB URL.
    Ensures the correct search_path is set to the tenant schema.
    URL format: postgresql://.../railway#schema_name
    """
    if not tenant_db_url:
        raise ValueError("❌ Missing tenant_db_url")

    # Split base URL and schema name
    if "#" in tenant_db_url:
        base_url, schema_name = tenant_db_url.split("#", 1)
    else:
        base_url = tenant_db_url
        if chat_id:
            schema_name = f"tenant_{chat_id}"
            logger.warning(f"⚠️ Schema missing in db_url. Derived schema_name={schema_name} from chat_id={chat_id}")
        else:
            schema_name = "public"

    # ✅ Create engine with tenant-specific search_path
    try:
        engine = create_engine(
            base_url,
            future=True,
            pool_pre_ping=True,
            connect_args={"options": f"-csearch_path={schema_name},public"}
        )

        # ✅ Bind ORM Base *before* creating session
        from app.models.models import TenantBase
        TenantBase.metadata.bind = engine
        TenantBase.metadata.schema = schema_name

        logger.info(f"✅ Tenant search_path set to: {schema_name}, public")

        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine, future=True)
        return SessionLocal()

    except Exception as e:
        logger.error(f"❌ Failed to create tenant engine for schema '{schema_name}': {e}")
        raise
