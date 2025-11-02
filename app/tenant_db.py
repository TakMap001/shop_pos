import os
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from app.models.models import User
from app.models.central_models import Tenant
from app.models.tenant_base import TenantBase

# -----------------------------------------------------
# Basic logger setup (since you don't have core.logger)
# -----------------------------------------------------
logger = logging.getLogger("tenant_db")
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
logger.setLevel(logging.INFO)


# ======================================================
# 🔹 CREATE TENANT SCHEMA
# ======================================================
def create_tenant_db(chat_id: int) -> str:
    """
    Create or verify tenant schema and link it to user + tenant records.
    Returns full tenant DB URL: postgresql://.../railway#tenant_{chat_id}
    """
    if not chat_id:
        raise ValueError("❌ Invalid chat_id for tenant schema creation")

    base_url = os.getenv("DATABASE_URL")
    if not base_url:
        raise RuntimeError("❌ DATABASE_URL is missing")

    schema_name = f"tenant_{chat_id}"
    tenant_db_url = f"{base_url}#{schema_name}"

    logger.info(f"📌 Preparing tenant schema: {schema_name}")

    engine = create_engine(base_url, execution_options={"isolation_level": "AUTOCOMMIT"})

    with engine.connect() as conn:
        # Create schema if needed
        result = conn.execute(
            text("SELECT schema_name FROM information_schema.schemata WHERE schema_name=:s"),
            {"s": schema_name},
        ).fetchone()
        if not result:
            conn.execute(text(f'CREATE SCHEMA "{schema_name}"'))
            logger.info(f"✅ Tenant schema '{schema_name}' created.")
        else:
            logger.info(f"ℹ️ Tenant schema '{schema_name}' already exists.")

        # Ensure tenant record exists
        existing = conn.execute(
            text("SELECT tenant_id FROM tenants WHERE telegram_owner_id = :oid"),
            {"oid": chat_id},
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
            logger.info(f"✅ Tenant record created for {chat_id}")
        else:
            conn.execute(
                text("UPDATE tenants SET database_url = :url WHERE telegram_owner_id = :oid"),
                {"url": tenant_db_url, "oid": chat_id},
            )
            logger.info(f"ℹ️ Tenant record updated for {chat_id}")

        # Link user to tenant schema
        conn.execute(
            text("UPDATE users SET tenant_schema = :schema WHERE chat_id = :cid"),
            {"schema": schema_name, "cid": chat_id},
        )
        logger.info(f"✅ Linked user {chat_id} → {schema_name}")

    ensure_tenant_tables(base_url, schema_name)
    logger.info(f"✅ Tenant setup complete for chat_id={chat_id}")
    return tenant_db_url


# ======================================================
# 🔹 ENSURE TENANT TABLES
# ======================================================
def ensure_tenant_tables(base_url: str, schema_name: str):
    """Ensure all tenant tables exist."""
    if not base_url or not schema_name:
        raise ValueError("Base URL or schema name missing")

    engine = create_engine(
        base_url,
        future=True,
        pool_pre_ping=True,
        connect_args={"options": f"-csearch_path={schema_name}"}
    )
    TenantBase.metadata.create_all(bind=engine)
    logger.info(f"✅ Tenant tables verified in '{schema_name}'.")


# ======================================================
# 🔹 CREATE TENANT SESSION (Updated Version)
# ======================================================
def get_tenant_session(tenant_identifier: str, chat_id: int):
    """
    Create a tenant-scoped SQLAlchemy session.
    Now accepts: full URL, schema name, or tenant_xxx format
    """
    if not tenant_identifier:
        raise ValueError("❌ Missing tenant identifier")

    # Determine schema name and base URL
    if "://" in tenant_identifier:
        # Full URL provided (backward compatibility)
        if "#" in tenant_identifier:
            base_url, schema_name = tenant_identifier.split("#", 1)
        else:
            base_url = tenant_identifier
            schema_name = f"tenant_{chat_id}"
    else:
        # Schema name provided (new approach)
        schema_name = tenant_identifier
        base_url = os.getenv("DATABASE_URL").rsplit('/', 1)[0]  # Remove DB name
    
    logger.info(f"🔗 Creating tenant session → {schema_name}")

    # Create engine with schema in search path
    engine = create_engine(
        base_url,
        pool_pre_ping=True,
        connect_args={"options": f"-csearch_path={schema_name},public"}
    )

    # Explicitly set and verify search_path at engine level
    with engine.connect() as conn:
        conn.execute(text(f"SET search_path TO {schema_name},public"))
        active_path = conn.execute(text("SHOW search_path")).scalar()
        logger.info(f"🧭 Active search_path (explicitly set): {active_path}")

    # Ensure tables exist in the schema
    TenantBase.metadata.schema = schema_name
    TenantBase.metadata.create_all(bind=engine)

    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

    # ✅ Explicitly enforce schema in ORM session itself
    session = SessionLocal()
    session.execute(text(f"SET search_path TO {schema_name},public"))
    logger.info(f"✅ ORM session search_path locked to: {schema_name},public")

    return session

# ======================================================
# 🔹 ENSURE TENANT SESSION (Updated Version)
# ======================================================
def ensure_tenant_session(chat_id, db):
    """
    Return a tenant-specific session, ensuring proper schema and persistence.
    Updated to handle schema names directly.
    """
    user = db.query(User).filter(User.chat_id == chat_id).first()
    tenant_schema = getattr(user, "tenant_schema", None)

    # Handle different identifier formats
    if tenant_schema:
        if "://" in tenant_schema or "#" in tenant_schema:
            # Old URL format - convert to schema name
            if "#" in tenant_schema:
                _, schema_name = tenant_schema.split("#", 1)
            else:
                schema_name = f"tenant_{chat_id}"
            
            # Update user record to use schema name only
            user.tenant_schema = schema_name
            db.commit()
            logger.info(f"🔄 Converted tenant URL to schema name: {schema_name}")
            
            return get_tenant_session(schema_name, chat_id)
        else:
            # Already using schema name format
            return get_tenant_session(tenant_schema, chat_id)
    else:
        # No tenant schema - create one
        schema_name = f"tenant_{chat_id}"
        from app.tenant_utils import create_tenant_schema
        tenant_created = create_tenant_schema(schema_name)
        
        if tenant_created:
            user.tenant_schema = schema_name
            db.commit()
            logger.info(f"✅ Created and linked tenant schema: {schema_name}")
            return get_tenant_session(schema_name, chat_id)
        else:
            logger.error(f"❌ Failed to create tenant schema for {chat_id}")
            return None
