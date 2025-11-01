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
# 🔹 CREATE TENANT SESSION (Corrected Version)
# ======================================================
def get_tenant_session(tenant_db_url: str, chat_id: int):
    """
    Create a tenant-scoped SQLAlchemy session using the '#tenant_xxx' schema tag.
    Ensures the ORM session itself executes within the tenant schema.
    """
    if not tenant_db_url:
        raise ValueError("❌ Missing tenant_db_url")

    if "#" in tenant_db_url:
        base_url, schema_name = tenant_db_url.split("#", 1)
    else:
        base_url = tenant_db_url
        schema_name = f"tenant_{chat_id}"

    logger.info(f"🔗 Creating tenant session → {schema_name}")

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

    TenantBase.metadata.schema = schema_name
    TenantBase.metadata.create_all(bind=engine)

    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

    # ✅ Explicitly enforce schema in ORM session itself
    session = SessionLocal()
    session.execute(text(f"SET search_path TO {schema_name},public"))
    logger.info(f"✅ ORM session search_path locked to: {schema_name},public")

    return session

# ======================================================
# 🔹 ENSURE TENANT SESSION (Main entry point)
# ======================================================
def ensure_tenant_session(chat_id, db):
    """
    Return a tenant-specific session, ensuring proper schema and persistence.
    """
    user = db.query(User).filter(User.chat_id == chat_id).first()
    tenant_schema = getattr(user, "tenant_schema", None)

    # Derive or rebuild URL
    if tenant_schema and "#" in tenant_schema:
        tenant_db_url = tenant_schema
    else:
        tenant = db.query(Tenant).filter(Tenant.telegram_owner_id == chat_id).first()
        if tenant and tenant.database_url:
            tenant_db_url = tenant.database_url
        else:
            base_url = os.getenv("DATABASE_URL")
            schema_name = tenant_schema if tenant_schema else f"tenant_{chat_id}"
            tenant_db_url = f"{base_url}#{schema_name}"
            logger.warning(f"⚠️ Reconstructed tenant_db_url: {tenant_db_url}")

        # Persist to user record
        user.tenant_schema = tenant_db_url
        db.commit()

    return get_tenant_session(tenant_db_url, chat_id)
