import os
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from datetime import datetime
from app.models.models import User
from app.models.models import ProductORM, CustomerORM, SaleORM, PendingApprovalORM
from app.models.central_models import Tenant
from app.models.central_models import Base as CentralBase
from app.models.tenant_base import TenantBase
from config import DATABASE_URL

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
# 🔹 CREATE CENTRAL DATABASE TABLES  
# ======================================================
def create_central_db():
    """
    Ensures central DB tables exist at startup.
    This creates users and tenants tables in the public schema.
    """
    
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("❌ DATABASE_URL is missing")
    
    engine = create_engine(database_url)
    CentralBase.metadata.create_all(bind=engine)
    logger.info("✅ Central DB tables (users, tenants) created in public schema")
    
# ======================================================
# 🔹 CREATE TENANT SCHEMA
# ======================================================
def create_tenant_db(chat_id: int, role: str = "owner") -> str:
    """
    Create or verify tenant schema ONLY for owners.
    For shopkeepers, return their existing tenant_schema.
    """
    if not chat_id:
        raise ValueError("❌ Invalid chat_id for tenant schema creation")

    # ✅ Check if this is a shopkeeper - they should NOT get their own schema
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("❌ DATABASE_URL is missing")
    
    engine = create_engine(database_url)
    
    # Check if user exists and get their role
    with engine.connect() as conn:
        user_result = conn.execute(
            text("SELECT role, tenant_schema FROM users WHERE chat_id = :cid"),
            {"cid": chat_id}
        ).fetchone()
    
    # If shopkeeper, return their existing tenant_schema
    if user_result and user_result[0] == "shopkeeper":
        existing_schema = user_result[1]
        if existing_schema:
            logger.info(f"🔄 Shopkeeper {chat_id} - returning existing schema: {existing_schema}")
            return existing_schema  # Just return schema name, not URL
    
    # Only create schema for owners or users without tenant_schema
    schema_name = f"tenant_{chat_id}"
    tenant_db_url = f"{database_url}#{schema_name}"
    
    logger.info(f"📌 Preparing tenant schema: {schema_name}")

    with engine.connect() as conn:
        # Create schema if needed (only for owners)
        if not user_result or user_result[0] == "owner":
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

        ensure_tenant_tables(database_url, schema_name)
        logger.info(f"✅ Tenant setup complete for chat_id={chat_id}")
        return schema_name  # Return schema name, not URL
        


# ======================================================
# 🔹 ENSURE TENANT TABLES
# ======================================================
def ensure_tenant_tables(base_url: str, schema_name: str):
    """Ensure all tenant tables exist in the correct schema."""
    if not base_url or not schema_name:
        raise ValueError("Base URL or schema name missing")

    logger.info(f"🔄 Ensuring tables in schema: {schema_name}")
    
    # Create engine with explicit schema setting
    engine = create_engine(
        base_url,
        future=True,
        pool_pre_ping=True,
        # This sets the search_path for the connection
        connect_args={"options": f"-csearch_path={schema_name}"}
    )
    
    try:
        # First, explicitly set the schema
        with engine.connect() as conn:
            conn.execute(text(f"SET search_path TO {schema_name}"))
            
            # Now create tables in that schema
            TenantBase.metadata.create_all(bind=conn)
            
            logger.info(f"✅ Tenant tables created in '{schema_name}'.")
            
    except Exception as e:
        logger.error(f"❌ Failed to create tenant tables in {schema_name}: {e}")
        raise
        
# ======================================================
# 🔹 CREATE TENANT SESSION (Fixed Version - No Table Recreation)
# ======================================================
def get_tenant_session(schema_name: str, chat_id: int = None):
    """
    Create a tenant-scoped SQLAlchemy session.
    Accepts schema name only.
    """
    if not schema_name:
        logger.error(f"❌ No schema_name provided")
        return None
    
    # Always use DATABASE_URL from config
    database_url = DATABASE_URL
    
    logger.info(f"🔗 Creating tenant session → {schema_name}")

    try:
        # Create engine with search_path set in connect_args
        engine = create_engine(
            database_url,
            pool_pre_ping=True,
            connect_args={"options": f"-csearch_path={schema_name},public"}
        )
        
        # Create session
        SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
        session = SessionLocal()
        
        # Double-check search_path
        session.execute(text(f"SET search_path TO {schema_name},public"))
        result = session.execute(text("SHOW search_path")).fetchone()
        logger.info(f"🧭 Active search_path: {result[0]}")
        
        logger.info(f"✅ ORM session search_path locked to: {schema_name},public")
        return session
        
    except Exception as e:
        logger.error(f"❌ Failed to create tenant session for {schema_name}: {e}")
        return None
        

# ======================================================
# 🔹 ENSURE TENANT SESSION (Updated Version)
# ======================================================
def ensure_tenant_session(chat_id, db):
    """
    Return a tenant-specific session.
    For shopkeepers, use their assigned tenant_schema.
    For owners, create schema if needed.
    """
    user = db.query(User).filter(User.chat_id == chat_id).first()
    
    if not user:
        logger.error(f"❌ User not found for chat_id: {chat_id}")
        return None
    
    # ✅ For shopkeepers, use their existing tenant_schema
    if user.role == "shopkeeper":
        if not user.tenant_schema:
            logger.error(f"❌ Shopkeeper {chat_id} has no tenant_schema assigned")
            return None
        
        # Shopkeepers should NOT create their own schema
        logger.info(f"🔄 Shopkeeper {chat_id} - using assigned schema: {user.tenant_schema}")
        return get_tenant_session(user.tenant_schema, chat_id)
    
    # ✅ For owners, create schema if needed
    if not user.tenant_schema:
        # Owner doesn't have schema - create one
        schema_name = create_tenant_db(chat_id, user.role)
        if schema_name:
            user.tenant_schema = schema_name
            db.commit()
            return get_tenant_session(schema_name, chat_id)
        else:
            return None
    
    # Owner has schema - use it
    return get_tenant_session(user.tenant_schema, chat_id)
    
