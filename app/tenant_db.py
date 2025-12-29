# app/tenant_db.py
import os
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import re
import secrets
import string
import time
from app.models.central_models import User
from app.models.models import ProductORM, CustomerORM, SaleORM, PendingApprovalORM, ShopORM, ProductShopStockORM
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

# ==================== PASSWORD UTILITIES ====================

def generate_password(length=8):
    """Generate a secure random password."""
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(length))

def create_username(base_name):
    """Create a unique username by adding timestamp."""
    timestamp = int(time.time()) % 10000
    clean_name = re.sub(r'[^a-zA-Z0-9]', '', base_name.lower())
    return f"{clean_name}{timestamp}"

def hash_password(password: str) -> str:
    """Hash password using bcrypt or similar."""
    # For now, use a simple hash - in production use bcrypt
    import hashlib
    return hashlib.sha256(password.encode()).hexdigest()

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
# 🔹 CREATE TENANT SCHEMA (UPDATED FOR MULTI-SHOP)
# ======================================================
def create_tenant_db(chat_id: int, role: str = "owner") -> tuple:
    """
    Create tenant schema for a new owner.
    Returns: (schema_name, credentials_dict)
    - schema_name: the created schema name
    - credentials_dict: owner credentials only (admin/shopkeeper created later with shops)
    """
    if not chat_id:
        raise ValueError("❌ Invalid chat_id for tenant schema creation")

    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("❌ DATABASE_URL is missing")
    
    engine = create_engine(database_url)
    
    # ✅ Check if this is a shopkeeper or admin - they should NOT get their own schema
    with engine.connect() as conn:
        user_result = conn.execute(
            text("SELECT role, tenant_schema FROM users WHERE chat_id = :cid"),
            {"cid": chat_id}
        ).fetchone()
    
    # If shopkeeper or admin, return their existing tenant_schema
    if user_result and user_result[0] in ["shopkeeper", "admin"]:
        existing_schema = user_result[1]
        if existing_schema:
            logger.info(f"🔄 {user_result[0].title()} {chat_id} - returning existing schema: {existing_schema}")
            return existing_schema, {}  # Return empty credentials
    
    # Only create schema for owners or users without tenant_schema
    schema_name = f"tenant_{chat_id}"
    tenant_db_url = f"{database_url}#{schema_name}"
    
    logger.info(f"📌 Preparing tenant schema: {schema_name}")

    credentials = {}  # ✅ Store credentials to return
    
    try:
        with engine.connect() as conn:
            # Create schema if needed (only for owners)
            if not user_result or user_result[0] == "owner":
                result = conn.execute(
                    text("SELECT schema_name FROM information_schema.schemata WHERE schema_name=:s"),
                    {"s": schema_name},
                ).fetchone()
                if not result:
                    conn.execute(text(f'CREATE SCHEMA "{schema_name}"'))
                    conn.commit()
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
                    conn.commit()
                    logger.info(f"✅ Tenant record created for {chat_id}")
                else:
                    conn.execute(
                        text("UPDATE tenants SET database_url = :url WHERE telegram_owner_id = :oid"),
                        {"url": tenant_db_url, "oid": chat_id},
                    )
                    conn.commit()
                    logger.info(f"ℹ️ Tenant record updated for {chat_id}")

                # Link user to tenant schema
                conn.execute(
                    text("UPDATE users SET tenant_schema = :schema WHERE chat_id = :cid"),
                    {"schema": schema_name, "cid": chat_id},
                )
                conn.commit()
                logger.info(f"✅ Linked user {chat_id} → {schema_name}")

        # Create tables in the schema
        ensure_tenant_tables(database_url, schema_name)
        logger.info(f"✅ Tenant setup complete for chat_id={chat_id}")
        
        # ✅ UPDATED: Generate owner credentials only
        # We DON'T create default users here - they'll be created per shop
        
        # Get owner info from database
        with engine.connect() as conn:
            owner_result = conn.execute(
                text("SELECT username FROM users WHERE chat_id = :cid"),
                {"cid": chat_id}
            ).fetchone()
            
            if owner_result:
                owner_username = owner_result[0]
                # Generate a temporary password for display (owner already has password)
                owner_password = generate_password()
                
                credentials = {
                    "owner": {
                        "username": owner_username,
                        "password": owner_password,  # This is just for display
                        "email": f"{owner_username}@example.com",
                        "note": "You already have an account. This is your username."
                    }
                    # ⚠️ NO admin/shopkeeper created here - they'll be created per shop
                }
        
        return schema_name, credentials  # ✅ Return schema name and owner credentials
        
    except Exception as e:
        logger.error(f"❌ Tenant creation failed for {schema_name}: {e}")
        import traceback
        traceback.print_exc()
        # Still return schema name if possible
        return schema_name, {}

# ======================================================
# 🔹 CREATE SHOP-SPECIFIC USERS (NEW FUNCTION)
# ======================================================
def create_shop_users(chat_id: int, shop_id: int, shop_name: str):
    """
    Create default admin and shopkeeper users for a specific shop.
    Returns: credentials dict for the new users or None if failed
    """
    logger.info(f"👥 Creating shop users for Shop {shop_name} (ID: {shop_id})")
    
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        logger.error("❌ DATABASE_URL is missing")
        return None
    
    engine = create_engine(database_url)
    
    try:
        # Get owner to copy tenant_schema
        with engine.connect() as conn:
            owner_result = conn.execute(
                text("SELECT tenant_schema FROM users WHERE chat_id = :cid AND role = 'owner'"),
                {"cid": chat_id}
            ).fetchone()
            
            if not owner_result or not owner_result[0]:
                logger.error(f"❌ Owner not found or no tenant schema for chat_id: {chat_id}")
                return None
            
            tenant_schema = owner_result[0]
            
            # Generate credentials
            clean_shop_name = re.sub(r'[^a-zA-Z0-9]', '', shop_name.lower())
            timestamp = int(time.time()) % 10000
            
            admin_username = f"admin_{clean_shop_name}_{timestamp}"
            admin_password = generate_password()
            
            shopkeeper_username = f"shopkeeper_{clean_shop_name}_{timestamp}"
            shopkeeper_password = generate_password()
            
            # Create admin user
            conn.execute(
                text("""
                    INSERT INTO users (name, username, email, password_hash, role, shop_id, shop_name, tenant_schema, created_at)
                    VALUES (:name, :username, :email, :password_hash, :role, :shop_id, :shop_name, :tenant_schema, NOW())
                """),
                {
                    "name": f"Admin - {shop_name}",
                    "username": admin_username,
                    "email": f"{admin_username}@example.com",
                    "password_hash": hash_password(admin_password),
                    "role": "admin",
                    "shop_id": shop_id,
                    "shop_name": shop_name,
                    "tenant_schema": tenant_schema
                }
            )
            
            # Create shopkeeper user
            conn.execute(
                text("""
                    INSERT INTO users (name, username, email, password_hash, role, shop_id, shop_name, tenant_schema, created_at)
                    VALUES (:name, :username, :email, :password_hash, :role, :shop_id, :shop_name, :tenant_schema, NOW())
                """),
                {
                    "name": f"Shopkeeper - {shop_name}",
                    "username": shopkeeper_username,
                    "email": f"{shopkeeper_username}@example.com",
                    "password_hash": hash_password(shopkeeper_password),
                    "role": "shopkeeper",
                    "shop_id": shop_id,
                    "shop_name": shop_name,
                    "tenant_schema": tenant_schema
                }
            )
            
            conn.commit()
            
            credentials = {
                "admin": {
                    "username": admin_username,
                    "password": admin_password,
                    "email": f"{admin_username}@example.com",
                    "shop_id": shop_id,
                    "shop_name": shop_name
                },
                "shopkeeper": {
                    "username": shopkeeper_username,
                    "password": shopkeeper_password,
                    "email": f"{shopkeeper_username}@example.com",
                    "shop_id": shop_id,
                    "shop_name": shop_name
                }
            }
            
            logger.info(f"✅ Created users for shop {shop_name} (ID: {shop_id})")
            return credentials
            
    except Exception as e:
        logger.error(f"❌ Failed to create shop users: {e}")
        import traceback
        traceback.print_exc()
        return None

# ======================================================
# 🔹 ENSURE TENANT TABLES (UPDATED FOR SHOP_ID IN PRODUCTS)
# ======================================================
def ensure_tenant_tables(base_url: str, schema_name: str):
    """Ensure all tenant tables exist in the correct schema using raw SQL."""
    logger.info(f"🔄 Ensuring tables in schema: {schema_name}")
    
    try:
        engine = create_engine(base_url, pool_timeout=30, connect_args={'connect_timeout': 10})
        
        with engine.connect() as conn:
            logger.info(f"✅ Connected to database for schema {schema_name}")
            
            # 1. Create schema if not exists
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
            conn.commit()
            logger.info(f"✅ Schema {schema_name} verified")
            
            # 2. Create shops table
            shops_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema_name}.shops (
                    shop_id SERIAL PRIMARY KEY,
                    name VARCHAR(150) NOT NULL,
                    location VARCHAR(255),
                    contact VARCHAR(100),
                    is_main BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """
            conn.execute(text(shops_sql))
            conn.commit()
            logger.info(f"✅ Created shops table in {schema_name}")
            
            # 3. Create products table WITH shop_id (optional for shop-specific products)
            products_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema_name}.products (
                    product_id SERIAL PRIMARY KEY,
                    name VARCHAR(150) NOT NULL,
                    description TEXT,
                    price NUMERIC(10, 2) NOT NULL,
                    unit_type VARCHAR(50) DEFAULT 'unit',
                    shop_id INTEGER,  -- ✅ ADDED: NULL for global products, shop_id for shop-specific
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """
            conn.execute(text(products_sql))
            conn.commit()
            logger.info(f"✅ Created products table in {schema_name}")
            
            # 4. Create product_shop_stock table
            product_shop_stock_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema_name}.product_shop_stock (
                    id SERIAL PRIMARY KEY,
                    product_id INTEGER NOT NULL,
                    shop_id INTEGER NOT NULL,
                    stock INTEGER DEFAULT 0,
                    min_stock_level INTEGER DEFAULT 0,
                    low_stock_threshold INTEGER DEFAULT 10,
                    reorder_quantity INTEGER DEFAULT 0,
                    UNIQUE(product_id, shop_id)
                )
            """
            conn.execute(text(product_shop_stock_sql))
            conn.commit()
            logger.info(f"✅ Created product_shop_stock table in {schema_name}")
            
            # 5. Create customers table
            customers_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema_name}.customers (
                    customer_id SERIAL PRIMARY KEY,
                    name VARCHAR(150),
                    contact VARCHAR(100),
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """
            conn.execute(text(customers_sql))
            conn.commit()
            logger.info(f"✅ Created customers table in {schema_name}")
            
            # 6. Create sales table
            sales_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema_name}.sales (
                    sale_id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    product_id INTEGER,
                    shop_id INTEGER NOT NULL,
                    customer_id INTEGER,
                    unit_type VARCHAR(50) DEFAULT 'unit',
                    quantity INTEGER,
                    total_amount NUMERIC(10, 2),
                    surcharge_amount NUMERIC(10, 2) DEFAULT 0.0,
                    sale_date TIMESTAMP DEFAULT NOW(),
                    payment_type VARCHAR(50) DEFAULT 'full',
                    payment_method VARCHAR(50) DEFAULT 'cash',
                    amount_paid NUMERIC(10, 2) DEFAULT 0.0,
                    pending_amount NUMERIC(10, 2) DEFAULT 0.0,
                    change_left NUMERIC(10, 2) DEFAULT 0.0
                )
            """
            conn.execute(text(sales_sql))
            conn.commit()
            logger.info(f"✅ Created sales table in {schema_name}")
            
            # 7. Create pending_approvals table WITH shop_id
            approvals_sql = f"""
                CREATE TABLE IF NOT EXISTS {schema_name}.pending_approvals (
                    approval_id SERIAL PRIMARY KEY,
                    action_type VARCHAR(50),
                    shopkeeper_id INTEGER,
                    shopkeeper_name VARCHAR(150),
                    shop_id INTEGER NOT NULL,  -- ✅ ADDED
                    product_data TEXT,
                    status VARCHAR(50) DEFAULT 'pending',
                    created_at TIMESTAMP DEFAULT NOW(),
                    resolved_at TIMESTAMP
                )
            """
            conn.execute(text(approvals_sql))
            conn.commit()
            logger.info(f"✅ Created pending_approvals table in {schema_name}")
            
            # ================== FOREIGN KEYS ==================
            
            try:
                # Sales -> Products
                fk_sql = f"""
                    ALTER TABLE {schema_name}.sales 
                    ADD CONSTRAINT fk_sales_products 
                    FOREIGN KEY (product_id) 
                    REFERENCES {schema_name}.products(product_id)
                """
                conn.execute(text(fk_sql))
                conn.commit()
                logger.info(f"✅ Added foreign key: sales → products")
            except Exception as e:
                logger.info(f"ℹ️ Foreign key might already exist: {e}")
            
            try:
                # Sales -> Customers
                fk_sql = f"""
                    ALTER TABLE {schema_name}.sales 
                    ADD CONSTRAINT fk_sales_customers 
                    FOREIGN KEY (customer_id) 
                    REFERENCES {schema_name}.customers(customer_id)
                """
                conn.execute(text(fk_sql))
                conn.commit()
                logger.info(f"✅ Added foreign key: sales → customers")
            except Exception as e:
                logger.info(f"ℹ️ Foreign key might already exist: {e}")
            
            try:
                # Sales -> Shops
                fk_sql = f"""
                    ALTER TABLE {schema_name}.sales 
                    ADD CONSTRAINT fk_sales_shops 
                    FOREIGN KEY (shop_id) 
                    REFERENCES {schema_name}.shops(shop_id)
                """
                conn.execute(text(fk_sql))
                conn.commit()
                logger.info(f"✅ Added foreign key: sales → shops")
            except Exception as e:
                logger.info(f"ℹ️ Foreign key might already exist: {e}")
            
            try:
                # Products -> Shops (optional foreign key for shop-specific products)
                fk_sql = f"""
                    ALTER TABLE {schema_name}.products 
                    ADD CONSTRAINT fk_products_shops 
                    FOREIGN KEY (shop_id) 
                    REFERENCES {schema_name}.shops(shop_id)
                """
                conn.execute(text(fk_sql))
                conn.commit()
                logger.info(f"✅ Added foreign key: products → shops")
            except Exception as e:
                logger.info(f"ℹ️ Foreign key might already exist: {e}")
            
            try:
                # Product_shop_stock -> Products
                fk_sql = f"""
                    ALTER TABLE {schema_name}.product_shop_stock 
                    ADD CONSTRAINT fk_stock_products 
                    FOREIGN KEY (product_id) 
                    REFERENCES {schema_name}.products(product_id)
                """
                conn.execute(text(fk_sql))
                conn.commit()
                logger.info(f"✅ Added foreign key: product_shop_stock → products")
            except Exception as e:
                logger.info(f"ℹ️ Foreign key might already exist: {e}")
            
            try:
                # Product_shop_stock -> Shops
                fk_sql = f"""
                    ALTER TABLE {schema_name}.product_shop_stock 
                    ADD CONSTRAINT fk_stock_shops 
                    FOREIGN KEY (shop_id) 
                    REFERENCES {schema_name}.shops(shop_id)
                """
                conn.execute(text(fk_sql))
                conn.commit()
                logger.info(f"✅ Added foreign key: product_shop_stock → shops")
            except Exception as e:
                logger.info(f"ℹ️ Foreign key might already exist: {e}")
            
            try:
                # Pending_approvals -> Shops
                fk_sql = f"""
                    ALTER TABLE {schema_name}.pending_approvals 
                    ADD CONSTRAINT fk_approvals_shops 
                    FOREIGN KEY (shop_id) 
                    REFERENCES {schema_name}.shops(shop_id)
                """
                conn.execute(text(fk_sql))
                conn.commit()
                logger.info(f"✅ Added foreign key: pending_approvals → shops")
            except Exception as e:
                logger.info(f"ℹ️ Foreign key might already exist: {e}")
            
            # 8. ✅ REMOVED: Don't create default main shop
            # Shops will be created by the owner during setup
            
            logger.info(f"✅ All tables created successfully in '{schema_name}'.")
            
    except Exception as e:
        logger.error(f"❌ Failed to create tenant tables in {schema_name}: {e}")
        logger.error(f"❌ Error details: {str(e)}")

# ======================================================
# 🔹 GET TENANT SESSION (SAME AS BEFORE)
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
            pool_recycle=3600,
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
# 🔹 ENSURE TENANT SESSION (UPDATED FOR MULTI-ROLE)
# ======================================================
def ensure_tenant_session(chat_id, db):
    """
    Return a tenant-specific session.
    For shopkeepers and admins, use their assigned tenant_schema.
    For owners, create schema if needed.
    """
    user = db.query(User).filter(User.chat_id == chat_id).first()
    
    if not user:
        logger.error(f"❌ User not found for chat_id: {chat_id}")
        return None
    
    # ✅ For shopkeepers and admins, use their existing tenant_schema
    if user.role in ["shopkeeper", "admin"]:
        if not user.tenant_schema:
            logger.error(f"❌ {user.role.title()} {chat_id} has no tenant_schema assigned")
            return None
        
        logger.info(f"🔄 {user.role.title()} {chat_id} - using assigned schema: {user.tenant_schema}")
        return get_tenant_session(user.tenant_schema, chat_id)
    
    # ✅ For owners, create schema if needed
    if not user.tenant_schema:
        # Owner doesn't have schema - create one
        schema_name, _ = create_tenant_db(chat_id, user.role)
        if schema_name:
            user.tenant_schema = schema_name
            db.commit()
            return get_tenant_session(schema_name, chat_id)
        else:
            return None
    
    # Owner has schema - use it
    return get_tenant_session(user.tenant_schema, chat_id)

# ======================================================
# 🔹 SHOP MANAGEMENT HELPERS (NEW FUNCTIONS)
# ======================================================
def create_initial_shop(tenant_session, shop_name: str, location: str = "", contact: str = ""):
    """
    Create the first shop for a tenant.
    Returns the created shop object or None.
    """
    try:
        # Check if any shop exists
        existing_shops = tenant_session.query(ShopORM).count()
        
        new_shop = ShopORM(
            name=shop_name,
            location=location,
            contact=contact,
            is_main=(existing_shops == 0)  # First shop is main
        )
        
        tenant_session.add(new_shop)
        tenant_session.commit()
        tenant_session.refresh(new_shop)
        
        logger.info(f"✅ Created initial shop: {shop_name} (ID: {new_shop.shop_id})")
        return new_shop
        
    except Exception as e:
        logger.error(f"❌ Failed to create initial shop: {e}")
        tenant_session.rollback()
        return None

def create_additional_shop(tenant_session, shop_name: str, location: str = "", contact: str = ""):
    """
    Create an additional shop for a tenant.
    Returns the created shop object or None.
    """
    try:
        new_shop = ShopORM(
            name=shop_name,
            location=location,
            contact=contact,
            is_main=False  # Additional shops are not main
        )
        
        tenant_session.add(new_shop)
        tenant_session.commit()
        tenant_session.refresh(new_shop)
        
        logger.info(f"✅ Created additional shop: {shop_name} (ID: {new_shop.shop_id})")
        return new_shop
        
    except Exception as e:
        logger.error(f"❌ Failed to create additional shop: {e}")
        tenant_session.rollback()
        return None