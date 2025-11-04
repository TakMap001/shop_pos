# app/tenant_utils.py
from sqlalchemy import text
import logging
from app.core import engine
from app.models.models import TenantBase

logger = logging.getLogger(__name__)

# In app/tenant_utils.py - Update create_tenant_schema function
# Add this to your create_tenant_schema function in tenant_utils.py
def create_tenant_schema(schema_name):
    """Create a new tenant schema with all required tables - NUCLEAR VERSION"""
    try:
        with engine.connect() as conn:
            logger.info(f"🆕 NUCLEAR DEBUG: Starting schema creation for '{schema_name}'")
            
            # 1. Force drop schema if exists
            conn.execute(text(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE"))
            conn.commit()
            logger.info(f"✅ Dropped schema '{schema_name}'")
            
            # 2. Create fresh schema
            conn.execute(text(f"CREATE SCHEMA {schema_name}"))
            conn.commit()
            logger.info(f"✅ Created fresh schema '{schema_name}'")
            
            # 3. Set search path
            conn.execute(text(f"SET search_path TO {schema_name}"))
            
            # 4. MANUALLY create each table to ensure no data
            conn.execute(text("""
                CREATE TABLE products (
                    product_id SERIAL PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    price DECIMAL(10,2) DEFAULT 0.00,
                    stock INTEGER DEFAULT 0,
                    unit_type VARCHAR(100),
                    min_stock_level INTEGER DEFAULT 0,
                    low_stock_threshold INTEGER DEFAULT 5,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            conn.execute(text("""
                CREATE TABLE customers (
                    customer_id SERIAL PRIMARY KEY,
                    name VARCHAR(255),
                    contact VARCHAR(255),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            conn.execute(text("""
                CREATE TABLE sales (
                    sale_id SERIAL PRIMARY KEY,
                    total_amount DECIMAL(10,2) DEFAULT 0.00,
                    quantity INTEGER DEFAULT 0,
                    payment_method VARCHAR(50),
                    sale_type VARCHAR(50),
                    amount_paid DECIMAL(10,2) DEFAULT 0.00,
                    pending_amount DECIMAL(10,2) DEFAULT 0.00,
                    change_left DECIMAL(10,2) DEFAULT 0.00,
                    customer_id INTEGER REFERENCES customers(customer_id),
                    product_id INTEGER REFERENCES products(product_id),
                    customer_name VARCHAR(255),
                    customer_contact VARCHAR(255),
                    sale_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            
            # 5. VERIFY tables are empty
            product_count = conn.execute(text("SELECT COUNT(*) FROM products")).scalar()
            sales_count = conn.execute(text("SELECT COUNT(*) FROM sales")).scalar()
            customers_count = conn.execute(text("SELECT COUNT(*) FROM customers")).scalar()
            
            logger.info(f"🆕 NUCLEAR DEBUG: After manual creation - Products: {product_count}, Sales: {sales_count}, Customers: {customers_count}")
            
            if product_count > 0 or sales_count > 0 or customers_count > 0:
                logger.error(f"🚨 NUCLEAR DEBUG: Tables created with data! This should never happen!")
                # Nuclear option: drop and recreate individual tables
                conn.execute(text("DROP TABLE IF EXISTS sales CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS products CASCADE"))
                conn.execute(text("DROP TABLE IF EXISTS customers CASCADE"))
                conn.commit()
                # Recreate empty
                conn.execute(text("CREATE TABLE products (product_id SERIAL PRIMARY KEY, name VARCHAR(255) NOT NULL)"))
                conn.execute(text("CREATE TABLE customers (customer_id SERIAL PRIMARY KEY, name VARCHAR(255))"))
                conn.execute(text("CREATE TABLE sales (sale_id SERIAL PRIMARY KEY, product_id INTEGER REFERENCES products(product_id))"))
            
            # Reset search path
            conn.execute(text("SET search_path TO public"))
            conn.commit()
        
        logger.info(f"✅ Tenant schema '{schema_name}' created with EMPTY tables")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to create tenant schema '{schema_name}': {e}")
        return False

def check_tenant_tables_exist(schema_name):
    """Check if all required tables exist in tenant schema"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = :schema
                ORDER BY table_name
            """), {"schema": schema_name})
            
            tables = [row[0] for row in result]
            required_tables = ['customers', 'products', 'sales']
            missing_tables = [t for t in required_tables if t not in tables]
            
            return {
                "exists": len(missing_tables) == 0,
                "tables": tables,
                "missing_tables": missing_tables
            }
            
    except Exception as e:
        logger.error(f"❌ Failed to check tenant tables: {e}")
        return {"exists": False, "error": str(e)}