# app/tenant_utils.py
from sqlalchemy import text
import logging
from app.core import engine
from app.models.models import TenantBase

logger = logging.getLogger(__name__)

# In app/tenant_utils.py - Update create_tenant_schema function
# Add this to your create_tenant_schema function in tenant_utils.py
def create_tenant_schema(schema_name):
    """Create a new tenant schema with all required tables - ULTRA DEBUG VERSION"""
    try:
        with engine.connect() as conn:
            logger.info(f"🆕 ULTRA DEBUG: Starting schema creation for '{schema_name}'")
            
            # Check if schema exists and what's in it BEFORE we drop it
            schema_exists = conn.execute(
                text("SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = :schema)"),
                {"schema": schema_name}
            ).scalar()
            logger.info(f"🆕 ULTRA DEBUG: Schema exists before drop: {schema_exists}")
            
            if schema_exists:
                # Check what tables and data exist before drop
                tables = conn.execute(
                    text("SELECT table_name FROM information_schema.tables WHERE table_schema = :schema"),
                    {"schema": schema_name}
                ).fetchall()
                logger.info(f"🆕 ULTRA DEBUG: Tables before drop: {[t[0] for t in tables]}")
                
                if tables:
                    # Check product count before drop
                    try:
                        product_count = conn.execute(
                            text(f'SELECT COUNT(*) FROM "{schema_name}".products')
                        ).scalar()
                        logger.info(f"🆕 ULTRA DEBUG: Products before drop: {product_count}")
                    except:
                        logger.info("🆕 ULTRA DEBUG: Could not count products (table might not exist)")
            
            # ✅ FORCE DROP AND RECREATE
            conn.execute(text(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE"))
            conn.execute(text(f"CREATE SCHEMA {schema_name}"))
            conn.commit()
            logger.info(f"✅ Fresh schema '{schema_name}' created")
            
            # Set search path to the new schema
            conn.execute(text(f"SET search_path TO {schema_name}"))
            
            # Create all tables in the tenant schema
            logger.info(f"🆕 ULTRA DEBUG: About to create tables in '{schema_name}'")
            TenantBase.metadata.create_all(bind=conn)
            
            # ✅ VERIFY TABLES ARE EMPTY AFTER CREATION
            product_count_after = conn.execute(text("SELECT COUNT(*) FROM products")).scalar()
            logger.info(f"🆕 ULTRA DEBUG: Products after table creation: {product_count_after}")
            
            if product_count_after > 0:
                logger.error(f"🚨 ULTRA DEBUG: Tables created with {product_count_after} existing products!")
                # Emergency: delete any products that magically appeared
                conn.execute(text("DELETE FROM products"))
                conn.execute(text("DELETE FROM sales"))
                conn.execute(text("DELETE FROM customers"))
                conn.commit()
                logger.info(f"🆕 ULTRA DEBUG: Emergency cleared {product_count_after} products")
            
            # Reset search path
            conn.execute(text("SET search_path TO public"))
            conn.commit()
        
        logger.info(f"✅ Tenant schema '{schema_name}' created with all tables")
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