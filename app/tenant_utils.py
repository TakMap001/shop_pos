# app/tenant_utils.py
from sqlalchemy import text
import logging
from app.core import engine
from app.models.models import TenantBase

logger = logging.getLogger(__name__)

# In app/tenant_utils.py - Update create_tenant_schema function
def create_tenant_schema(schema_name):
    """Create a new tenant schema with all required tables - IMPROVED VERSION"""
    try:
        with engine.connect() as conn:
            # ✅ DROP AND RECREATE for complete freshness
            conn.execute(text(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE"))
            conn.execute(text(f"CREATE SCHEMA {schema_name}"))
            conn.commit()
            
            logger.info(f"✅ Fresh schema '{schema_name}' created")
            
            # Set search path to the new schema
            conn.execute(text(f"SET search_path TO {schema_name}"))
            
            # Create all tables in the tenant schema
            TenantBase.metadata.create_all(bind=conn)
            
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