# app/tenant_utils.py
from sqlalchemy import text
import logging
from app.core import engine
from app.models.models import TenantBase

logger = logging.getLogger(__name__)

def create_tenant_schema(schema_name):
    """Create a new tenant schema with all required tables - FIXED VERSION"""
    try:
        with engine.connect() as conn:
            # ✅ Create schema if not exists
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
            conn.commit()
            
            # ✅ Set search path to the new schema BEFORE creating tables
            conn.execute(text(f"SET search_path TO {schema_name}"))
            
            # ✅ Create all tables in the tenant schema
            # This ensures tables are created in the correct schema
            TenantBase.metadata.create_all(bind=conn)
            
            # ✅ Verify tables were created in correct schema
            result = conn.execute(text("""
                SELECT table_name, table_schema
                FROM information_schema.tables 
                WHERE table_schema = :schema
                ORDER BY table_name
            """), {"schema": schema_name})
            
            created_tables = [row[0] for row in result]
            logger.info(f"🔍 Tables created in {schema_name}: {created_tables}")
            
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