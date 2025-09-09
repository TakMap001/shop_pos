# app/tenants.py
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import sessionmaker
import psycopg2
from app.models.central_models import Base as CentralBase
from config import DATABASE_URL

def create_central_db():
    """
    Ensures central DB tables exist at startup.
    """
    engine = create_engine(DATABASE_URL, echo=False, future=True, pool_pre_ping=True)
    CentralBase.metadata.create_all(bind=engine)
    print("✅ Central DB tables created")

def create_tenant_db(tenant_db_url: str):
    """
    Creates a new database for the tenant if it doesn't already exist.
    """
    db_name = tenant_db_url.rsplit("/", 1)[-1]
    default_url = tenant_db_url.rsplit("/", 1)[0] + "/postgres"  # connect to default DB
    engine = create_engine(default_url, isolation_level="AUTOCOMMIT")

    try:
        with engine.connect() as conn:
            conn.execute(text(f'CREATE DATABASE "{db_name}"'))
            print(f"✅ Database {db_name} created successfully")
    except ProgrammingError as e:
        # Check Postgres error code 42P04 = duplicate_database
        if hasattr(e.orig, "pgcode") and e.orig.pgcode == "42P04":
            # Database already exists, silently pass
            pass
        else:
            # Re-raise other errors
            raise

def get_engine_for_tenant(tenant_db_url: str):
    """
    Returns a SQLAlchemy engine for a tenant database.
    """
    return create_engine(tenant_db_url, echo=False, future=True, pool_pre_ping=True)

def get_session_for_tenant(tenant_db_url: str):
    """
    Returns a sessionmaker (factory) for the tenant database.
    Usage:
        SessionLocal = get_session_for_tenant(url)
        db = SessionLocal()  # create session
    """
    engine = get_engine_for_tenant(tenant_db_url)
    SessionLocal = sessionmaker(
        autocommit=False,
        autoflush=False,
        bind=engine,
        future=True
    )
    return SessionLocal
