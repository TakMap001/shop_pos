# app/core.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from config import DATABASE_URL

# -------------------- Central DB Base --------------------
Base = declarative_base()  # <-- central DB models import this

# -------------------- Engine & Session --------------------
engine = create_engine(
    DATABASE_URL,
    echo=False,
    future=True,
    pool_pre_ping=True
)
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
    future=True
)

def init_db():
    """Initialize central database tables (Tenant, User, etc.)"""
    from app.models.central_models import Base as CentralBase
    try:
        CentralBase.metadata.create_all(bind=engine)
        print("✅ Central DB tables created / verified successfully.")
    except Exception as e:
        print("❌ Failed to initialize central DB:", e)

# -------------------- Dependency --------------------
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# -------------------- Tenant DB Helper --------------------
def get_engine_for_tenant(tenant_db_url: str):
    """Return an engine for a tenant DB"""
    return create_engine(
        tenant_db_url,
        echo=False,
        future=True,
        pool_pre_ping=True
    )

def get_tenant_session(tenant_db_url: str):
    """Return a session for a tenant DB"""
    engine = get_engine_for_tenant(tenant_db_url)
    Session = sessionmaker(bind=engine, autocommit=False, autoflush=False, future=True)
    return Session()

