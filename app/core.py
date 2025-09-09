# app/core.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from config import DATABASE_URL

# -------------------- Central DB Base --------------------
Base = declarative_base()  # <-- this is what models will import

# -------------------- Engine & Session --------------------
engine = create_engine(DATABASE_URL, echo=False, future=True, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine, future=True)

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
