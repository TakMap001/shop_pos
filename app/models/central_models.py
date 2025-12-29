# app/models/central_models.py
from sqlalchemy import Column, String, BigInteger, DateTime, Integer, TIMESTAMP, Boolean
from datetime import datetime
from sqlalchemy.sql import func

# ✅ Import the shared Base from core
from app.core import Base


class Tenant(Base):
    __tablename__ = "tenants"

    tenant_id = Column(String, primary_key=True)
    store_name = Column(String, nullable=False)
    telegram_owner_id = Column(BigInteger, unique=True, nullable=False)
    database_url = Column(String, nullable=False)

    # Optional metadata
    location = Column(String, nullable=True)
    contact = Column(String, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow)


class User(Base):
    __tablename__ = "users"
    
    user_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255))
    username = Column(String(255), unique=True, index=True)
    email = Column(String(255))
    password_hash = Column(String(255))
    chat_id = Column(BigInteger, unique=True, nullable=True) 
    role = Column(String(50))  # 'owner', 'admin', 'shopkeeper'
    shop_id = Column(Integer, nullable=True)  # Which shop this account is for
    shop_name = Column(String(255), nullable=True)  # ✅ ADD THIS: Shop name for display
    tenant_schema = Column(String(255), nullable=True)
    created_at = Column(TIMESTAMP, server_default=func.now())
    created_by = Column(String(255), nullable=True)
    is_active = Column(Boolean, default=True)
    
    
    # No relationships to tenant models (different schemas)