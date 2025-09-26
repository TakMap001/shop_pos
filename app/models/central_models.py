# app/models/central_models.py
from sqlalchemy import Column, String, BigInteger, DateTime
from datetime import datetime

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
