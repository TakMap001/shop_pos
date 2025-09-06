# app/models/central_models.py
from sqlalchemy import Column, String, BigInteger, DateTime
from sqlalchemy.orm import declarative_base
from datetime import datetime

Base = declarative_base()

class Tenant(Base):
    __tablename__ = "tenants"
    
    tenant_id = Column(String, primary_key=True)
    store_name = Column(String, nullable=False)
    telegram_owner_id = Column(BigInteger, unique=True, nullable=False)
    database_url = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

