# app/models/models.py
from sqlalchemy import Column, Integer, String, Text, Numeric, DateTime, TIMESTAMP, ForeignKey
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from app.core import Base  # Central DB uses Base
from datetime import datetime
from sqlalchemy import BigInteger

# -------------------- Central DB Models --------------------
class User(Base):
    __tablename__ = "users"

    user_id = Column(BigInteger, primary_key=True, index=True)
    name = Column(String(255))
    email = Column(String(255))
    password_hash = Column(String(255))
    chat_id = Column(BigInteger, unique=True, index=True, nullable=False)
    role = Column(String(50))
    tenant_db_url = Column(Text, nullable=True)  # link to tenant DB
    created_at = Column(TIMESTAMP, server_default=func.now())


# -------------------- Tenant DB Models --------------------
from app.models.tenant_base import TenantBase  # tenant DB Base


class ProductORM(TenantBase):
    __tablename__ = "products"

    product_id = Column(Integer, primary_key=True, index=True)
    name = Column(String(150), nullable=False, index=True)
    description = Column(Text)
    price = Column(Numeric(10, 2), nullable=False)
    stock = Column(Integer, default=0)
    min_stock_level = Column(Integer, default=0)
    low_stock_threshold = Column(Integer, default=10)
    unit_type = Column(String(50), default="unit")
    created_at = Column(TIMESTAMP, server_default=func.now())

    sales = relationship("SaleORM", back_populates="product")


class CustomerORM(TenantBase):
    __tablename__ = "customers"

    customer_id = Column(Integer, primary_key=True, index=True)
    name = Column(String(150), nullable=True)
    contact = Column(String(50), nullable=True)
    created_at = Column(TIMESTAMP, server_default=func.now())

    # Optional: relationship with sales
    sales = relationship("SaleORM", back_populates="customer")


class SaleORM(TenantBase):
    __tablename__ = "sales"

    sale_id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, nullable=True)  # shopkeeper ID (from central DB, stored as int only)
    product_id = Column(Integer, ForeignKey("products.product_id"))
    customer_id = Column(Integer, ForeignKey("customers.customer_id"), nullable=True)

    unit_type = Column(String(50), default="unit")
    quantity = Column(Integer)
    total_amount = Column(Numeric(10, 2))
    sale_date = Column(DateTime, default=datetime.utcnow)

    payment_type = Column(String(50), default="full")
    amount_paid = Column(Numeric(10, 2), default=0.0)
    pending_amount = Column(Numeric(10, 2), default=0.0)
    change_left = Column(Numeric(10, 2), default=0.0)

    # Relationships
    product = relationship("ProductORM", back_populates="sales")
    customer = relationship("CustomerORM", back_populates="sales")
