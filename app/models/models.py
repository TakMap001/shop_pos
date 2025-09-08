#app/models/models.py
from sqlalchemy import Column, Integer, String, Text, Numeric, DateTime, TIMESTAMP, ForeignKey
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from app.database import Base
from datetime import datetime
from sqlalchemy import BigInteger

class User(Base):
    __tablename__ = "users"

    user_id = Column(BigInteger, primary_key=True, index=True)
    name = Column(String(255))
    email = Column(String(255))
    password_hash = Column(String(255))
    role = Column(String(50))
    tenant_db_url = Column(Text, nullable=True)  # <-- Add this
    created_at = Column(TIMESTAMP, server_default=func.now())

class ProductORM(Base):
    __tablename__ = "products"
    product_id = Column(Integer, primary_key=True, index=True)
    name = Column(String(150), nullable=False, index=True)
    description = Column(Text)
    price = Column(Numeric(10, 2), nullable=False)
    stock = Column(Integer, default=0)
    low_stock_threshold = Column(Integer, default=10)  # for alerts
    created_at = Column(TIMESTAMP, server_default=func.now())

class SaleORM(Base):
    __tablename__ = "sales"
    
    sale_id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.user_id"))
    product_id = Column(Integer, ForeignKey("products.product_id"))
    quantity = Column(Integer)
    total_amount = Column(Numeric(10,2))
    sale_date = Column(DateTime, default=datetime.utcnow)  # <-- ensure default

    # Relationships (inside tenant DB only)
    user = relationship("User")
    product = relationship("ProductORM")
