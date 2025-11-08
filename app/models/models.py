# app/models/models.py
from sqlalchemy import Column, Integer, String, Text, Numeric, DateTime, TIMESTAMP, ForeignKey, BigInteger
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from datetime import datetime

# ✅ Import central DB Base
from app.core import Base  

# -------------------- Central DB Models --------------------

class User(Base):
    __tablename__ = "users"

    user_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255))
    username = Column(String(255), unique=True, index=True)
    email = Column(String(255))
    password_hash = Column(String(255))
    chat_id = Column(BigInteger, unique=True, nullable=True) 
    role = Column(String(50))

    # Store only tenant schema name (e.g., 'tenant_782962404')
    tenant_schema = Column(String(255), nullable=True)

    created_at = Column(TIMESTAMP, server_default=func.now())

    def get_tenant_db_url(self, base_url: str):
        """
        Returns full SQLAlchemy DB URL for this tenant.
        base_url should be DATABASE_URL without DB name (e.g., postgresql://user:pass@host:port)
        """
        if not self.tenant_schema:
            return None
        return f"{base_url}/{self.tenant_schema}"


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

    sales = relationship("SaleORM", back_populates="customer")


class SaleORM(TenantBase):
    __tablename__ = "sales"

    sale_id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, nullable=True)  # shopkeeper ID (from central DB)
    product_id = Column(Integer, ForeignKey("products.product_id"))
    customer_id = Column(Integer, ForeignKey("customers.customer_id"), nullable=True)

    unit_type = Column(String(50), default="unit")
    quantity = Column(Integer)
    total_amount = Column(Numeric(10, 2))
    sale_date = Column(DateTime, default=datetime.utcnow)

    payment_type = Column(String(50), default="full")
    payment_method = Column(String(50), default="cash")  # ✅ ADD THIS FIELD
    amount_paid = Column(Numeric(10, 2), default=0.0)
    pending_amount = Column(Numeric(10, 2), default=0.0)
    change_left = Column(Numeric(10, 2), default=0.0)

    product = relationship("ProductORM", back_populates="sales")
    customer = relationship("CustomerORM", back_populates="sales")
    

class PendingApprovalORM(TenantBase):
    __tablename__ = "pending_approvals"

    approval_id = Column(Integer, primary_key=True, index=True)
    action_type = Column(String(50), nullable=False)  # 'add_product', 'update_product', 'stock_update'
    shopkeeper_id = Column(Integer, nullable=False)
    shopkeeper_name = Column(String(150), nullable=False)
    product_data = Column(Text)  # JSON string of product data
    status = Column(String(20), default='pending')  # pending, approved, rejected
    created_at = Column(TIMESTAMP, server_default=func.now())
    resolved_at = Column(TIMESTAMP, nullable=True)