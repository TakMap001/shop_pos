# app/models/models.py
from sqlalchemy import Column, Integer, String, Text, Numeric, DateTime, TIMESTAMP, ForeignKey, BigInteger, Boolean, UniqueConstraint
from app.models.tenant_base import TenantBase  # tenant DB Base
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from datetime import datetime


# -------------------- Tenant DB Models --------------------

# Shop-User mapping table (in tenant schema) - REMOVED
# We'll handle user assignments in central DB instead

class ShopORM(TenantBase):
    __tablename__ = "shops"
    
    shop_id = Column(Integer, primary_key=True, index=True)
    name = Column(String(150), nullable=False)
    location = Column(String(255))
    contact = Column(String(100))
    is_main = Column(Boolean, default=False)  # Main/headquarters shop
    created_at = Column(TIMESTAMP, server_default=func.now())
    
    # ✅ Relationships within tenant schema only
    product_stocks = relationship("ProductShopStockORM", back_populates="shop", cascade="all, delete-orphan")
    sales = relationship("SaleORM", back_populates="shop")
    
    # ❌ REMOVED: Cross-schema relationship to User
    # users = relationship("User", backref="assigned_shop", primaryjoin="remote(User.shop_id) == foreign(ShopORM.shop_id)")
    # ❌ REMOVED: shop_users relationship since we're handling in central DB


class ProductORM(TenantBase):
    __tablename__ = "products"

    product_id = Column(Integer, primary_key=True, index=True)
    name = Column(String(150), nullable=False, index=True)
    description = Column(Text)
    price = Column(Numeric(10, 2), nullable=False)
    unit_type = Column(String(50), default="unit")
    shop_id = Column(Integer, ForeignKey("shops.shop_id"), nullable=True)  # ✅ ADD: NULL for global products
    created_at = Column(TIMESTAMP, server_default=func.now())

    # Relationships
    shop = relationship("ShopORM", backref="products")  # ✅ ADD
    shop_stocks = relationship("ProductShopStockORM", back_populates="product", cascade="all, delete-orphan")
    sales = relationship("SaleORM", back_populates="product")
    

class ProductShopStockORM(TenantBase):
    __tablename__ = "product_shop_stock"
    
    id = Column(Integer, primary_key=True, index=True)
    product_id = Column(Integer, ForeignKey("products.product_id"), nullable=False)
    shop_id = Column(Integer, ForeignKey("shops.shop_id"), nullable=False)
    
    # ✅ Stock fields PER SHOP (each shop has its own stock levels)
    stock = Column(Integer, default=0)
    min_stock_level = Column(Integer, default=0)
    low_stock_threshold = Column(Integer, default=10)
    reorder_quantity = Column(Integer, default=0)
    
    # Relationships
    product = relationship("ProductORM", back_populates="shop_stocks")
    shop = relationship("ShopORM", back_populates="product_stocks")
    
    # Unique constraint - one stock record per product per shop
    __table_args__ = (UniqueConstraint('product_id', 'shop_id', name='unique_product_shop'),)
    
    # Helper method to check if stock is low for THIS shop
    def is_low_stock(self):
        return self.stock <= self.low_stock_threshold
    
    # Helper method to check if stock is at or below minimum for THIS shop
    def is_at_minimum(self):
        return self.stock <= self.min_stock_level


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
    user_id = Column(BigInteger, nullable=True) 
    product_id = Column(Integer, ForeignKey("products.product_id"))
    shop_id = Column(Integer, ForeignKey("shops.shop_id"), nullable=False)  # ✅ REQUIRED: Which shop made the sale
    customer_id = Column(Integer, ForeignKey("customers.customer_id"), nullable=True)

    unit_type = Column(String(50), default="unit")
    quantity = Column(Integer)
    total_amount = Column(Numeric(10, 2))
    surcharge_amount = Column(Numeric(10, 2), default=0.0)  # Track ecocash surcharge
    sale_date = Column(DateTime, default=datetime.utcnow)

    payment_type = Column(String(50), default="full")
    payment_method = Column(String(50), default="cash")
    amount_paid = Column(Numeric(10, 2), default=0.0)
    pending_amount = Column(Numeric(10, 2), default=0.0)
    change_left = Column(Numeric(10, 2), default=0.0)

    # Relationships
    product = relationship("ProductORM", back_populates="sales")
    shop = relationship("ShopORM", back_populates="sales")  # ✅ NEW
    customer = relationship("CustomerORM", back_populates="sales")
    
    
# In app/models/models.py - PendingApprovalORM
class PendingApprovalORM(TenantBase):
    __tablename__ = "pending_approvals"

    approval_id = Column(Integer, primary_key=True, index=True)
    action_type = Column(String(50), nullable=False)
    shopkeeper_id = Column(Integer, nullable=False)
    shopkeeper_name = Column(String(150), nullable=False)
    shop_id = Column(Integer, nullable=False)  # ✅ ADD: Which shop this approval is for
    product_data = Column(Text)
    status = Column(String(20), default='pending')
    created_at = Column(TIMESTAMP, server_default=func.now())
    resolved_at = Column(TIMESTAMP, nullable=True)
    
