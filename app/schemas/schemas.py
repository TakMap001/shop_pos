from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class ProductBase(BaseModel):
    name: str
    description: Optional[str] = None
    price: float
    stock: int = 0

class ProductCreate(ProductBase):
    pass

class Product(ProductBase):
    product_id: int
    created_at: Optional[datetime]

    class Config:
        orm_mode = True

class UserBase(BaseModel):
    name: str
    email: str

class UserCreate(UserBase):
    password_hash: str

class User(UserBase):
    user_id: int
    created_at: Optional[datetime]

    class Config:
        orm_mode = True

class SaleBase(BaseModel):
    user_id: int
    product_id: int
    quantity: int
    total_amount: float

class SaleCreate(SaleBase):
    total_amount: float = 0

class Sale(BaseModel):
    sale_id: int
    user_id: int
    product_id: int
    quantity: int
    total_amount: float
    sale_date: datetime  # updated

    class Config:
        from_attributes = True  # for SQLAlchemy ORM


