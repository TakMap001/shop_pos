from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.database import get_db
from sqlalchemy import text

router = APIRouter(prefix="/views", tags=["Views"])

# Helper function to execute raw SQL on a view
def fetch_view(db: Session, view_name: str):
    result = db.execute(text(f"SELECT * FROM {view_name}"))
    return [dict(row._mapping) for row in result]

# Low stock products
@router.get("/low_stock_products")
def low_stock_products(db: Session = Depends(get_db)):
    return fetch_view(db, "low_stock_products")

# Recent products
@router.get("/recent_products")
def recent_products(db: Session = Depends(get_db)):
    return fetch_view(db, "recent_products")

# Product inventory value
@router.get("/product_inventory_value")
def product_inventory_value(db: Session = Depends(get_db)):
    return fetch_view(db, "product_inventory_value")

# Product summary
@router.get("/product_summary")
def product_summary(db: Session = Depends(get_db)):
    return fetch_view(db, "product_summary")

# Sales detailed
@router.get("/sales_detailed")
def sales_detailed(db: Session = Depends(get_db)):
    return fetch_view(db, "sales_detailed")

# Daily sales summary
@router.get("/daily_sales_summary")
def daily_sales_summary(db: Session = Depends(get_db)):
    return fetch_view(db, "daily_sales_summary")

# Sales by product
@router.get("/sales_by_product")
def sales_by_product(db: Session = Depends(get_db)):
    return fetch_view(db, "sales_by_product")

# Sales by customer
@router.get("/sales_by_customer")
def sales_by_customer(db: Session = Depends(get_db)):
    return fetch_view(db, "sales_by_customer")

