from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from pydantic import BaseModel
from app.database import get_db
from app.models.models import Product as ProductORM
from app.schemas.schemas import Product, ProductCreate
from typing import List

router = APIRouter(prefix="/products", tags=["Products"])

@router.get("/", response_model=list[Product])
def get_products(db: Session = Depends(get_db)):
    """Return all products"""
    products = db.query(ProductORM).all()  # ORM for DB query
    return products  # FastAPI converts to Pydantic automatically

@router.post("/", response_model=Product)
def create_product(product: ProductCreate, db: Session = Depends(get_db)):
    db_product = ProductORM(**product.dict())  # Use ORM model here
    db.add(db_product)
    db.commit()
    db.refresh(db_product)
    return db_product

@router.patch("/{product_id}/add_stock", response_model=Product)
def add_stock(
    product_id: int,
    quantity: int = Query(..., gt=0, description="Number of units to add"),
    db: Session = Depends(get_db)
):
    """
    Increment the stock of a product.
    - `quantity` must be greater than 0
    - Returns the updated product
    """
    product = db.query(ProductORM).filter(ProductORM.product_id == product_id).first()  # ORM
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")

    product.stock += quantity
    db.commit()
    db.refresh(product)

    return product

@router.patch("/{product_id}/reduce_stock", response_model=Product)
def reduce_stock(
    product_id: int,
    quantity: int = Query(..., gt=0, description="Number of units to reduce"),
    db: Session = Depends(get_db)
):
    """
    Decrement the stock of a product.
    - `quantity` must be greater than 0
    - Prevents stock from going negative
    - Returns the updated product
    """
    product = db.query(ProductORM).filter(ProductORM.product_id == product_id).first()
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")

    if product.stock < quantity:
        raise HTTPException(status_code=400, detail="Insufficient stock to reduce")

    product.stock -= quantity
    db.commit()
    db.refresh(product)

    return product

class StockUpdate(BaseModel):
    product_id: int
    quantity: int

@router.patch("/batch_update_stock", response_model=List[Product])
def batch_update_stock(
    updates: List[StockUpdate],
    db: Session = Depends(get_db)
):
    """
    Update stock for multiple products at once.
    - Positive quantity increases stock
    - Negative quantity reduces stock (validated)
    """
    updated_products = []

    for upd in updates:
        product = db.query(ProductORM).filter(ProductORM.product_id == upd.product_id).first()
        if not product:
            continue  # skip invalid product_ids

        new_stock = product.stock + upd.quantity
        if new_stock < 0:
            raise HTTPException(
                status_code=400,
                detail=f"Insufficient stock for product_id {upd.product_id}"
            )

        product.stock = new_stock
        updated_products.append(product)

    db.commit()
    for p in updated_products:
        db.refresh(p)

    return updated_products

@router.get("/low_stock", response_model=List[Product])
def low_stock(threshold: int = Query(5, gt=0), db: Session = Depends(get_db)):
    """
    List products where stock <= threshold
    """
    products = db.query(ProductORM).filter(ProductORM.stock <= threshold).all()
    return products

