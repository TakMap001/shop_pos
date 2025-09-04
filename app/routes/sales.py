# app/routes/sales.py

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from decimal import Decimal
from app.database import get_db
from app.models.models import SaleORM, ProductORM, User
from app.schemas.schemas import SaleCreate, Sale

router = APIRouter(
    prefix="/sales",
    tags=["sales"]
)

@router.post("/", response_model=Sale)
def create_sale(sale: SaleCreate, db: Session = Depends(get_db)):
    """
    Create a new sale.
    - Checks if product exists
    - Checks if user exists
    - Checks if stock is sufficient
    - Calculates total_amount automatically
    - Reduces product stock
    """
    user = db.query(User).filter(User.user_id == sale.user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    product = db.query(Product).filter(Product.product_id == sale.product_id).first()
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")

    if product.stock < sale.quantity:
        raise HTTPException(status_code=400, detail="Insufficient stock")

    total_amount = Decimal(product.price) * sale.quantity

    db_sale = SaleORM(
        user_id=sale.user_id,
        product_id=sale.product_id,
        quantity=sale.quantity,
        total_amount=total_amount
    )

    product.stock -= sale.quantity

    db.add(db_sale)
    db.commit()
    db.refresh(db_sale)

    return db_sale


@router.get("/", response_model=list[Sale])
def get_all_sales(db: Session = Depends(get_db)):
    """Retrieve all sales records"""
    return db.query(Sale).all()


@router.get("/{sale_id}", response_model=SaleORM)
def get_sale(sale_id: int, db: Session = Depends(get_db)):
    """Retrieve a specific sale by ID"""
    sale = db.query(SaleORM).filter(SaleORM.sale_id == sale_id).first()
    if not sale:
        raise HTTPException(status_code=404, detail="Sale not found")
    return sale


@router.delete("/{sale_id}", response_model=dict)
def delete_sale(sale_id: int, db: Session = Depends(get_db)):
    """
    Delete a sale by ID.
    - Restores product stock when sale is deleted
    """
    sale = db.query(SaleORM).filter(SaleORM.sale_id == sale_id).first()
    if not sale:
        raise HTTPException(status_code=404, detail="Sale not found")

    product = db.query(ProductORM).filter(ProductORM.product_id == sale.product_id).first()
    if product:
        product.stock += sale.quantity  # restore stock

    db.delete(sale)
    db.commit()

    return {"message": f"Sale {sale_id} deleted successfully"}


@router.put("/{sale_id}", response_model=Sale)
def update_sale(sale_id: int, updated_sale: SaleCreate, db: Session = Depends(get_db)):
    """
    Update a sale by ID.
    - Adjusts stock difference
    - Recalculates total_amount
    """
    sale = db.query(SaleORM).filter(SaleORM.sale_id == sale_id).first()
    if not sale:
        raise HTTPException(status_code=404, detail="Sale not found")

    product = db.query(ProductORM).filter(ProductORM.product_id == updated_sale.product_id).first()
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")

    # restore old stock before applying new sale
    old_quantity = sale.quantity
    product.stock += old_quantity

    # check new stock availability
    if product.stock < updated_sale.quantity:
        raise HTTPException(status_code=400, detail="Insufficient stock for update")

    # apply updates
    sale.user_id = updated_sale.user_id
    sale.product_id = updated_sale.product_id
    sale.quantity = updated_sale.quantity
    sale.total_amount = Decimal(product.price) * updated_sale.quantity

    product.stock -= updated_sale.quantity

    db.commit()
    db.refresh(sale)

    return sale

