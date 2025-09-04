# app/routes/reports.py

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import func, extract
from datetime import datetime
from app.database import get_db
from app.models.models import SaleORM, ProductORM, User

router = APIRouter(
    prefix="/reports",
    tags=["reports"]
)

@router.get("/total_sales_per_product")
def total_sales_per_product(db: Session = Depends(get_db)):
    """
    Returns total quantity sold and total revenue per product
    """
    results = (
        db.query(
            ProductORM.name,
            func.sum(SaleORM.quantity).label("total_quantity"),
            func.sum(SaleORM.total_amount).label("total_revenue")
        )
        .join(SaleORM, SaleORM.product_id == ProductORM.product_id)
        .group_by(ProductORM.name)
        .all()
    )
    return [{"product": r.name, "total_quantity": r.total_quantity, "total_revenue": float(r.total_revenue)} for r in results]


@router.get("/total_sales_per_user")
def total_sales_per_user(db: Session = Depends(get_db)):
    """
    Returns total quantity purchased and total spent per user
    """
    results = (
        db.query(
            User.name,
            func.sum(SaleORM.quantity).label("total_quantity"),
            func.sum(SaleORM.total_amount).label("total_spent")
        )
        .join(SaleORM, SaleORM.user_id == User.user_id)
        .group_by(User.name)
        .all()
    )
    return [{"user": r.name, "total_quantity": r.total_quantity, "total_spent": float(r.total_spent)} for r in results]


@router.get("/daily_sales")
def daily_sales(db: Session = Depends(get_db)):
    """
    Returns total sales and revenue per day
    """
    results = (
        db.query(
            func.date(SaleORM.sale_date).label("sale_day"),
            func.sum(SaleORM.quantity).label("total_quantity"),
            func.sum(SaleORM.total_amount).label("total_revenue")
        )
        .group_by(func.date(SaleORM.sale_date))
        .order_by(func.date(SaleORM.sale_date))
        .all()
    )
    return [{"date": str(r.sale_day), "total_quantity": r.total_quantity, "total_revenue": float(r.total_revenue)} for r in results]


@router.get("/low_stock_products")
def low_stock_products(db: Session = Depends(get_db), threshold: int = 10):
    """
    Returns products with stock below a certain threshold (default 10)
    """
    results = db.query(ProductORM).filter(ProductORM.stock <= threshold).all()
    return [{"product_id": p.product_id, "name": p.name, "stock": p.stock, "price": float(p.price)} for p in results]

@router.get("/top_selling_products")
def top_selling_products(limit: int = Query(5, gt=0), db: Session = Depends(get_db)):
    """
    Retrieve the top-selling products by total quantity sold.
    Default limit: 5
    """
    results = (
        db.query(
            ProductORM.name.label("product"),
            func.sum(SaleORM.quantity).label("total_quantity"),
            func.sum(SaleORM.total_amount).label("total_revenue")
        )
        .join(SaleORM, ProductORM.product_id == SaleORM.product_id)
        .group_by(ProductORM.name)
        .order_by(func.sum(SaleORM.quantity).desc())
        .limit(limit)
        .all()
    )
    return [dict(r._mapping) for r in results]

@router.get("/top_customers")
def top_customers(limit: int = Query(5, gt=0), db: Session = Depends(get_db)):
    """
    Retrieve the top customers by total quantity purchased.
    Default limit: 5
    """
    results = (
        db.query(
            User.name.label("user"),
            func.sum(SaleORM.quantity).label("total_quantity"),
            func.sum(SaleORM.total_amount).label("total_spent")
        )
        .join(SaleORM, User.user_id == SaleORM.user_id)
        .group_by(User.name)
        .order_by(func.sum(SaleORM.total_amount).desc())
        .limit(limit)
        .all()
    )
    return [dict(r._mapping) for r in results]

@router.get("/monthly_sales_per_product")
def monthly_sales_per_product(
    year: int = Query(None, gt=2000),
    month: int = Query(None, ge=1, le=12),
    db: Session = Depends(get_db)
):
    """
    Get total sales quantity and revenue per product for a given month and year.
    Defaults to current month and year if not provided.
    """
    from datetime import datetime

    now = datetime.now()
    year = year or now.year
    month = month or now.month

    results = (
        db.query(
            ProductORM.name.label("product"),
            func.sum(SaleORM.quantity).label("total_quantity"),
            func.sum(SaleORM.total_amount).label("total_revenue")
        )
        .join(ProductORM, SaleORM.product_id == ProductORM.product_id)
        .filter(extract("year", SaleORM.sale_date) == year)
        .filter(extract("month", SaleORM.sale_date) == month)
        .group_by(ProductORM.name)
        .all()
    )

    return [
        {
            "product": r.product,
            "total_quantity": r.total_quantity or 0,
            "total_revenue": float(r.total_revenue or 0)
        } for r in results
    ]

@router.get("/monthly_sales_per_user")
def monthly_sales_per_user(
    year: int = Query(default=datetime.now().year, description="Year filter"),
    month: int = Query(default=datetime.now().month, description="Month filter"),
    db: Session = Depends(get_db)
):
    """
    Get monthly sales aggregated by user.
    """
    results = (
        db.query(
            User.name.label("user"),
            func.sum(SaleORM.quantity).label("total_quantity"),
            func.sum(SaleORM.total_amount).label("total_spent")
        )
        .join(User, SaleORM.user_id == User.user_id)
        .filter(extract("year", SaleORM.sale_date) == year)
        .filter(extract("month", SaleORM.sale_date) == month)
        .group_by(User.name)
        .all()
    )

    return [
        {"user": r.user, "total_quantity": r.total_quantity, "total_spent": float(r.total_spent)}
        for r in results
    ]

@router.get("/stock_turnover_per_product")
def stock_turnover_per_product(db: Session = Depends(get_db)):
    """
    Returns turnover rate per product: units sold / (stock + units sold)
    """
    products = db.query(ProductORM).all()
    results = []

    for product in products:
        total_sold = db.query(func.sum(SaleORM.quantity))\
                       .filter(SaleORM.product_id == product.product_id)\
                       .scalar() or 0
        turnover_rate = total_sold / (product.stock + total_sold) if (product.stock + total_sold) > 0 else 0
        results.append({
            "product": product.name,
            "units_sold": int(total_sold),
            "stock": product.stock,
            "turnover_rate": round(turnover_rate, 2)
        })

    return results

@router.get("/average_order_value")
def average_order_value(db: Session = Depends(get_db)):
    """
    Returns the average order value
    """
    total_sales = db.query(func.count(SaleORM.sale_id)).scalar() or 0
    total_revenue = db.query(func.sum(SaleORM.total_amount)).scalar() or 0

    aov = round(total_revenue / total_sales, 2) if total_sales > 0 else 0

    return {"total_orders": total_sales, "total_revenue": total_revenue, "average_order_value": aov}


@router.get("/top_repeat_customers")
def top_repeat_customers(limit: int = 5, db: Session = Depends(get_db)):
    """
    Returns top repeat customers by purchase frequency
    """
    customers = db.query(
        Sale.user_id,
        func.count(SaleORM.sale_id).label("num_purchases"),
        func.sum(SaleORM.total_amount).label("total_spent")
    ).group_by(SaleORM.user_id)\
     .order_by(func.count(SaleORM.sale_id).desc())\
     .limit(limit).all()

    results = []
    for c in customers:
        user = db.query(User).filter(User.user_id == c.user_id).first()
        results.append({
            "user": user.name if user else f"User {c.user_id}",
            "num_purchases": c.num_purchases,
            "total_spent": float(c.total_spent)
        })
    return results

@router.get("/weekly_revenue")
def weekly_revenue(year: int = datetime.now().year, db: Session = Depends(get_db)):
    """
    Returns total revenue and quantity per week for a given year
    """
    sales = db.query(
        extract('week', SaleORM.sale_date).label("week"),
        func.sum(SaleORM.quantity).label("total_quantity"),
        func.sum(SaleORM.total_amount).label("total_revenue")
    ).filter(extract('year', SaleORM.sale_date) == year)\
     .group_by("week")\
     .order_by("week").all()

    results = [{"week": int(s.week), "total_quantity": int(s.total_quantity), "total_revenue": float(s.total_revenue)} for s in sales]

    return results

