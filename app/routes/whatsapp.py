from fastapi import APIRouter, Form
from app.twilio_client import send_whatsapp_message
from app.database import get_db
from sqlalchemy.orm import Session
from app.routes.products import create_product
from app.schemas.schemas import ProductCreate
from app.models.models import Product as ProductORM, Sale
from datetime import date
from sqlalchemy import func, extract

router = APIRouter(prefix="/whatsapp", tags=["WhatsApp"])

LOW_STOCK_THRESHOLD = 10  # adjust as needed

def check_low_stock_and_alert(db: Session):
    low_stock_products = db.query(ProductORM).filter(ProductORM.stock <= LOW_STOCK_THRESHOLD).all()
    for p in low_stock_products:
        message = f"⚠️ Low stock alert: {p.name} has {p.stock} units left!"
        # Replace with shopkeeper number
        send_whatsapp_message(to="whatsapp:+1234567890", body=message)

@router.post("/webhook")
async def whatsapp_webhook(From: str = Form(...), Body: str = Form(...)):
    incoming_message = Body.strip().lower()
    db: Session = next(get_db())

    try:
        # ---------- PRODUCT MANAGEMENT ----------
        if incoming_message.startswith("add product"):
            # Format: add product;name;price;stock
            _, name, price, stock = incoming_message.split(";")
            product_data = ProductCreate(
                name=name.strip(),
                description="Added via WhatsApp",
                price=float(price),
                stock=int(stock)
            )
            product = create_product(product_data, db)
            reply = f"✅ Product added: {product.name} (Stock: {product.stock})"

        elif incoming_message.startswith("stock add"):
            # Format: stock add;product_id;quantity
            _, product_id, quantity = incoming_message.split(";")
            product = db.query(ProductORM).filter(ProductORM.product_id==int(product_id)).first()
            if product:
                product.stock += int(quantity)
                db.commit()
                db.refresh(product)
                reply = f"✅ Stock added. {product.name} new stock: {product.stock}"
            else:
                reply = "❌ Product ID not found"

        elif incoming_message.startswith("stock reduce"):
            # Format: stock reduce;product_id;quantity
            _, product_id, quantity = incoming_message.split(";")
            product = db.query(ProductORM).filter(ProductORM.product_id==int(product_id)).first()
            if product:
                product.stock = max(0, product.stock - int(quantity))
                db.commit()
                db.refresh(product)
                reply = f"✅ Stock reduced. {product.name} new stock: {product.stock}"
            else:
                reply = "❌ Product ID not found"

        elif incoming_message.startswith("low stock"):
            threshold = 10  # default threshold
            products = db.query(ProductORM).filter(ProductORM.stock <= threshold).all()
            if products:
                reply = "⚠️ Low stock products:\n" + "\n".join([f"{p.product_id}. {p.name}: {p.stock}" for p in products])
            else:
                reply = "✅ No products with low stock"

        # ---------- SALES REPORTS ----------
        elif incoming_message.startswith("sales daily"):
            today = date.today()
            sales = db.query(Sale.product_id, func.sum(Sale.quantity), func.sum(Sale.total_amount))\
                      .filter(func.date(Sale.sale_date) == today)\
                      .group_by(Sale.product_id).all()
            if sales:
                reply_lines = []
                for pid, qty, total in sales:
                    product = db.query(ProductORM).filter(ProductORM.product_id==pid).first()
                    reply_lines.append(f"{product.name}: Sold {qty}, Revenue {total}")
                reply = "\n".join(reply_lines)
            else:
                reply = "No sales today"

        elif incoming_message.startswith("sales monthly"):
            year = date.today().year
            month = date.today().month
            sales = db.query(Sale.product_id, func.sum(Sale.quantity), func.sum(Sale.total_amount))\
                      .filter(extract("year", Sale.sale_date)==year)\
                      .filter(extract("month", Sale.sale_date)==month)\
                      .group_by(Sale.product_id).all()
            if sales:
                reply_lines = []
                for pid, qty, total in sales:
                    product = db.query(ProductORM).filter(ProductORM.product_id==pid).first()
                    reply_lines.append(f"{product.name}: Sold {qty}, Revenue {total}")
                reply = "\n".join(reply_lines)
            else:
                reply = "No sales this month"

        elif incoming_message.startswith("sales top products"):
            # Top 5 products by quantity sold
            sales = db.query(Sale.product_id, func.sum(Sale.quantity).label("total_qty"))\
                      .group_by(Sale.product_id)\
                      .order_by(func.sum(Sale.quantity).desc())\
                      .limit(5).all()
            if sales:
                reply_lines = []
                for pid, total_qty in sales:
                    product = db.query(ProductORM).filter(ProductORM.product_id==pid).first()
                    reply_lines.append(f"{product.name}: {total_qty} units sold")
                reply = "🏆 Top products:\n" + "\n".join(reply_lines)
            else:
                reply = "No sales yet"

        elif incoming_message.startswith("sales top customers"):
            # Top 5 customers by total spent
            sales = db.query(Sale.user_id, func.sum(Sale.total_amount).label("total_spent"))\
                      .group_by(Sale.user_id)\
                      .order_by(func.sum(Sale.total_amount).desc())\
                      .limit(5).all()
            if sales:
                reply_lines = [f"User {uid}: Spent {spent}" for uid, spent in sales]
                reply = "🏆 Top customers:\n" + "\n".join(reply_lines)
            else:
                reply = "No sales yet"

        # ---------- HELP / DEFAULT ----------
        elif incoming_message.startswith("stock batch"):
            # Format: stock batch;1:10,2:5
            try:
                _, batch_data = incoming_message.split(";")
                updates = batch_data.split(",")
                reply_lines = []
                for item in updates:
                    pid, qty = item.split(":")
                    product = db.query(ProductORM).filter(ProductORM.product_id==int(pid)).first()
                    if product:
                        product.stock += int(qty)
                        db.commit()
                        db.refresh(product)
                        reply_lines.append(f"{product.name}: +{qty} units, new stock {product.stock}")
                    else:
                        reply_lines.append(f"Product ID {pid} not found")
                reply = "✅ Batch stock update:\n" + "\n".join(reply_lines)
            except Exception:
                reply = "❌ Failed batch update. Format: stock batch;1:10,2:5"

        else:
            reply = (
                "📋 WhatsApp Shop Commands:\n"
                "- add product;name;price;stock\n"
                "- stock add;product_id;qty\n"
                "- stock reduce;product_id;qty\n"
                "- low stock\n"
                "- sales daily\n"
                "- sales monthly\n"
                "- sales top products\n"
                "- sales top customers"
            )

    except Exception as e:
        reply = f"❌ Failed to process command. Please check format. Error: {str(e)}"

    # send reply back to WhatsApp
    send_whatsapp_message(to=From, body=reply)
    return "OK"

