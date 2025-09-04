# app/routes/telegram.py

from fastapi import APIRouter, Request, Depends
import requests, os
from sqlalchemy.orm import Session
from decimal import Decimal
from datetime import datetime
from sqlalchemy import func, extract
from app.database import get_db
from app.models.models import Product as ProductORM, Sale as SaleORM, User
from app.schemas.schemas import SaleCreate

router = APIRouter()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"


# ------------------- UTILITY FUNCTIONS -------------------

def send_message(chat_id, text, keyboard=None):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
    if keyboard:
        payload["reply_markup"] = keyboard
    requests.post(f"{TELEGRAM_API_URL}/sendMessage", json=payload)


def main_menu(chat_id):
    keyboard = {
        "inline_keyboard": [
            [{"text": "➕ Add Product", "callback_data": "add_product"}],
            [{"text": "🛒 Record Sale", "callback_data": "record_sale"}],
            [{"text": "📦 View Stock", "callback_data": "view_stock"}],
            [{"text": "📊 Reports", "callback_data": "reports"}],
        ]
    }
    send_message(chat_id, "📋 Main Menu:", keyboard)


# ------------------- PRODUCTS -------------------

def get_stock_list(db: Session):
    products = db.query(ProductORM).all()
    if not products:
        return "📦 No products found."
    lines = ["📦 *Stock Levels:*"]
    for p in products:
        lines.append(f"{p.name} — {p.stock}")
    return "\n".join(lines)


def add_product(db: Session, chat_id: int, text: str):
    """
    Parse product details: 'name;price;stock'
    Adds new product to DB
    """
    try:
        name, price, stock = text.split(";")
        price = float(price)
        stock = int(stock)
    except:
        send_message(chat_id, "❌ Invalid format. Send as: `name;price;stock`")
        return

    # Check if product exists
    existing = db.query(ProductORM).filter(ProductORM.name == name.strip()).first()
    if existing:
        send_message(chat_id, f"❌ Product '{name}' already exists.")
        return

    new_product = ProductORM(name=name.strip(), price=price, stock=stock)
    db.add(new_product)
    db.commit()
    db.refresh(new_product)

    send_message(chat_id, f"✅ Product added: {name} — ${price:.2f}, Stock: {stock}")


# ------------------- SALES -------------------

def record_sale(db: Session, chat_id: int, text: str, user_id: int = None):
    """
    Parse and record sale: 'product_name;quantity'
    """
    try:
        product_name, qty = text.split(";")
        qty = int(qty)
    except:
        send_message(chat_id, "❌ Invalid format. Send as: `product_name;quantity`")
        return

    product = db.query(ProductORM).filter(ProductORM.name == product_name.strip()).first()
    if not product:
        send_message(chat_id, f"❌ Product '{product_name}' not found.")
        return

    if product.stock < qty:
        send_message(chat_id, f"❌ Insufficient stock. Available: {product.stock}")
        return

    if user_id:
        user = db.query(User).filter(User.user_id == user_id).first()
    else:
        user = db.query(User).first()

    if not user:
        send_message(chat_id, "❌ No users available in the system.")
        return

    total_amount = Decimal(product.price) * qty
    sale = SaleORM(user_id=user.user_id, product_id=product.product_id,
                   quantity=qty, total_amount=total_amount)
    product.stock -= qty

    db.add(sale)
    db.commit()
    db.refresh(sale)

    send_message(chat_id, f"🛒 Sale recorded: {qty} × {product.name} = ${float(total_amount):.2f}")


# ------------------- REPORTS -------------------

def generate_report(db: Session, report_type: str):
    if report_type == "report_daily":
        results = db.query(
            func.date(SaleORM.sale_date).label("day"),
            func.sum(SaleORM.quantity).label("total_qty"),
            func.sum(SaleORM.total_amount).label("total_revenue")
        ).group_by(func.date(SaleORM.sale_date)).all()
        if not results:
            return "No sales data available."
        lines = ["📅 *Daily Sales*"]
        for r in results:
            lines.append(f"{r.day}: {r.total_qty} items, ${float(r.total_revenue):.2f}")
        return "\n".join(lines)

    elif report_type == "report_weekly":
        results = db.query(
            extract('week', SaleORM.sale_date).label("week"),
            func.sum(SaleORM.quantity).label("total_qty"),
            func.sum(SaleORM.total_amount).label("total_revenue")
        ).group_by("week").order_by("week").all()
        if not results:
            return "No sales data available."
        lines = ["📅 *Weekly Sales*"]
        for r in results:
            lines.append(f"Week {int(r.week)}: {r.total_qty} items, ${float(r.total_revenue):.2f}")
        return "\n".join(lines)

    elif report_type == "report_monthly":
        now = datetime.now()
        results = db.query(
            ProductORM.name.label("product"),
            func.sum(SaleORM.quantity).label("total_qty"),
            func.sum(SaleORM.total_amount).label("total_revenue")
        ).join(ProductORM, SaleORM.product_id == ProductORM.product_id)\
         .filter(extract("year", SaleORM.sale_date) == now.year)\
         .filter(extract("month", SaleORM.sale_date) == now.month)\
         .group_by(ProductORM.name).all()
        if not results:
            return "No sales data available."
        lines = ["📊 *Monthly Sales per Product*"]
        for r in results:
            lines.append(f"{r.product}: {r.total_qty} items, ${float(r.total_revenue):.2f}")
        return "\n".join(lines)

    else:
        return "❌ Unknown report type."


# ------------------- TELEGRAM WEBHOOK -------------------

@router.post("/telegram/webhook")
async def telegram_webhook(request: Request, db: Session = Depends(get_db)):
    data = await request.json()

    if "message" in data:
        chat_id = data["message"]["chat"]["id"]
        text = data["message"].get("text", "")

        if text.lower() in ["/start", "menu"]:
            main_menu(chat_id)
        else:
            parts = text.split(";")
            if len(parts) == 2:
                record_sale(db, chat_id, text)
            elif len(parts) == 3:
                add_product(db, chat_id, text)
            else:
                send_message(chat_id, "⚠️ Invalid format. Check menu instructions.")

    elif "callback_query" in data:
        chat_id = data["callback_query"]["message"]["chat"]["id"]
        action = data["callback_query"]["data"]

        if action == "add_product":
            send_message(chat_id, "➕ Send product details as: `name;price;stock`")
        elif action == "record_sale":
            send_message(chat_id, "🛒 Send sale as: `product_name;quantity`")
        elif action == "view_stock":
            stock_list = get_stock_list(db)
            send_message(chat_id, stock_list)
        elif action in ["reports", "report_daily", "report_weekly", "report_monthly"]:
            if action == "reports":
                keyboard = {
                    "inline_keyboard": [
                        [{"text": "📅 Daily", "callback_data": "report_daily"}],
                        [{"text": "📆 Weekly", "callback_data": "report_weekly"}],
                        [{"text": "📊 Monthly", "callback_data": "report_monthly"}],
                    ]
                }
                send_message(chat_id, "📊 Choose report type:", keyboard)
            else:
                report_text = generate_report(db, action)
                send_message(chat_id, report_text)

    return {"ok": True}
