# app/telegram_notifications.py

from sqlalchemy.orm import Session
from sqlalchemy import func
from app.models.models import User, ProductORM, SaleORM
import telebot
from app.config import TELEGRAM_BOT_TOKEN

LOW_STOCK_THRESHOLD = 10
TOP_PRODUCT_THRESHOLD = 50
HIGH_VALUE_SALE_THRESHOLD = 100

bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN)

def send_message(user_id: int, text: str):
    bot.send_message(user_id, text)

def notify_low_stock(db: Session, product: ProductORM):
    if product.stock <= LOW_STOCK_THRESHOLD:
        owners = db.query(User).filter(User.role == "owner").all()
        for owner in owners:
            send_message(owner.user_id, f"⚠️ Low Stock Alert: '{product.name}' has only {product.stock} units left!")

def notify_top_product(db: Session, product: ProductORM):
    total_sold = db.query(func.sum(SaleORM.quantity)).filter(SaleORM.product_id == product.product_id).scalar() or 0
    if total_sold >= TOP_PRODUCT_THRESHOLD:
        owners = db.query(User).filter(User.role == "owner").all()
        for owner in owners:
            send_message(owner.user_id, f"🏆 Milestone! '{product.name}' sold {total_sold} units!")

def notify_high_value_sale(db: Session, sale: SaleORM):
    if sale.total_amount >= HIGH_VALUE_SALE_THRESHOLD:
        owners = db.query(User).filter(User.role == "owner").all()
        for owner in owners:
            send_message(owner.user_id, f"💰 High-value Sale Alert: {sale.quantity} × {sale.product.name} = ${sale.total_amount}")

def send_daily_sales_summary(db: Session):
    from datetime import date
    today = date.today()
    results = db.query(
        func.sum(SaleORM.quantity).label("total_qty"),
        func.sum(SaleORM.total_amount).label("total_revenue")
    ).filter(func.date(SaleORM.sale_date) == today).first()

    owners = db.query(User).filter(User.role == "owner").all()
    summary = f"📊 Daily Sales Summary ({today}):\nItems Sold: {results.total_qty or 0}\nRevenue: ${results.total_revenue or 0}"
    for owner in owners:
        send_message(owner.user_id, summary)

