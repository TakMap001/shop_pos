# app/telegram_notifications.py

from telebot import TeleBot, types
from sqlalchemy.orm import Session
from sqlalchemy import func
from app.models.models import User, ProductORM, SaleORM
from config import TELEGRAM_BOT_TOKEN

LOW_STOCK_THRESHOLD = 10
TOP_PRODUCT_THRESHOLD = 50
HIGH_VALUE_SALE_THRESHOLD = 100

bot = TeleBot(TELEGRAM_BOT_TOKEN)

# -------------------- Generic Message Sender --------------------
def send_message(user_id, text, keyboard=None):
    """
    Send Telegram message with optional inline keyboard (dict or InlineKeyboardMarkup).
    """
    try:
        markup = None

        # Case 1: Already a valid InlineKeyboardMarkup
        if isinstance(keyboard, types.InlineKeyboardMarkup):
            markup = keyboard

        # Case 2: Dict → Convert to InlineKeyboardMarkup
        elif isinstance(keyboard, dict) and "inline_keyboard" in keyboard:
            markup = types.InlineKeyboardMarkup()
            for row in keyboard["inline_keyboard"]:
                buttons = []
                for btn in row:
                    # Make sure both text and callback_data exist
                    text_val = btn.get("text")
                    cb_val = btn.get("callback_data")
                    if not text_val or not cb_val:
                        continue  # skip invalid buttons
                    buttons.append(types.InlineKeyboardButton(text=text_val, callback_data=cb_val))
                if buttons:
                    markup.add(*buttons)  # add row

        # Send with markup (if any)
        bot.send_message(user_id, text, reply_markup=markup, parse_mode="Markdown")

    except Exception as e:
        print("❌ Failed to send Telegram message:", e)
        print("➡️ Keyboard passed in:", keyboard)  # DEBUG what you are sending

# -------------------- Stock / Sales Notifications --------------------
def notify_low_stock(tenant_db: Session, product: ProductORM):
    """
    Send low-stock alert to owner(s) when product stock falls below threshold.
    """
    if product.stock <= product.low_stock_threshold:
        owners = tenant_db.query(User).filter(User.role == "owner").all()
        for owner in owners:
            send_message(
                owner.user_id,
                f"⚠️ Low Stock Alert!\n"
                f"📦 Product: {product.name}\n"
                f"📊 Current Stock: {product.stock}\n"
                f"⚠️ Threshold: {product.low_stock_threshold}"
            )

def notify_top_product(db: Session, product: ProductORM):
    """
    Notify owner when a product reaches top sales milestone.
    """
    total_sold = db.query(func.sum(SaleORM.quantity)).filter(SaleORM.product_id == product.product_id).scalar() or 0
    if total_sold >= TOP_PRODUCT_THRESHOLD:
        owners = db.query(User).filter(User.role == "owner").all()
        for owner in owners:
            send_message(owner.user_id, f"🏆 Milestone! '{product.name}' sold {total_sold} units!")

def notify_high_value_sale(db: Session, sale: SaleORM):
    """
    Notify owner about a high-value sale.
    """
    if sale.total_amount >= HIGH_VALUE_SALE_THRESHOLD:
        owners = db.query(User).filter(User.role == "owner").all()
        for owner in owners:
            send_message(
                owner.user_id,
                f"💰 High-value Sale Alert:\n"
                f"{sale.quantity} × {sale.product.name} = ${sale.total_amount}"
            )

def send_daily_sales_summary(db: Session):
    """
    Send daily sales summary to owner(s).
    """
    from datetime import date
    today = date.today()
    results = db.query(
        func.sum(SaleORM.quantity).label("total_qty"),
        func.sum(SaleORM.total_amount).label("total_revenue")
    ).filter(func.date(SaleORM.sale_date) == today).first()

    owners = db.query(User).filter(User.role == "owner").all()
    summary = (
        f"📊 Daily Sales Summary ({today}):\n"
        f"🛒 Items Sold: {results.total_qty or 0}\n"
        f"💵 Revenue: ${results.total_revenue or 0}"
    )
    for owner in owners:
        send_message(owner.user_id, summary)

# -------------------- Shopkeeper → Owner Notifications --------------------
def notify_owner_of_new_product(shopkeeper_chat_id: int, product_data: dict, tenant_db: Session):
    """
    Notify owner when a shopkeeper adds a new product (awaiting approval).
    """
    owner = tenant_db.query(User).filter(User.role == "owner").first()
    if owner:
        send_message(
            owner.user_id,
            f"📢 New Product Awaiting Approval\n"
            f"👤 Added by Shopkeeper (ID {shopkeeper_chat_id}):\n"
            f"• Name: {product_data.get('name')}\n"
            f"• Quantity: {product_data.get('quantity')}\n"
            f"• Unit Type: {product_data.get('unit_type')}\n"
            f"• Price: {product_data.get('price', 'N/A')}\n\n"
            f"✅ Please review and approve."
        )

def notify_owner_of_product_update(shopkeeper_chat_id: int, product: ProductORM, updated_fields: list, tenant_db: Session):
    """
    Notify owner when a shopkeeper updates product details (limited: quantity, unit type).
    """
    owner = tenant_db.query(User).filter(User.role == "owner").first()
    if owner:
        updates_text = "\n".join([f"• {field}: {getattr(product, field)}" for field in updated_fields])
        send_message(
            owner.user_id,
            f"📢 Product Updated by Shopkeeper (ID {shopkeeper_chat_id}):\n"
            f"{updates_text}\n"
            f"✅ Please review changes."
        )

# -------------------- Shopkeeper Account Notifications --------------------
def notify_owner_of_new_shopkeeper(shopkeeper: User, tenant_db: Session):
    """
    Notify the owner when a new shopkeeper account is created.
    """
    owner = tenant_db.query(User).filter(User.role == "owner").first()
    if owner:
        send_message(
            owner.user_id,
            f"👤 *New Shopkeeper Added*\n"
            f"• Name: {shopkeeper.name}\n"
            f"• Username: {shopkeeper.username}\n"
            f"• Role: {shopkeeper.role}\n\n"
            f"✅ Please review and confirm access."
        )
