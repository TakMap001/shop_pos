# app/telegram_notifications.py

# TEMPORARY DEBUG - Add at the VERY TOP of telegram_notifications.py
import os
print("🟢 DEBUG: telegram_notifications.py is loading")
print(f"🟢 DEBUG: TELEGRAM_BOT_TOKEN from os.getenv: {os.getenv('TELEGRAM_BOT_TOKEN')}")

from config import TELEGRAM_BOT_TOKEN
print(f"🟢 DEBUG: TELEGRAM_BOT_TOKEN from config import: {TELEGRAM_BOT_TOKEN}")

# Then your existing code...
from telebot import TeleBot, types
from sqlalchemy.orm import Session
from sqlalchemy import func
from app.models.models import User, ProductORM, SaleORM
import re

LOW_STOCK_THRESHOLD = 10
TOP_PRODUCT_THRESHOLD = 50
HIGH_VALUE_SALE_THRESHOLD = 100

bot = TeleBot(TELEGRAM_BOT_TOKEN)

def escape_markdown_v2(text: str) -> str:
    """
    Safely escape text for Telegram MarkdownV2.
    """
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text or '')

# -------------------- Generic Message Sender --------------------
def send_message(user_id, text, keyboard=None):
    """
    Send Telegram message with optional inline keyboard (dict or InlineKeyboardMarkup).
    Escapes text safely for MarkdownV2.
    """
    try:
        markup = None

        # Escape the text for MarkdownV2
        safe_text = escape_markdown_v2(text)

        # Case 1: Already a valid InlineKeyboardMarkup
        if isinstance(keyboard, types.InlineKeyboardMarkup):
            markup = keyboard

        # Case 2: Dict → Convert to InlineKeyboardMarkup
        elif isinstance(keyboard, dict) and "inline_keyboard" in keyboard:
            markup = types.InlineKeyboardMarkup()
            for row in keyboard["inline_keyboard"]:
                buttons = []
                for btn in row:
                    text_val = btn.get("text")
                    cb_val = btn.get("callback_data")
                    if not text_val or not cb_val:
                        continue  # skip invalid buttons
                    # Escape button text as well
                    safe_btn_text = escape_markdown_v2(str(text_val))
                    buttons.append(
                        types.InlineKeyboardButton(text=safe_btn_text, callback_data=cb_val)
                    )
                if buttons:
                    markup.add(*buttons)

        # Send safely using MarkdownV2
        bot.send_message(user_id, safe_text, reply_markup=markup, parse_mode="MarkdownV2")

    except Exception as e:
        print("❌ Failed to send Telegram message:", e)
        print("➡️ Keyboard passed in:", keyboard)
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

def notify_owner_of_pending_approval(owner_chat_id: int, action_type: str, product_name: str, shopkeeper_name: str, approval_id: int):
    """
    Notify owner about pending approval with action buttons.
    """
    try:
        # Create the notification message
        message = f"🔄 *Pending Approval Required*\n\n"
        message += f"👤 *Shopkeeper:* {escape_markdown_v2(shopkeeper_name)}\n"
        message += f"📦 *Action:* {escape_markdown_v2(action_type.replace('_', ' ').title())}\n"
        message += f"🛒 *Product:* {escape_markdown_v2(product_name)}\n\n"
        message += f"⚠️ *Urgent action required\\!* Please approve or reject this request\\."

        # Create approval buttons
        keyboard = {
            "inline_keyboard": [
                [
                    {"text": "✅ Approve", "callback_data": f"approve_action:{approval_id}"},
                    {"text": "❌ Reject", "callback_data": f"reject_action:{approval_id}"}
                ],
                [{"text": "📋 View Details", "callback_data": f"view_approval:{approval_id}"}]
            ]
        }

        send_message(owner_chat_id, message, keyboard)
        return True

    except Exception as e:
        print(f"❌ Failed to send approval notification: {e}")
        return False

def notify_shopkeeper_of_approval_result(shopkeeper_chat_id: int, product_name: str, action: str, approved: bool):
    """
    Notify shopkeeper about approval result.
    """
    try:
        if approved:
            message = f"✅ *Approval Granted*\n\n"
            message += f"Your product *{escape_markdown_v2(product_name)}* has been approved and {action} to inventory\\!"
            message += f"\n\nYou can now view it in stock and sell it\\."
        else:
            message = f"❌ *Approval Denied*\n\n"
            message += f"Your request for *{escape_markdown_v2(product_name)}* has been rejected\\."
            message += f"\n\nPlease contact the owner for more details\\."

        send_message(shopkeeper_chat_id, message)
        return True

    except Exception as e:
        print(f"❌ Failed to send approval result notification: {e}")
        return False

def notify_owner_of_stock_update_request(shopkeeper_chat_id: int, product_name: str, old_stock: int, new_stock: int, shopkeeper_name: str, approval_id: int):
    """
    Notify owner about stock update request from shopkeeper.
    """
    try:
        message = f"📈 *Stock Update Request*\n\n"
        message += f"👤 *Shopkeeper:* {escape_markdown_v2(shopkeeper_name)}\n"
        message += f"🛒 *Product:* {escape_markdown_v2(product_name)}\n"
        message += f"📊 *Current Stock:* {old_stock}\n"
        message += f"🆕 *Requested Stock:* {new_stock}\n"
        message += f"📈 *Change:* \\+{new_stock - old_stock}\n\n"
        message += f"⚠️ *Approval required\\!*"

        keyboard = {
            "inline_keyboard": [
                [
                    {"text": "✅ Approve", "callback_data": f"approve_stock:{approval_id}"},
                    {"text": "❌ Reject", "callback_data": f"reject_stock:{approval_id}"}
                ]
            ]
        }

        send_message(shopkeeper_chat_id, message, keyboard)
        return True

    except Exception as e:
        print(f"❌ Failed to send stock update notification: {e}")
        return False