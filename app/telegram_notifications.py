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
from app.models.central_models import User
from app.models.models import ProductORM, CustomerORM, SaleORM, PendingApprovalORM, ShopORM, ProductShopStockORM

import re

LOW_STOCK_THRESHOLD = 10
TOP_PRODUCT_THRESHOLD = 50
HIGH_VALUE_SALE_THRESHOLD = 100

# DEBUG: Bot initialization
print(f"🟢 [telegram_notifications] Initializing bot...")
print(f"🟢 [telegram_notifications] TELEGRAM_BOT_TOKEN length: {len(TELEGRAM_BOT_TOKEN)}")
print(f"🟢 [telegram_notifications] TELEGRAM_BOT_TOKEN first 10 chars: {TELEGRAM_BOT_TOKEN[:10]}...")

bot = TeleBot(TELEGRAM_BOT_TOKEN)
print(f"🟢 [telegram_notifications] Bot initialized: {bot}")

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
    print(f"🟢 [send_message] START: user_id={user_id}, text={text[:50]}...")
    
    try:
        markup = None

        # Escape the text for MarkdownV2
        safe_text = escape_markdown_v2(text)
        print(f"🟢 [send_message] Text escaped: {safe_text[:50]}...")

        # Case 1: Already a valid InlineKeyboardMarkup
        if isinstance(keyboard, types.InlineKeyboardMarkup):
            markup = keyboard
            print(f"🟢 [send_message] Using InlineKeyboardMarkup")

        # Case 2: Dict → Convert to InlineKeyboardMarkup
        elif isinstance(keyboard, dict) and "inline_keyboard" in keyboard:
            markup = types.InlineKeyboardMarkup()
            print(f"🟢 [send_message] Converting dict to keyboard")
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
            print(f"🟢 [send_message] Keyboard created with {len(keyboard['inline_keyboard'])} rows")

        # Send safely using MarkdownV2
        print(f"🟢 [send_message] Calling bot.send_message...")
        result = bot.send_message(user_id, safe_text, reply_markup=markup, parse_mode="MarkdownV2")
        print(f"✅ [send_message] SUCCESS: Message sent to {user_id}, message_id: {result.message_id}")
        return True

    except Exception as e:
        print(f"❌ [send_message] ERROR: {e}")
        print("➡️ Keyboard passed in:", keyboard)
        import traceback
        traceback.print_exc()
        return False

# ==================== SHOP-SPECIFIC NOTIFICATIONS ====================

def notify_low_stock(tenant_db: Session, product: ProductORM, shop_id: int = None):
    """
    Send low-stock alert to owner(s) and shop admin when product stock falls below threshold.
    """
    # Get stock for specific shop if shop_id provided
    if shop_id:
        stock_record = tenant_db.query(ProductShopStockORM).filter(
            ProductShopStockORM.product_id == product.product_id,
            ProductShopStockORM.shop_id == shop_id
        ).first()
        
        if not stock_record or stock_record.stock > stock_record.low_stock_threshold:
            return
            
        current_stock = stock_record.stock
        threshold = stock_record.low_stock_threshold
        shop_name = tenant_db.query(ShopORM.name).filter(ShopORM.shop_id == shop_id).scalar() or f"Shop {shop_id}"
    else:
        # Fallback to product stock (global)
        if product.stock <= product.low_stock_threshold:
            current_stock = product.stock
            threshold = product.low_stock_threshold
            shop_name = "Global"
        else:
            return
    
    # Get recipients
    recipients = []
    
    # 1. Owner(s) - all owners in this tenant
    owners = tenant_db.query(User).filter(User.role == "owner").all()
    recipients.extend(owners)
    
    # 2. Shop admin for this specific shop
    if shop_id:
        shop_admin = tenant_db.query(User).filter(
            User.role == "admin",
            User.shop_id == shop_id
        ).first()
        if shop_admin:
            recipients.append(shop_admin)
    
    # Send notifications
    for recipient in recipients:
        if shop_id:
            message = (
                f"⚠️ *Low Stock Alert!*\n\n"
                f"🏪 *Shop:* {shop_name}\n"
                f"📦 *Product:* {product.name}\n"
                f"📊 *Current Stock:* {current_stock}\n"
                f"⚠️ *Threshold:* {threshold}"
            )
        else:
            message = (
                f"⚠️ *Global Low Stock Alert!*\n\n"
                f"📦 *Product:* {product.name}\n"
                f"📊 *Current Stock:* {current_stock}\n"
                f"⚠️ *Threshold:* {threshold}"
            )
        
        send_message(recipient.chat_id, message)

def notify_top_product(db: Session, product: ProductORM, shop_id: int = None):
    """
    Notify owner and shop admin when a product reaches top sales milestone.
    """
    # Filter sales by shop if shop_id provided
    if shop_id:
        total_sold = db.query(func.sum(SaleORM.quantity)).filter(
            SaleORM.product_id == product.product_id,
            SaleORM.shop_id == shop_id
        ).scalar() or 0
        shop_name = db.query(ShopORM.name).filter(ShopORM.shop_id == shop_id).scalar() or f"Shop {shop_id}"
    else:
        total_sold = db.query(func.sum(SaleORM.quantity)).filter(
            SaleORM.product_id == product.product_id
        ).scalar() or 0
        shop_name = "All Shops"
    
    if total_sold >= TOP_PRODUCT_THRESHOLD:
        # Get recipients
        recipients = []
        
        # 1. Owner(s)
        owners = db.query(User).filter(User.role == "owner").all()
        recipients.extend(owners)
        
        # 2. Shop admin for this specific shop
        if shop_id:
            shop_admin = db.query(User).filter(
                User.role == "admin",
                User.shop_id == shop_id
            ).first()
            if shop_admin:
                recipients.append(shop_admin)
        
        # Send notifications
        for recipient in recipients:
            message = (
                f"🏆 *Milestone Achievement!*\n\n"
                f"🏪 *Shop:* {shop_name}\n"
                f"📦 *Product:* {product.name}\n"
                f"🛒 *Total Sold:* {total_sold} units!"
            )
            send_message(recipient.chat_id, message)

def notify_high_value_sale(db: Session, sale: SaleORM):
    """
    Notify owner and shop admin about a high-value sale.
    """
    if sale.total_amount >= HIGH_VALUE_SALE_THRESHOLD:
        # Get shop info
        shop = db.query(ShopORM).filter(ShopORM.shop_id == sale.shop_id).first()
        shop_name = shop.name if shop else f"Shop {sale.shop_id}"
        
        # Get recipients
        recipients = []
        
        # 1. Owner(s)
        owners = db.query(User).filter(User.role == "owner").all()
        recipients.extend(owners)
        
        # 2. Shop admin for this specific shop
        shop_admin = db.query(User).filter(
            User.role == "admin",
            User.shop_id == sale.shop_id
        ).first()
        if shop_admin:
            recipients.append(shop_admin)
        
        # Send notifications
        for recipient in recipients:
            message = (
                f"💰 *High-value Sale Alert!*\n\n"
                f"🏪 *Shop:* {shop_name}\n"
                f"📦 *Product:* {sale.product.name}\n"
                f"📊 *Quantity:* {sale.quantity}\n"
                f"💵 *Total Amount:* ${sale.total_amount}"
            )
            send_message(recipient.chat_id, message)

def send_daily_sales_summary(db: Session, shop_id: int = None):
    """
    Send daily sales summary to owner(s) and shop admins.
    """
    from datetime import date, datetime
    today = date.today()
    
    # Build query based on shop_id
    query = db.query(
        func.sum(SaleORM.quantity).label("total_qty"),
        func.sum(SaleORM.total_amount).label("total_revenue"),
        func.count(SaleORM.sale_id).label("total_sales")
    ).filter(func.date(SaleORM.sale_date) == today)
    
    if shop_id:
        query = query.filter(SaleORM.shop_id == shop_id)
        shop = db.query(ShopORM).filter(ShopORM.shop_id == shop_id).first()
        shop_name = shop.name if shop else f"Shop {shop_id}"
        scope = f"Shop: {shop_name}"
    else:
        scope = "All Shops"
    
    results = query.first()
    
    # Get recipients
    recipients = []
    
    if shop_id:
        # For specific shop: send to owner and shop admin
        owners = db.query(User).filter(User.role == "owner").all()
        recipients.extend(owners)
        
        shop_admin = db.query(User).filter(
            User.role == "admin",
            User.shop_id == shop_id
        ).first()
        if shop_admin:
            recipients.append(shop_admin)
    else:
        # For all shops: send only to owner(s)
        owners = db.query(User).filter(User.role == "owner").all()
        recipients.extend(owners)
    
    # Prepare summary
    summary = (
        f"📊 *Daily Sales Summary*\n"
        f"📅 *Date:* {today}\n"
        f"🏪 *Scope:* {scope}\n"
        f"🛒 *Transactions:* {results.total_sales or 0}\n"
        f"📦 *Items Sold:* {results.total_qty or 0}\n"
        f"💵 *Revenue:* ${results.total_revenue or 0:.2f}"
    )
    
    # Send notifications
    for recipient in recipients:
        send_message(recipient.chat_id, summary)

# -------------------- Shopkeeper → Owner Notifications --------------------

def notify_owner_of_new_product(shopkeeper_chat_id: int, product_data: dict, tenant_db: Session, shop_id: int):
    """
    Notify owner when a shopkeeper adds a new product (awaiting approval).
    """
    # Get shop info
    shop = tenant_db.query(ShopORM).filter(ShopORM.shop_id == shop_id).first()
    shop_name = shop.name if shop else f"Shop {shop_id}"
    
    # Get shopkeeper info
    shopkeeper = tenant_db.query(User).filter(User.chat_id == shopkeeper_chat_id).first()
    shopkeeper_name = shopkeeper.name if shopkeeper else f"Shopkeeper {shopkeeper_chat_id}"
    
    # Get owner(s) for this tenant
    owners = tenant_db.query(User).filter(User.role == "owner").all()
    
    for owner in owners:
        message = (
            f"📢 *New Product Awaiting Approval*\n\n"
            f"🏪 *Shop:* {shop_name}\n"
            f"👤 *Added by:* {shopkeeper_name}\n"
            f"📦 *Product:* {product_data.get('name')}\n"
            f"📊 *Quantity:* {product_data.get('quantity')}\n"
            f"📏 *Unit Type:* {product_data.get('unit_type')}"
        )
        
        if 'price' in product_data and product_data['price']:
            message += f"\n💰 *Price:* ${product_data.get('price')}"
        
        message += "\n\n⚠️ *Urgent action required!* Please review and approve."
        
        send_message(owner.chat_id, message)

def notify_owner_of_product_update(shopkeeper_chat_id: int, product: ProductORM, updated_fields: list, tenant_db: Session, shop_id: int):
    """
    Notify owner when a shopkeeper updates product details (limited: quantity, unit type).
    """
    # Get shop info
    shop = tenant_db.query(ShopORM).filter(ShopORM.shop_id == shop_id).first()
    shop_name = shop.name if shop else f"Shop {shop_id}"
    
    # Get shopkeeper info
    shopkeeper = tenant_db.query(User).filter(User.chat_id == shopkeeper_chat_id).first()
    shopkeeper_name = shopkeeper.name if shopkeeper else f"Shopkeeper {shopkeeper_chat_id}"
    
    # Get owner(s)
    owners = tenant_db.query(User).filter(User.role == "owner").all()
    
    updates_text = "\n".join([f"• *{field}:* {getattr(product, field)}" for field in updated_fields])
    
    for owner in owners:
        message = (
            f"📢 *Product Update Awaiting Approval*\n\n"
            f"🏪 *Shop:* {shop_name}\n"
            f"👤 *Updated by:* {shopkeeper_name}\n"
            f"📦 *Product:* {product.name}\n\n"
            f"*Changes:*\n{updates_text}\n\n"
            f"⚠️ *Please review changes.*"
        )
        
        send_message(owner.chat_id, message)

# -------------------- Account Management Notifications --------------------

def notify_owner_of_new_shop_user(user: User, tenant_db: Session):
    """
    Notify the owner when a new shop user (admin/shopkeeper) is created.
    """
    # Get shop info
    shop = tenant_db.query(ShopORM).filter(ShopORM.shop_id == user.shop_id).first()
    shop_name = shop.name if shop else f"Shop {user.shop_id}"
    
    # Get owner(s)
    owners = tenant_db.query(User).filter(User.role == "owner").all()
    
    role_icon = "🛡️" if user.role == "admin" else "👨‍💼"
    
    for owner in owners:
        message = (
            f"{role_icon} *New Shop User Added*\n\n"
            f"🏪 *Shop:* {shop_name}\n"
            f"👤 *Name:* {user.name}\n"
            f"📛 *Username:* `{user.username}`\n"
            f"🎭 *Role:* {user.role.title()}\n\n"
            f"✅ User has been created successfully."
        )
        
        send_message(owner.chat_id, message)

def notify_owner_of_pending_approval(owner_chat_id: int, action_type: str, product_name: str, shopkeeper_name: str, approval_id: int, shop_id: int = None):
    """
    Notify owner about pending approval with action buttons.
    """
    try:
        # Get shop info if shop_id provided
        shop_info = ""
        if shop_id:
            from app.core import SessionLocal
            tenant_db = SessionLocal()
            shop = tenant_db.query(ShopORM).filter(ShopORM.shop_id == shop_id).first()
            if shop:
                shop_info = f"\n🏪 *Shop:* {shop.name}"
            tenant_db.close()
        
        # Create the notification message
        message = f"🔄 *Pending Approval Required*\n\n"
        message += f"👤 *Shopkeeper:* {escape_markdown_v2(shopkeeper_name)}"
        message += shop_info
        message += f"\n📦 *Action:* {escape_markdown_v2(action_type.replace('_', ' ').title())}"
        message += f"\n🛒 *Product:* {escape_markdown_v2(product_name)}"
        message += f"\n\n⚠️ *Urgent action required!* Please approve or reject this request."

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

def notify_shopkeeper_of_approval_result(shopkeeper_chat_id: int, product_name: str, action: str, approved: bool, shop_id: int = None):
    """
    Notify shopkeeper about approval result.
    """
    try:
        # Get shop info if shop_id provided
        shop_info = ""
        if shop_id:
            from app.core import SessionLocal
            tenant_db = SessionLocal()
            shop = tenant_db.query(ShopORM).filter(ShopORM.shop_id == shop_id).first()
            if shop:
                shop_info = f"\n🏪 *Shop:* {shop.name}"
            tenant_db.close()
        
        if approved:
            message = f"✅ *Approval Granted*\n\n"
            message += f"Your product *{escape_markdown_v2(product_name)}* has been approved and {action} to inventory!"
            message += shop_info
            message += f"\n\nYou can now view it in stock and sell it."
        else:
            message = f"❌ *Approval Denied*\n\n"
            message += f"Your request for *{escape_markdown_v2(product_name)}* has been rejected."
            message += shop_info
            message += f"\n\nPlease contact the owner for more details."

        send_message(shopkeeper_chat_id, message)
        return True

    except Exception as e:
        print(f"❌ Failed to send approval result notification: {e}")
        return False

def notify_owner_of_stock_update_request(shopkeeper_chat_id: int, product_name: str, old_stock: int, new_stock: int, shopkeeper_name: str, approval_id: int, shop_id: int):
    """
    Notify owner about stock update request from shopkeeper.
    """
    try:
        # Get shop info
        from app.core import SessionLocal
        tenant_db = SessionLocal()
        shop = tenant_db.query(ShopORM).filter(ShopORM.shop_id == shop_id).first()
        shop_name = shop.name if shop else f"Shop {shop_id}"
        tenant_db.close()
        
        message = f"📈 *Stock Update Request*\n\n"
        message += f"🏪 *Shop:* {shop_name}\n"
        message += f"👤 *Shopkeeper:* {escape_markdown_v2(shopkeeper_name)}\n"
        message += f"🛒 *Product:* {escape_markdown_v2(product_name)}\n"
        message += f"📊 *Current Stock:* {old_stock}\n"
        message += f"🆕 *Requested Stock:* {new_stock}\n"
        message += f"📈 *Change:* +{new_stock - old_stock}\n\n"
        message += f"⚠️ *Approval required!*"

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

# ==================== SHOP-SPECIFIC USER NOTIFICATIONS ====================

def send_new_user_credentials(owner_chat_id: int, user_type: str, username: str, password: str, email: str, shop_name: str):
    """
    Send new user credentials to owner with shop context.
    """
    role_icon = "🛡️" if user_type == "admin" else "👨‍💼"
    role_name = "Admin" if user_type == "admin" else "Shopkeeper"
    
    message = (
        f"{role_icon} *New {role_name} User Created*\n\n"
        f"🏪 *Shop:* {shop_name}\n"
        f"📛 *Username:* `{username}`\n"
        f"🔑 *Password:* `{password}`\n"
        f"📧 *Email:* {email}\n\n"
        f"📝 *Instructions:*\n"
        f"1. Share these credentials with the {role_name.lower()}\n"
        f"2. They use /start in Telegram\n"
        f"3. Select 'I'm a Shop User'\n"
        f"4. Enter username and password\n\n"
        f"⚠️ *Save this information!*"
    )
    
    send_message(owner_chat_id, message)

def notify_user_assigned_to_shop(user_chat_id: int, shop_name: str, role: str):
    """
    Notify user when they are assigned to a shop.
    """
    role_display = "Admin" if role == "admin" else "Shopkeeper"
    
    message = (
        f"🏪 *Shop Assignment*\n\n"
        f"✅ You have been assigned to:\n"
        f"🏪 *Shop:* {shop_name}\n"
        f"👤 *Role:* {role_display}\n\n"
        f"You can now access this shop's inventory and sales."
    )
    
    send_message(user_chat_id, message)
    
def notify_owner_of_new_shopkeeper(owner_chat_id: int, shopkeeper_username: str, shop_name: str, created_by: str = None):
    """
    Notify owner when a new shopkeeper is created (by admin).
    """
    try:
        message = f"👤 *New Shopkeeper Added*\n\n"
        message += f"🏪 *Shop:* {escape_markdown_v2(shop_name)}\n"
        message += f"📛 *Username:* `{escape_markdown_v2(shopkeeper_username)}`\n"
        
        if created_by:
            message += f"👨‍💼 *Created by:* {escape_markdown_v2(created_by)}\n"
        
        message += f"\n✅ Shopkeeper account has been created successfully.\n"
        message += f"Share credentials with the shopkeeper to link their Telegram."

        send_message(owner_chat_id, message)
        return True

    except Exception as e:
        print(f"❌ Failed to send new shopkeeper notification: {e}")
        return False
    