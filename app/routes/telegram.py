# app/routes/telegram.py

import json 
from fastapi import APIRouter, Request, Depends
import requests, os
from sqlalchemy.orm import Session
from decimal import Decimal
from datetime import datetime, timedelta
from sqlalchemy import func, text, extract
from app.models.central_models import Tenant, User  # ✅ ADD User here
from app.models.models import TenantBase  # ✅ FIXED: Remove "Base as User"
from app.models.models import ProductORM, CustomerORM, SaleORM, PendingApprovalORM, ShopORM, ProductShopStockORM  # Tenant DB
from app.database import get_db  # central DB session - KEEP THIS ONE
from app.telegram_notifications import notify_low_stock, notify_top_product, notify_high_value_sale, send_message, notify_owner_of_pending_approval
from app.telegram_notifications import notify_shopkeeper_of_approval_result
from config import DATABASE_URL
from telebot import types
from app.telegram_notifications import notify_owner_of_new_shopkeeper
from config import TELEGRAM_BOT_TOKEN, TELEGRAM_API_URL
from app.tenant_db import get_tenant_session, create_tenant_db, ensure_tenant_tables, ensure_tenant_session
import random
import string
import bcrypt
import time
from app.core import SessionLocal  # ✅ REMOVE duplicate get_db
from sqlalchemy.exc import SQLAlchemyError
import uuid
import logging
from telegram.helpers import escape_markdown
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
import re
import html
import traceback

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

router = APIRouter()

# Tracks multi-step actions per user
user_states = {}  # chat_id -> {"action": "awaiting_shop_name" / "awaiting_product" / "awaiting_update" / "awaiting_sale"}

# Ensure the token is set
if not TELEGRAM_BOT_TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN is not set in environment or .env file")


# -------------------- Helpers --------------------

def escape_markdown_v2(text: str) -> str:
    """
    Safely escape text for Telegram MarkdownV2.
    """
    escape_chars = r'_*[]()~`>#+-=|{}.!'
    return re.sub(f'([{re.escape(escape_chars)}])', r'\\\1', text or '')

def create_username(full_name: str) -> str:
    """Generate a simple username from full name."""
    base = "".join(full_name.lower().split())  # remove spaces
    suffix = str(random.randint(100, 999))
    return f"{base}{suffix}"

def generate_password(length: int = 10) -> str:
    """Generate a secure random password."""
    chars = string.ascii_letters + string.digits + "!@#$%^&*()"
    return "".join(random.choice(chars) for _ in range(length))

def hash_password(password: str) -> str:
    """Hash password using bcrypt."""
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(password.encode("utf-8"), salt)
    return hashed.decode("utf-8")

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a plain password against a hashed password."""
    return bcrypt.checkpw(plain_password.encode("utf-8"), hashed_password.encode("utf-8"))


def get_user(chat_id: int, db: Session):
    return db.query(User).filter(User.user_id == chat_id).first()

def send_owner_credentials(chat_id, username, password):
    send_message(
        chat_id,
        f"✅ Welcome! Your Owner credentials:\n\n"
        f"🆔 Username: {username}\n"
        f"🔑 Password: {password}"
    )

def get_user_by_chat(chat_id: int):
    """
    Return the central User row matching the Telegram chat_id.
    """
    if not chat_id:
        return None
    db = next(get_db())  # get a central DB session
    return db.query(User).filter(User.chat_id == chat_id).first()

def create_shopkeeper(tenant_session, username, password):
    from utils.security import hash_password
    new_user = User(
        username=username,
        password_hash=hash_password(password),
        role="shopkeeper",
        chat_id=None  # intentionally blank until first login
    )
    tenant_session.add(new_user)
    tenant_session.commit()
    return new_user

def role_menu(chat_id):
    """Role selection menu (Owner vs Shopkeeper)."""
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(
        types.InlineKeyboardButton("👑 Owner", callback_data="role_owner"),
        types.InlineKeyboardButton("🛍 Shopkeeper", callback_data="role_keeper")
    )
    send_message(chat_id, "👋 Welcome! Please choose your role:", keyboard)

def main_menu(role: str):
    if role == "owner":
        kb_dict = {
            "inline_keyboard": [
                [{"text": "➕ Add Product", "callback_data": "add_product"}],
                [{"text": "✏️ Update Product", "callback_data": "update_product"}],
                [{"text": "📈 Quick Stock Update", "callback_data": "quick_stock_update"}],
                [{"text": "📦 View Stock", "callback_data": "view_stock"}],
                [{"text": "💰 Record Sale", "callback_data": "record_sale"}],
                [{"text": "📊 Reports", "callback_data": "report_menu"}],
                [{"text": "🏪 Manage Shops", "callback_data": "manage_shops"}],  # ✅ CHANGED from "setup_shop" to "manage_shops"
                [{"text": "👤 Create Shopkeeper", "callback_data": "create_shopkeeper"}],
                [{"text": "❓ Help", "callback_data": "help"}]
            ]
        }
    elif role == "shopkeeper":
        kb_dict = {
            "inline_keyboard": [
                [{"text": "➕ Add Product", "callback_data": "add_product"}],
                [{"text": "✏️ Update Product", "callback_data": "update_product"}],
                [{"text": "📈 Quick Stock Update", "callback_data": "quick_stock_update"}],
                [{"text": "📦 View Stock", "callback_data": "view_stock"}],
                [{"text": "💰 Record Sale", "callback_data": "record_sale"}],
                [{"text": "📊 Reports", "callback_data": "report_menu"}],
                [{"text": "❓ Help", "callback_data": "help"}]
            ]
        }
    else:
        kb_dict = {"inline_keyboard": []}

    return kb_dict
    
            
def build_keyboard(kb_dict):
    """Convert our menu dict into a Telebot InlineKeyboardMarkup."""
    keyboard = types.InlineKeyboardMarkup()
    for row in kb_dict["inline_keyboard"]:
        buttons = [
            types.InlineKeyboardButton(text=b["text"], callback_data=b["callback_data"])
            for b in row
        ]
        keyboard.add(*buttons)   # ✅ use add() instead of row()
    return keyboard

def products_page_view(tenant_db, page: int = 1, per_page: int = 5):
    """
    Returns (text, kb_dict) showing products for `tenant_db` for the given page.
    Buttons:
      - Each product has a button labeled: "ID {id}: {name}"
        callback_data -> "select_product:{product_id}"
      - Navigation row with Back / Next where applicable:
        callback_data -> "products_page:{page}"
      - Always include "⬅️ Back to Menu" button
    """
    if not tenant_db:
        return "❌ No tenant DB connected.", {"inline_keyboard": [[{"text": "⬅️ Back to Menu", "callback_data": "back_to_menu"}]]}

    # total count
    total = tenant_db.query(func.count(ProductORM.product_id)).scalar() or 0
    total_pages = max(1, -(-total // per_page))  # ceil division

    page = max(1, int(page))
    if page > total_pages:
        page = total_pages

    offset = (page - 1) * per_page
    products = (
        tenant_db.query(ProductORM)
        .order_by(ProductORM.product_id)
        .offset(offset)
        .limit(per_page)
        .all()
    )

    if not products:
        text = "📦 No products found."
        kb = {"inline_keyboard": [[{"text": "⬅️ Back to Menu", "callback_data": "back_to_menu"}]]}
        return text, kb

    # Prepare textual listing with clear IDs
    lines = [f"📦 *Products — Page {page}/{total_pages}*"]
    for p in products:
        # Ensure price cast to float for printing
        price = float(p.price) if p.price is not None else 0.0
        lines.append(f"ID {p.product_id}: {p.name} — ${price:.2f} — Stock: {p.stock}")

    text = "\n".join(lines)

    # Build keyboard: one button per product (compact label) + nav row + back to menu
    kb_rows = []
    for p in products:
        label = f"ID {p.product_id}: {p.name}"
        kb_rows.append([{"text": label, "callback_data": f"select_product:{p.product_id}"}])

    nav_row = []
    if page > 1:
        nav_row.append({"text": "⬅️ Back", "callback_data": f"products_page:{page-1}"})
    if page < total_pages:
        nav_row.append({"text": "Next ➡️", "callback_data": f"products_page:{page+1}"})
    if nav_row:
        kb_rows.append(nav_row)

    # Always show back to main menu
    kb_rows.append([{"text": "⬅️ Back to Menu", "callback_data": "back_to_menu"}])

    kb_dict = {"inline_keyboard": kb_rows}
    return text, kb_dict

def help_text():
    return (
        "ℹ️ *Help / Instructions*\n\n"
        "➕ Add Product: `name;price;stock`\n"
        "✏️ Update Product: `id;new_name;new_price;new_stock`\n"
        "🛒 Record Sale: `product_name;quantity`\n"
        "📦 View Stock: Shows current stock levels.\n"
        "📊 Reports: Choose Daily, Weekly, or Monthly sales reports.\n"
        "⬅️ Use Back to Menu buttons to return to the main menu anytime."
    )


# -------------------- Helpers --------------------
def parse_input(text: str, expected_parts: int):
    """
    Normalize input and split into expected parts.
    Accepts both ';' and ',' as separators.
    """
    normalized = text.replace(",", ";")
    parts = [p.strip() for p in normalized.split(";") if p.strip()]
    
    if len(parts) != expected_parts:
        raise ValueError(f"Expected {expected_parts} parts, got {len(parts)}")
    
    return parts


def create_user(chat_id: int, username: str, password: str, full_name: str, email: str) -> User:
    """Create a new user object and save to DB."""
    db = SessionLocal()
    try:
        user = User(
            chat_id=chat_id,
            username=username,
            password_hash=password,
            full_name=full_name,
            email=email
        )
        db.add(user)
        db.commit()
        db.refresh(user)
        return user
    except SQLAlchemyError as e:
        db.rollback()
        print("❌ Failed to create user:", e)
        return None
    finally:
        db.close()

def save_user(user: User):
    """Optional helper, if you already commit in create_user, this can be just pass."""
    pass

def register_new_user(central_db: Session, chat_id: int, text: str, role="keeper"):
    """
    Register a new user in a tenant-aware way - UPDATED for schema-based multi-tenancy.
    
    - central_db: SQLAlchemy session for central DB
    - chat_id: ID of the user sending the command (owner)
    - text: input text (user_id;name)
    - role: 'keeper' or 'owner'
    """

    # -------------------- Parse Input --------------------
    try:
        user_id_str, name = parse_input(text, 2)
        new_chat_id = int(user_id_str)
        name = name.strip()
        if not name:
            raise ValueError("Name cannot be empty")
    except Exception as e:
        send_message(chat_id, f"❌ Invalid input: {str(e)}\nSend as: `user_id;name`")
        return

    # -------------------- Check for Existing User/Owner --------------------
    existing_user = central_db.query(User).filter(User.chat_id == new_chat_id).first()
    if existing_user:
        send_message(chat_id, f"❌ User with ID {new_chat_id} already exists.")
        return

    # -------------------- Handle Owner Registration --------------------
    if role == "owner":
        # Create new owner user
        new_user = User(
            name=name,
            username=f"owner{new_chat_id}",
            email=f"{new_chat_id}@example.com",
            password_hash=hash_password(generate_password()),  # Generate random password
            chat_id=new_chat_id,
            role="owner",
            tenant_schema=f"tenant_{new_chat_id}"  # Will be set by create_tenant_db
        )
        
        try:
            central_db.add(new_user)
            central_db.commit()
            central_db.refresh(new_user)
            
            # Create tenant schema and tables
            tenant_db_url = create_tenant_db(new_chat_id)
            
            send_message(chat_id, f"✅ Owner '{name}' registered successfully.")
            send_message(new_chat_id, f"👋 Hello {name}! Use /start to begin and set up your shop.")
            
        except Exception as e:
            central_db.rollback()
            send_message(chat_id, f"❌ Database error: {str(e)}")
            return

    # -------------------- Handle Shopkeeper Registration --------------------
    else:
        # Find the owner who's creating this shopkeeper
        owner = central_db.query(User).filter(User.chat_id == chat_id, User.role == "owner").first()
        if not owner:
            send_message(chat_id, "❌ Only owners can create shopkeepers.")
            return

        if not owner.tenant_schema:
            send_message(chat_id, "❌ Owner doesn't have a tenant schema. Please set up your shop first.")
            return

        # Create shopkeeper user (shared owner's tenant schema)
        new_user = User(
            name=name,
            username=f"sk{new_chat_id}",
            email=f"{new_chat_id}@example.com",
            password_hash=hash_password(generate_password()),  # Generate random password
            chat_id=None,  # Will be set when shopkeeper logs in
            role="shopkeeper",
            tenant_schema=owner.tenant_schema  # Share owner's tenant schema
        )
        
        try:
            central_db.add(new_user)
            central_db.commit()
            central_db.refresh(new_user)
            
            send_message(chat_id, f"✅ Shopkeeper '{name}' registered successfully.")
            send_message(new_chat_id, f"👋 Hello {name}! You've been added as a shopkeeper. Use /start to begin.")
            
        except Exception as e:
            central_db.rollback()
            send_message(chat_id, f"❌ Database error: {str(e)}")
            return
            
# -------------------- Products --------------------

def get_stock_list(db: Session):
    """
    Retrieve the stock list for the current tenant.
    The `db` session should already be connected to the tenant's database.
    """
    products = db.query(ProductORM).all()  # Only products in this tenant DB
    if not products:
        return "📦 No products found."
    
    lines = ["📦 *Stock Levels:*"]
    for p in products:
        lines.append(f"{p.name} — {p.stock}")
    
    return "\n".join(lines)


def add_product(db: Session, chat_id: int, data: dict):
    """
    Add a product in a tenant-aware way using structured `data` collected step by step.
    The `db` session is already connected to the tenant's DB.
    """
    try:
        name = data.get("name")
        price = float(data.get("price", 0))
        stock = int(data.get("quantity", 0))
        unit_type = data.get("unit_type", "unit")
        min_stock_level = int(data.get("min_stock_level", 0))
        low_stock_threshold = int(data.get("low_stock_threshold", 0))

        if not name:
            raise ValueError("Missing product name.")
        if price <= 0:
            raise ValueError("Price must be greater than 0.")
        if stock < 0:
            raise ValueError("Stock cannot be negative.")
    except Exception as e:
        send_message(chat_id, f"❌ Invalid product data: {str(e)}")
        return

    # Ensure product is unique for this tenant
    existing = db.query(ProductORM).filter(func.lower(ProductORM.name) == name.lower()).first()
    if existing:
        send_message(chat_id, f"❌ Product '{name}' already exists.")
        return

    new_product = ProductORM(
        name=name,
        price=price,
        stock=stock,
        unit_type=unit_type,
        min_stock_level=min_stock_level,
        low_stock_threshold=low_stock_threshold,
    )

    try:
        db.add(new_product)
        db.commit()
        db.refresh(new_product)
    except Exception as e:
        db.rollback()
        send_message(chat_id, f"❌ Database error: {str(e)}")
        return

    send_message(
        chat_id,
        f"✅ Product added: *{name}*\n💲 Price: {price}\n📦 Stock: {stock} {unit_type}\n"
        f"📊 Min Level: {min_stock_level}, ⚠️ Low Stock Alert: {low_stock_threshold}"
    )


# In your telegram.py, replace the notification functions with:

def add_product_pending_approval(tenant_db, chat_id, data):
    """Save product addition request for owner approval"""
    try:
        # Get shopkeeper info
        central_db = SessionLocal()
        shopkeeper = central_db.query(User).filter(User.chat_id == chat_id).first()
        
        if not shopkeeper:
            logger.error(f"❌ Shopkeeper not found for chat_id: {chat_id}")
            central_db.close()
            return False

        # Create pending approval record
        pending_approval = PendingApprovalORM(
            action_type='add_product',
            shopkeeper_id=shopkeeper.user_id,
            shopkeeper_name=shopkeeper.name,
            product_data=json.dumps(data),
            status='pending'
        )
        
        tenant_db.add(pending_approval)
        tenant_db.commit()
        tenant_db.refresh(pending_approval)  # Get the approval_id
        
        # Find owner for this tenant
        owner = central_db.query(User).filter(
            User.tenant_schema == shopkeeper.tenant_schema,
            User.role == 'owner'
        ).first()
        central_db.close()
        
        if owner:
            # Use centralized notification system
            notify_owner_of_pending_approval(
                owner.chat_id, 
                'add_product', 
                data.get('name', 'Unknown Product'), 
                shopkeeper.name, 
                pending_approval.approval_id
            )
        
        logger.info(f"✅ Product addition pending approval: {data.get('name', 'Unknown')}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to save pending approval: {e}")
        tenant_db.rollback()
        return False

def handle_approval_action(owner_chat_id, approval_id, action):
    """Handle approval or rejection of pending actions"""
    try:
        central_db = SessionLocal()
        owner = central_db.query(User).filter(User.chat_id == owner_chat_id).first()
        
        if not owner or owner.role != 'owner':
            logger.error(f"❌ Only owners can approve actions: {owner_chat_id}")
            central_db.close()
            return False
        
        tenant_db = get_tenant_session(owner.tenant_schema, owner_chat_id)
        if not tenant_db:
            central_db.close()
            return False
        
        # Get pending approval
        pending = tenant_db.query(PendingApprovalORM).filter(
            PendingApprovalORM.approval_id == approval_id,
            PendingApprovalORM.status == 'pending'
        ).first()
        
        if not pending:
            logger.error(f"❌ Pending approval not found: {approval_id}")
            tenant_db.close()
            central_db.close()
            return False
        
        product_data = json.loads(pending.product_data)
        product_name = product_data.get('name', 'Unknown Product')
        
        if action == "approved":
            # Process the approved action
            if pending.action_type == 'add_product':
                # Add the product to the database
                add_product(tenant_db, pending.shopkeeper_id, product_data)
            
            # Update approval status
            pending.status = 'approved'
            pending.resolved_at = func.now()
            
            # Notify shopkeeper using centralized system
            shopkeeper = central_db.query(User).filter(User.user_id == pending.shopkeeper_id).first()
            if shopkeeper and shopkeeper.chat_id:
                notify_shopkeeper_of_approval_result(
                    shopkeeper.chat_id, 
                    product_name, 
                    'added', 
                    True
                )
            
        else:  # rejected
            pending.status = 'rejected'
            pending.resolved_at = func.now()
            
            # Notify shopkeeper using centralized system
            shopkeeper = central_db.query(User).filter(User.user_id == pending.shopkeeper_id).first()
            if shopkeeper and shopkeeper.chat_id:
                notify_shopkeeper_of_approval_result(
                    shopkeeper.chat_id, 
                    product_name, 
                    'added', 
                    False
                )
        
        tenant_db.commit()
        tenant_db.close()
        central_db.close()
        
        logger.info(f"✅ Approval {action}: {approval_id}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to handle approval action: {e}")
        return False
        
def handle_stock_approval_action(owner_chat_id, approval_id, action):
    """Handle approval or rejection of stock update requests"""
    try:
        central_db = SessionLocal()
        owner = central_db.query(User).filter(User.chat_id == owner_chat_id).first()
        
        if not owner or owner.role != 'owner':
            logger.error(f"❌ Only owners can approve stock updates: {owner_chat_id}")
            central_db.close()
            return False
        
        tenant_db = get_tenant_session(owner.tenant_schema, owner_chat_id)
        if not tenant_db:
            central_db.close()
            return False
        
        # Get pending stock approval
        pending = tenant_db.query(PendingApprovalORM).filter(
            PendingApprovalORM.approval_id == approval_id,
            PendingApprovalORM.action_type == 'stock_update',
            PendingApprovalORM.status == 'pending'
        ).first()
        
        if not pending:
            logger.error(f"❌ Pending stock approval not found: {approval_id}")
            tenant_db.close()
            central_db.close()
            return False
        
        stock_data = json.loads(pending.product_data)
        product_id = stock_data.get('product_id')
        product_name = stock_data.get('product_name', 'Unknown Product')
        old_stock = stock_data.get('old_stock', 0)
        new_stock = stock_data.get('new_stock', 0)
        quantity_added = stock_data.get('quantity_added', 0)
        
        if action == "approved":
            # Update the product stock
            product = tenant_db.query(ProductORM).filter(
                ProductORM.product_id == product_id
            ).first()
            
            if product:
                product.stock = new_stock
                logger.info(f"✅ Stock updated: {product_name} from {old_stock} to {new_stock}")
            
            # Update approval status
            pending.status = 'approved'
            pending.resolved_at = func.now()
            
            # Notify shopkeeper
            shopkeeper = central_db.query(User).filter(
                User.user_id == pending.shopkeeper_id
            ).first()
            
            if shopkeeper and shopkeeper.chat_id:
                # Use the existing notification function
                notify_shopkeeper_of_approval_result(
                    shopkeeper.chat_id,
                    product_name,
                    'stock updated',
                    True
                )
            
        else:  # rejected
            pending.status = 'rejected'
            pending.resolved_at = func.now()
            
            # Notify shopkeeper
            shopkeeper = central_db.query(User).filter(
                User.user_id == pending.shopkeeper_id
            ).first()
            
            if shopkeeper and shopkeeper.chat_id:
                notify_shopkeeper_of_approval_result(
                    shopkeeper.chat_id,
                    product_name,
                    'stock updated',
                    False
                )
        
        tenant_db.commit()
        tenant_db.close()
        central_db.close()
        
        logger.info(f"✅ Stock update {action}: {approval_id}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to handle stock approval action: {e}")
        return False
        
def show_approval_details(chat_id, approval_id):
    """Show details of a specific approval request"""
    try:
        central_db = SessionLocal()
        user = central_db.query(User).filter(User.chat_id == chat_id).first()
        
        if not user:
            send_message(chat_id, "❌ User not found.")
            central_db.close()
            return False
        
        tenant_db = get_tenant_session(user.tenant_schema, chat_id)
        if not tenant_db:
            send_message(chat_id, "❌ Unable to access store database.")
            central_db.close()
            return False
        
        # Get pending approval
        pending = tenant_db.query(PendingApprovalORM).filter(
            PendingApprovalORM.approval_id == approval_id
        ).first()
        
        if not pending:
            send_message(chat_id, "❌ Approval request not found.")
            tenant_db.close()
            central_db.close()
            return False
        
        # Parse product data
        product_data = json.loads(pending.product_data)
        
        # Build message based on action type
        if pending.action_type == 'add_product':
            message = f"📋 *Product Addition Request*\n\n"
            message += f"👤 Requested by: {pending.shopkeeper_name}\n"
            message += f"🕐 Date: {pending.created_at.strftime('%Y-%m-%d %H:%M')}\n\n"
            message += f"📦 *Product Details:*\n"
            message += f"• Name: {product_data.get('name', 'N/A')}\n"
            message += f"• Price: ${product_data.get('price', 0):.2f}\n"
            message += f"• Quantity: {product_data.get('quantity', 0)}\n"
            message += f"• Unit Type: {product_data.get('unit_type', 'N/A')}\n"
            
        elif pending.action_type == 'stock_update':
            message = f"📋 *Stock Update Request*\n\n"
            message += f"👤 Requested by: {pending.shopkeeper_name}\n"
            message += f"🕐 Date: {pending.created_at.strftime('%Y-%m-%d %H:%M')}\n\n"
            message += f"📦 *Stock Details:*\n"
            message += f"• Product: {product_data.get('product_name', 'N/A')}\n"
            message += f"• Old Stock: {product_data.get('old_stock', 0)}\n"
            message += f"• New Stock: {product_data.get('new_stock', 0)}\n"
            message += f"• Quantity Added: {product_data.get('quantity_added', 0)}\n"
        
        else:
            message = f"📋 *Approval Request*\n\n"
            message += f"Type: {pending.action_type}\n"
            message += f"Requested by: {pending.shopkeeper_name}\n"
            message += f"Status: {pending.status}\n"
        
        # Add action buttons if pending
        if pending.status == 'pending':
            if pending.action_type == 'stock_update':
                approve_cb = f"approve_stock:{approval_id}"
                reject_cb = f"reject_stock:{approval_id}"
            else:
                approve_cb = f"approve_action:{approval_id}"
                reject_cb = f"reject_action:{approval_id}"
                
            kb_rows = [
                [
                    {"text": "✅ Approve", "callback_data": approve_cb},
                    {"text": "❌ Reject", "callback_data": reject_cb}
                ],
                [{"text": "⬅️ Back to Menu", "callback_data": "back_to_menu"}]
            ]
        else:
            kb_rows = [
                [{"text": "⬅️ Back to Menu", "callback_data": "back_to_menu"}]
            ]
        
        send_message(chat_id, message, {"inline_keyboard": kb_rows})
        
        tenant_db.close()
        central_db.close()
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to show approval details: {e}")
        send_message(chat_id, "❌ Error loading approval details.")
        return False
        
        
def update_product(db: Session, chat_id: int, product: ProductORM, data: dict):
    """
    Update a product in a tenant-aware way.
    Accepts a ProductORM instance and a `data` dict containing any updated fields.
    Supports "-" to keep existing values.
    """
    try:
        # -------------------- Name --------------------
        if "new_name" in data and data["new_name"] != "-":
            product.name = data["new_name"].strip()

        # -------------------- Price --------------------
        if "new_price" in data and data["new_price"] != "-":
            try:
                product.price = float(data["new_price"])
                if product.price <= 0:
                    raise ValueError("Price must be greater than 0.")
            except ValueError:
                send_message(chat_id, "❌ Invalid price. Please enter a number.")
                return

        # -------------------- Quantity --------------------
        if "new_quantity" in data and data["new_quantity"] != "-":
            try:
                product.stock = int(data["new_quantity"])
                if product.stock < 0:
                    raise ValueError("Stock cannot be negative.")
            except ValueError:
                send_message(chat_id, "❌ Invalid quantity. Please enter a whole number.")
                return

        # -------------------- Unit Type --------------------
        if "new_unit" in data and data["new_unit"] != "-":
            product.unit_type = data["new_unit"].strip()

        # -------------------- Min Stock Level --------------------
        if "new_min_stock" in data and data["new_min_stock"] != "-":
            try:
                product.min_stock_level = int(data["new_min_stock"])
            except ValueError:
                send_message(chat_id, "❌ Invalid minimum stock level. Please enter a whole number.")
                return

        # -------------------- Low Stock Threshold --------------------
        if "new_low_threshold" in data and data["new_low_threshold"] != "-":
            try:
                product.low_stock_threshold = int(data["new_low_threshold"])
            except ValueError:
                send_message(chat_id, "❌ Invalid low stock threshold. Please enter a whole number.")
                return

        # -------------------- Commit --------------------
        db.commit()
        db.refresh(product)
        send_message(
            chat_id,
            f"✅ Product updated successfully:\n"
            f"📦 {product.name}\n"
            f"💲 Price: {product.price}\n"
            f"📊 Stock: {product.stock} {product.unit_type}\n"
            f"📉 Min Level: {product.min_stock_level}, ⚠️ Alert: {product.low_stock_threshold}"
        )

    except Exception as e:
        db.rollback()
        send_message(chat_id, f"❌ Failed to update product: {str(e)}")


def get_cart_summary(cart):
    """Generate a formatted cart summary"""
    if not cart:
        return "🛒 Cart is empty"
    
    summary = "🛒 *Current Cart:*\n"
    total = 0
    for i, item in enumerate(cart, 1):
        summary += f"{i}. {item['name']} - {item['quantity']} {item['unit_type']} × ${item['price']:.2f} = ${item['subtotal']:.2f}\n"
        total += item['subtotal']
    
    summary += f"\n💰 *Total: ${total:.2f}*\n"
    return summary

def ensure_payment_method_column(tenant_db, schema_name):
    """Safely add payment_method column if it doesn't exist"""
    try:
        # Check if column exists using raw SQL
        check_stmt = text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = :schema 
            AND table_name = 'sales' 
            AND column_name = 'payment_method'
        """)
        result = tenant_db.execute(check_stmt, {"schema": schema_name}).fetchone()
        
        if not result:
            # Add the column
            alter_stmt = text("""
                ALTER TABLE sales 
                ADD COLUMN payment_method VARCHAR(50) DEFAULT 'cash'
            """)
            tenant_db.execute(alter_stmt)
            tenant_db.commit()
            logger.info(f"✅ Added payment_method column to sales table in {schema_name}")
            return True
        else:
            logger.info(f"✅ payment_method column already exists in {schema_name}")
            return True
            
    except Exception as e:
        logger.error(f"❌ Failed to ensure payment_method column: {e}")
        tenant_db.rollback()
        return False
        
def record_cart_sale(tenant_db, chat_id, data):
    """Record a sale from cart data with payment_method tracking and stock updates - UPDATED FOR MULTI-SHOP"""
    try:
        # ✅ Calculate surcharge for Ecocash
        payment_method = data.get("payment_method", "cash")
        surcharge = 0
        
        if payment_method == "ecocash":
            # Calculate 10% surcharge on cart total
            cart_total = sum(item["subtotal"] for item in data["cart"])
            surcharge = cart_total * 0.10
            data["surcharge"] = surcharge  # Store for receipt
            data["final_total"] = cart_total + surcharge
            data["original_total"] = cart_total  # Store original total for receipt
        
        # ✅ Get selected shop ID (default to main shop if not specified)
        shop_id = data.get("selected_shop_id")
        
        # If no shop specified, find main shop
        if not shop_id:
            main_shop = tenant_db.query(ShopORM).filter(ShopORM.is_main == True).first()
            if not main_shop:
                # If no main shop, use first shop
                main_shop = tenant_db.query(ShopORM).first()
            if main_shop:
                shop_id = main_shop.shop_id
                shop_name = main_shop.name
            else:
                logger.error("❌ No shops found in database")
                send_message(chat_id, "❌ No shops configured. Please set up shops first.")
                return False
        else:
            # Get shop name for selected shop
            shop = tenant_db.query(ShopORM).filter(ShopORM.shop_id == shop_id).first()
            shop_name = shop.name if shop else "Selected Shop"
        
        # ✅ Get or create customer
        customer_id = None
        if data.get("customer_name"):
            # Check if customer exists
            existing_customer = tenant_db.query(CustomerORM).filter(
                CustomerORM.name.ilike(data["customer_name"])
            ).first()
            
            if existing_customer:
                customer_id = existing_customer.customer_id
            else:
                # Create new customer
                new_customer = CustomerORM(
                    name=data["customer_name"],
                    contact=data.get("customer_contact", "")
                )
                tenant_db.add(new_customer)
                tenant_db.flush()  # Get the customer_id
                customer_id = new_customer.customer_id
        
        # ✅ Check stock availability for each item in the selected shop
        for item in data["cart"]:
            # Check shop-specific stock
            shop_stock = tenant_db.query(ProductShopStockORM).filter(
                ProductShopStockORM.product_id == item["product_id"],
                ProductShopStockORM.shop_id == shop_id
            ).first()

            if not shop_stock:
                # Get product name for error message
                product = tenant_db.query(ProductORM).filter(
                    ProductORM.product_id == item["product_id"]
                ).first()
                product_name = product.name if product else f"ID:{item['product_id']}"
                
                logger.error(f"❌ Product {product_name} not available in selected shop")
                send_message(chat_id, f"❌ {product_name} not available in shop '{shop_name}'.")
                return False

            if shop_stock.stock < item["quantity"]:
                # Get product name for error message
                product = tenant_db.query(ProductORM).filter(
                    ProductORM.product_id == item["product_id"]
                ).first()
                product_name = product.name if product else f"ID:{item['product_id']}"
                
                logger.error(f"❌ Insufficient stock for {product_name} in selected shop")
                send_message(chat_id, f"❌ Insufficient stock for {product_name} in shop '{shop_name}'. Available: {shop_stock.stock}")
                return False
        
        # ✅ THEN: Record each item as separate sale WITH SHOP ID
        cart_total = sum(item["subtotal"] for item in data["cart"])
        
        for item in data["cart"]:
            # Calculate item's share of surcharge (proportional)
            item_share = (item["subtotal"] / cart_total * surcharge) if cart_total > 0 else 0
            item_total = item["subtotal"] + item_share
            
            # ✅ UPDATED: Include shop_id in sale record
            stmt = text("""
                INSERT INTO sales 
                (user_id, product_id, shop_id, customer_id, unit_type, quantity, total_amount, 
                 surcharge_amount, sale_date, payment_type, payment_method, amount_paid, 
                 pending_amount, change_left)
                VALUES 
                (:user_id, :product_id, :shop_id, :customer_id, :unit_type, :quantity, :total_amount,
                 :surcharge_amount, :sale_date, :payment_type, :payment_method, :amount_paid, 
                 :pending_amount, :change_left)
            """)
            
            params = {
                "user_id": chat_id,
                "product_id": item["product_id"],
                "shop_id": shop_id,  # ✅ ADDED: Store which shop made the sale
                "customer_id": customer_id,
                "unit_type": item["unit_type"],
                "quantity": item["quantity"],
                "total_amount": item_total,  # Includes surcharge share
                "surcharge_amount": item_share,  # Item's share of surcharge
                "sale_date": datetime.utcnow(),
                "payment_type": data.get("payment_type", "full"),
                "payment_method": payment_method,
                "amount_paid": data.get("amount_paid", 0),
                "pending_amount": data.get("pending_amount", 0),
                "change_left": data.get("change_left", 0)
            }
            
            tenant_db.execute(stmt, params)
            
            # ✅ Update shop-specific stock
            shop_stock = tenant_db.query(ProductShopStockORM).filter(
                ProductShopStockORM.product_id == item["product_id"],
                ProductShopStockORM.shop_id == shop_id
            ).first()
            
            if shop_stock:
                shop_stock.stock -= item["quantity"]
                logger.info(f"✅ Stock updated for shop {shop_id}: {item['name']} -{item['quantity']}")
            
            logger.info(f"✅ Sale recorded: {item['name']} x {item['quantity']}, Shop: {shop_id}, Surcharge: ${item_share:.2f}")        
        
        tenant_db.commit()
        logger.info(f"✅ All sales recorded and stock updated for chat_id: {chat_id}, shop_id: {shop_id}")
        
        # ✅ Show final receipt with shop information
        receipt = f"✅ *Sale Completed Successfully!*\n\n"
        receipt += f"🏪 Shop: {shop_name}\n"
        receipt += f"📅 Date: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}\n"
        receipt += f"---\n"
        
        # Add cart items to receipt
        receipt += get_cart_summary(data["cart"])
        
        if data.get("payment_method") == "ecocash" and data.get("surcharge", 0) > 0:
            receipt += f"\n💳 *Payment Method: Ecocash*\n"
            receipt += f"💰 Subtotal: ${data.get('original_total', 0):.2f}\n"
            receipt += f"⚡ Surcharge (10%): ${data.get('surcharge', 0):.2f}\n"
            receipt += f"💵 *Amount Paid: ${data.get('amount_paid', 0):.2f}*\n"
        else:
            receipt += f"\n💳 Payment Method: {data.get('payment_method', 'cash').title()}\n"
            receipt += f"💰 Sale Type: {data.get('sale_type', 'cash').title()}\n"
            receipt += f"💵 Amount Paid: ${data.get('amount_paid', 0):.2f}\n"
        
        if data.get("change_left", 0) > 0:
            receipt += f"🪙 Change: ${data['change_left']:.2f}\n"
        if data.get("pending_amount", 0) > 0:
            receipt += f"📋 Pending: ${data['pending_amount']:.2f}\n"
        if data.get("customer_name"):
            receipt += f"👤 Customer: {data['customer_name']}\n"
            if data.get("customer_contact"):
                receipt += f"📞 Contact: {data['customer_contact']}\n"
            
        send_message(chat_id, receipt)
        
        # ✅ Check for low stock alerts for this shop
        for item in data["cart"]:
            check_low_stock_alerts(tenant_db, item["product_id"], shop_id)
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Cart sale recording failed: {e}")
        tenant_db.rollback()
        send_message(chat_id, f"❌ Failed to record sale: {str(e)}")
        return False

def check_low_stock_alerts(tenant_db, product_id, shop_id):
    """Check and notify about low stock for specific shop"""
    
    shop_stock = tenant_db.query(ProductShopStockORM).filter(
        ProductShopStockORM.product_id == product_id,
        ProductShopStockORM.shop_id == shop_id
    ).first()
    
    if shop_stock and shop_stock.is_low_stock():
        # Get shop info
        shop = tenant_db.query(ShopORM).filter(ShopORM.shop_id == shop_id).first()
        product = tenant_db.query(ProductORM).filter(ProductORM.product_id == product_id).first()
        
        if shop and product:
            # Notify owner (find owner in central DB)
            from app.core import SessionLocal
            central_db = SessionLocal()
            
            try:
                # Get current schema name from tenant_db
                result = tenant_db.execute(text("SELECT current_schema()")).fetchone()
                schema_name = result[0] if result else None
                
                if schema_name:
                    owner = central_db.query(User).filter(
                        User.tenant_schema == schema_name,
                        User.role == "owner"
                    ).first()
                    
                    if owner and owner.chat_id:
                        alert_msg = f"⚠️ *LOW STOCK ALERT* ⚠️\n\n"
                        alert_msg += f"🏪 Shop: {shop.name}\n"
                        alert_msg += f"📦 Product: {product.name}\n"
                        alert_msg += f"📊 Current Stock: {shop_stock.stock}\n"
                        alert_msg += f"⚡ Low Threshold: {shop_stock.low_stock_threshold}\n"
                        if shop_stock.stock <= shop_stock.min_stock_level:
                            alert_msg += f"🚨 *CRITICAL: Below minimum stock level!*\n"
                        else:
                            alert_msg += f"⚠️ *Running low!*\n"
                        
                        send_message(owner.chat_id, alert_msg)
            
            except Exception as e:
                logger.error(f"❌ Error sending low stock alert: {e}")
            finally:
                central_db.close()

def show_approval_details(chat_id, approval_id):
    """Show details of a specific approval request"""
    try:
        central_db = SessionLocal()
        user = central_db.query(User).filter(User.chat_id == chat_id).first()
        
        if not user:
            send_message(chat_id, "❌ User not found.")
            central_db.close()
            return False
        
        tenant_db = get_tenant_session(user.tenant_schema, chat_id)
        if not tenant_db:
            send_message(chat_id, "❌ Unable to access store database.")
            central_db.close()
            return False
        
        # Get pending approval
        pending = tenant_db.query(PendingApprovalORM).filter(
            PendingApprovalORM.approval_id == approval_id
        ).first()
        
        if not pending:
            send_message(chat_id, "❌ Approval request not found.")
            tenant_db.close()
            central_db.close()
            return False
        
        # Parse product data
        product_data = json.loads(pending.product_data)
        
        # Build message based on action type
        if pending.action_type == 'add_product':
            message = f"📋 *Product Addition Request*\n\n"
            message += f"👤 Requested by: {pending.shopkeeper_name}\n"
            message += f"🕐 Date: {pending.created_at.strftime('%Y-%m-%d %H:%M')}\n\n"
            message += f"📦 *Product Details:*\n"
            message += f"• Name: {product_data.get('name', 'N/A')}\n"
            message += f"• Price: ${product_data.get('price', 0):.2f}\n"
            message += f"• Quantity: {product_data.get('quantity', 0)}\n"
            message += f"• Unit Type: {product_data.get('unit_type', 'N/A')}\n"
            
        elif pending.action_type == 'stock_update':
            message = f"📋 *Stock Update Request*\n\n"
            message += f"👤 Requested by: {pending.shopkeeper_name}\n"
            message += f"🕐 Date: {pending.created_at.strftime('%Y-%m-%d %H:%M')}\n\n"
            message += f"📦 *Stock Details:*\n"
            message += f"• Product: {product_data.get('product_name', 'N/A')}\n"
            message += f"• Old Stock: {product_data.get('old_stock', 0)}\n"
            message += f"• New Stock: {product_data.get('new_stock', 0)}\n"
            message += f"• Quantity Added: {product_data.get('quantity_added', 0)}\n"
        
        else:
            message = f"📋 *Approval Request*\n\n"
            message += f"Type: {pending.action_type}\n"
            message += f"Requested by: {pending.shopkeeper_name}\n"
            message += f"Status: {pending.status}\n"
        
        # Add action buttons if pending
        if pending.status == 'pending':
            if pending.action_type == 'stock_update':
                approve_cb = f"approve_stock:{approval_id}"
                reject_cb = f"reject_stock:{approval_id}"
            else:
                approve_cb = f"approve_action:{approval_id}"
                reject_cb = f"reject_action:{approval_id}"
                
            kb_rows = [
                [
                    {"text": "✅ Approve", "callback_data": approve_cb},
                    {"text": "❌ Reject", "callback_data": reject_cb}
                ],
                [{"text": "⬅️ Back to Menu", "callback_data": "back_to_menu"}]
            ]
        else:
            kb_rows = [
                [{"text": "⬅️ Back to Menu", "callback_data": "back_to_menu"}]
            ]
        
        send_message(chat_id, message, {"inline_keyboard": kb_rows})
        
        tenant_db.close()
        central_db.close()
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to show approval details: {e}")
        send_message(chat_id, "❌ Error loading approval details.")
        return False
        
        
def record_sale(db: Session, chat_id: int, data: dict):
    """
    Record a sale in tenant DB step-by-step.
    Expects `data` dict with keys:
    - product_id
    - unit_type (optional, defaults to product.unit_type)
    - quantity
    - payment_type (full/partial/credit)
    - amount_paid (optional if full)
    - customer_name (optional)
    - customer_contact (optional)
    """
    try:
        # -------------------- Fetch Product --------------------
        product = db.query(ProductORM).filter(ProductORM.product_id == data["product_id"]).first()
        if not product:
            send_message(chat_id, "❌ Product not found.")
            return

        qty = int(data.get("quantity", 0))
        if qty <= 0:
            send_message(chat_id, "❌ Quantity must be > 0")
            return
        if product.stock < qty:
            send_message(chat_id, f"❌ Insufficient stock. Available: {product.stock}")
            return

        # -------------------- Fetch User --------------------
        user = db.query(User).filter(User.user_id == chat_id).first()
        if not user:
            send_message(chat_id, "❌ User not found.")
            return

        # -------------------- Payment Calculations --------------------
        payment_type = data.get("payment_type", "full")
        amount_paid = float(data.get("amount_paid", 0.0)) if data.get("amount_paid") is not None else 0.0
        total_amount = float(product.price) * qty
        pending_amount = max(total_amount - amount_paid, 0.0)
        change_left = max(amount_paid - total_amount, 0.0)
        unit_type = data.get("unit_type", product.unit_type)

        # -------------------- Save Customer (if partial/credit or change) --------------------
        customer_id = None
        if payment_type in ["partial", "credit"] or change_left > 0.0:
            if data.get("customer_name") or data.get("customer_contact"):
                customer = CustomerORM(
                    name=data.get("customer_name"),
                    contact=data.get("customer_contact")
                )
                db.add(customer)
                db.flush()  # assign customer_id before commit
                customer_id = customer.customer_id

        # -------------------- Create Sale --------------------
        sale = SaleORM(
            user_id=user.user_id,
            product_id=product.product_id,
            unit_type=unit_type,
            quantity=qty,
            total_amount=total_amount,
            payment_type=payment_type,
            amount_paid=amount_paid,
            pending_amount=pending_amount,
            change_left=change_left,
            customer_id=customer_id
        )

        # -------------------- Update Stock --------------------
        product.stock = max(product.stock - qty, 0)

        # -------------------- Commit --------------------
        db.add(sale)
        db.commit()
        db.refresh(sale)

        # -------------------- Notify User --------------------
        send_message(chat_id, f"✅ Sale recorded: {qty} × {product.name} ({unit_type}) = ${total_amount}")
        send_message(chat_id, get_stock_list(db))

        # -------------------- Additional Notifications --------------------
        notify_low_stock(db, product)
        notify_top_product(db, product)
        notify_high_value_sale(db, sale)

    except Exception as e:
        db.rollback()
        send_message(chat_id, f"❌ Failed to record sale: {str(e)}")


# -------------------- Clean Tenant-Aware Reports --------------------
def generate_report(db: Session, report_type: str):
    """
    Generate tenant-aware reports with payment method details.
    - db: SQLAlchemy session (already tenant-specific)
    - report_type: report_daily, report_weekly, report_monthly, etc.
    """

    # -------------------- Daily Sales --------------------
    if report_type == "report_daily":
        # Get daily totals with payment method breakdown
        daily_totals = (
            db.query(
                func.date(SaleORM.sale_date).label("day"),
                func.sum(SaleORM.total_amount).label("total_revenue"),
                func.count(SaleORM.sale_id).label("total_orders")
            )
            .group_by(func.date(SaleORM.sale_date))
            .order_by(func.date(SaleORM.sale_date).desc())
            .limit(1)
            .first()
        )
        
        if not daily_totals:
            return "No sales data for today."
        
        # Get payment method breakdown for today
        payment_breakdown = (
            db.query(
                SaleORM.payment_method,
                func.sum(SaleORM.total_amount).label("amount"),
                func.count(SaleORM.sale_id).label("count")
            )
            .filter(func.date(SaleORM.sale_date) == daily_totals.day)
            .group_by(SaleORM.payment_method)
            .all()
        )
        
        lines = ["📅 *Daily Sales Report*"]
        lines.append(f"📊 Date: {daily_totals.day}")
        lines.append(f"💰 Total Revenue: ${float(daily_totals.total_revenue or 0):.2f}")
        lines.append(f"🛒 Total Orders: {daily_totals.total_orders}")
        
        # Payment method breakdown
        lines.append(f"\n💳 Payment Methods:")
        for payment in payment_breakdown:
            method = payment.payment_method or "Cash"  # Default to Cash if null
            percentage = (payment.amount / daily_totals.total_revenue * 100) if daily_totals.total_revenue > 0 else 0
            lines.append(f"• {method}: ${float(payment.amount):.2f} ({payment.count} orders, {percentage:.1f}%)")
        
        return "\n".join(lines)

    # -------------------- Weekly Sales (Last 7 Days) --------------------
    elif report_type == "report_weekly":        
        # Calculate last 7 days
        today = datetime.utcnow().date()
        week_ago = today - timedelta(days=7)
        
        # Get weekly totals
        weekly_totals = (
            db.query(
                func.sum(SaleORM.total_amount).label("total_revenue"),
                func.count(SaleORM.sale_id).label("total_orders")
            )
            .filter(SaleORM.sale_date >= week_ago)
            .first()
        )
        
        if not weekly_totals or not weekly_totals.total_revenue:
            return "No sales data for the past week."
        
        # Get payment method breakdown for the week
        payment_breakdown = (
            db.query(
                SaleORM.payment_method,
                func.sum(SaleORM.total_amount).label("amount"),
                func.count(SaleORM.sale_id).label("count")
            )
            .filter(SaleORM.sale_date >= week_ago)
            .group_by(SaleORM.payment_method)
            .all()
        )
        
        # Get daily breakdown
        daily_results = (
            db.query(
                func.date(SaleORM.sale_date).label("day"),
                func.sum(SaleORM.total_amount).label("daily_revenue"),
                func.count(SaleORM.sale_id).label("daily_orders")
            )
            .filter(SaleORM.sale_date >= week_ago)
            .group_by(func.date(SaleORM.sale_date))
            .order_by(func.date(SaleORM.sale_date))
            .all()
        )
        
        lines = [f"📆 *Weekly Sales Report - Last 7 Days*"]
        lines.append(f"📅 Period: {week_ago} to {today}")
        lines.append(f"💰 Total Revenue: ${float(weekly_totals.total_revenue):.2f}")
        lines.append(f"🛒 Total Orders: {weekly_totals.total_orders}")
        
        # Payment method breakdown
        lines.append(f"\n💳 Payment Methods:")
        for payment in payment_breakdown:
            method = payment.payment_method or "Cash"
            percentage = (payment.amount / weekly_totals.total_revenue * 100) if weekly_totals.total_revenue > 0 else 0
            lines.append(f"• {method}: ${float(payment.amount):.2f} ({payment.count} orders, {percentage:.1f}%)")
        
        # Daily breakdown
        lines.append(f"\n📊 Daily Breakdown:")
        
        # Fill in missing days with zero sales
        current_date = week_ago
        while current_date <= today:
            # Find sales for this date
            day_sales = next((r for r in daily_results if r.day == current_date), None)
            
            if day_sales:
                lines.append(f"• {current_date}: ${float(day_sales.daily_revenue or 0):.2f} ({day_sales.daily_orders} orders)")
            else:
                lines.append(f"• {current_date}: $0.00 (0 orders)")
            
            current_date += timedelta(days=1)
        
        return "\n".join(lines)
        
    # -------------------- Monthly Sales (Current Month by Day) --------------------
    elif report_type == "report_monthly":        
        today = datetime.utcnow().date()
        month_start = today.replace(day=1)
        
        # Get monthly totals
        monthly_totals = (
            db.query(
                func.sum(SaleORM.total_amount).label("total_revenue"),
                func.count(SaleORM.sale_id).label("total_orders")
            )
            .filter(SaleORM.sale_date >= month_start)
            .first()
        )
        
        if not monthly_totals or not monthly_totals.total_revenue:
            return f"No sales data for {today.strftime('%B %Y')}."
        
        # Get payment method breakdown for the month
        payment_breakdown = (
            db.query(
                SaleORM.payment_method,
                func.sum(SaleORM.total_amount).label("amount"),
                func.count(SaleORM.sale_id).label("count")
            )
            .filter(SaleORM.sale_date >= month_start)
            .group_by(SaleORM.payment_method)
            .all()
        )
        
        # Get daily results
        daily_results = (
            db.query(
                func.date(SaleORM.sale_date).label("day"),
                func.sum(SaleORM.total_amount).label("daily_revenue"),
                func.count(SaleORM.sale_id).label("daily_orders")
            )
            .filter(SaleORM.sale_date >= month_start)
            .group_by(func.date(SaleORM.sale_date))
            .order_by(func.date(SaleORM.sale_date))
            .all()
        )
        
        lines = [f"📊 *Monthly Sales Report - {today.strftime('%B %Y')}*"]
        lines.append(f"💰 Monthly Total: ${float(monthly_totals.total_revenue):.2f}")
        lines.append(f"🛒 Total Orders: {monthly_totals.total_orders}")
        
        # Payment method breakdown
        lines.append(f"\n💳 Payment Methods:")
        for payment in payment_breakdown:
            method = payment.payment_method or "Cash"
            percentage = (payment.amount / monthly_totals.total_revenue * 100) if monthly_totals.total_revenue > 0 else 0
            lines.append(f"• {method}: ${float(payment.amount):.2f} ({payment.count} orders, {percentage:.1f}%)")
        
        lines.append(f"\n📅 Daily Breakdown:")
        
        for r in daily_results:
            lines.append(f"• {r.day}: ${float(r.daily_revenue or 0):.2f} ({r.daily_orders} orders)")
        
        return "\n".join(lines)

    # -------------------- Payment Method Summary Report --------------------
    elif report_type == "report_payment_summary":        
        today = datetime.utcnow().date()
        month_start = today.replace(day=1)
        week_ago = today - timedelta(days=7)
        
        # Today's payment breakdown
        today_breakdown = (
            db.query(
                SaleORM.payment_method,
                func.sum(SaleORM.total_amount).label("amount"),
                func.count(SaleORM.sale_id).label("count")
            )
            .filter(func.date(SaleORM.sale_date) == today)
            .group_by(SaleORM.payment_method)
            .all()
        )
        
        # Weekly payment breakdown
        weekly_breakdown = (
            db.query(
                SaleORM.payment_method,
                func.sum(SaleORM.total_amount).label("amount"),
                func.count(SaleORM.sale_id).label("count")
            )
            .filter(SaleORM.sale_date >= week_ago)
            .group_by(SaleORM.payment_method)
            .all()
        )
        
        # Monthly payment breakdown
        monthly_breakdown = (
            db.query(
                SaleORM.payment_method,
                func.sum(SaleORM.total_amount).label("amount"),
                func.count(SaleORM.sale_id).label("count")
            )
            .filter(SaleORM.sale_date >= month_start)
            .group_by(SaleORM.payment_method)
            .all()
        )
        
        lines = ["💳 *Payment Method Summary*"]
        
        # Today's summary
        lines.append(f"\n📅 Today ({today}):")
        today_total = sum(payment.amount for payment in today_breakdown)
        for payment in today_breakdown:
            method = payment.payment_method or "Cash"
            percentage = (payment.amount / today_total * 100) if today_total > 0 else 0
            lines.append(f"• {method}: ${float(payment.amount):.2f} ({payment.count} orders, {percentage:.1f}%)")
        
        if not today_breakdown:
            lines.append("• No sales today")
        
        # Weekly summary
        lines.append(f"\n📆 Last 7 Days:")
        weekly_total = sum(payment.amount for payment in weekly_breakdown)
        for payment in weekly_breakdown:
            method = payment.payment_method or "Cash"
            percentage = (payment.amount / weekly_total * 100) if weekly_total > 0 else 0
            lines.append(f"• {method}: ${float(payment.amount):.2f} ({payment.count} orders, {percentage:.1f}%)")
        
        # Monthly summary
        lines.append(f"\n📊 This Month ({today.strftime('%B')}):")
        monthly_total = sum(payment.amount for payment in monthly_breakdown)
        for payment in monthly_breakdown:
            method = payment.payment_method or "Cash"
            percentage = (payment.amount / monthly_total * 100) if monthly_total > 0 else 0
            lines.append(f"• {method}: ${float(payment.amount):.2f} ({payment.count} orders, {percentage:.1f}%)")
        
        return "\n".join(lines)
        
    # -------------------- Low Stock Products --------------------
    elif report_type == "report_low_stock":
        # Products at or below their individual low stock threshold
        products = db.query(ProductORM).filter(
            ProductORM.stock <= ProductORM.low_stock_threshold
        ).order_by(ProductORM.stock).all()
        
        if not products:
            return "✅ All products have sufficient stock!"
        
        lines = ["⚠️ *Low Stock Alert*"]
        
        # Separate out-of-stock from low stock
        out_of_stock = [p for p in products if p.stock == 0]
        low_stock = [p for p in products if p.stock > 0]
        
        if out_of_stock:
            lines.append("\n🔴 *OUT OF STOCK:*")
            for p in out_of_stock:
                lines.append(f"• {p.name}: 0 {p.unit_type}")
        
        if low_stock:
            lines.append("\n🟡 *LOW STOCK:*")
            for p in low_stock:
                lines.append(f"• {p.name}: {p.stock} {p.unit_type} (threshold: {p.low_stock_threshold})")
        
        # Summary
        lines.append(f"\n📊 Summary: {len(out_of_stock)} out of stock, {len(low_stock)} low stock")
        
        return "\n".join(lines)
        
    # -------------------- Top Products --------------------
    elif report_type == "report_top_products":
        results = (
            db.query(
                ProductORM.name.label("product"),
                func.sum(SaleORM.quantity).label("total_qty"),
                func.sum(SaleORM.total_amount).label("total_revenue")
            )
            .join(SaleORM, ProductORM.product_id == SaleORM.product_id)
            .group_by(ProductORM.name)
            .order_by(func.sum(SaleORM.quantity).desc())
            .limit(5)
            .all()
        )
        if not results:
            return "No sales data."
        lines = ["🏆 *Top Selling Products*"]
        for r in results:
            lines.append(f"{r.product}: {r.total_qty} sold, ${float(r.total_revenue or 0):.2f} revenue")
        return "\n".join(lines)

    # -------------------- Average Order Value --------------------
    elif report_type == "report_aov":
        total_orders = db.query(func.count(SaleORM.sale_id)).scalar() or 0
        total_revenue = db.query(func.sum(SaleORM.total_amount)).scalar() or 0
        aov = round(total_revenue / total_orders, 2) if total_orders > 0 else 0
        
        # Get payment method breakdown for AOV context
        payment_breakdown = (
            db.query(
                SaleORM.payment_method,
                func.avg(SaleORM.total_amount).label("avg_amount"),
                func.count(SaleORM.sale_id).label("count")
            )
            .group_by(SaleORM.payment_method)
            .all()
        )
        
        lines = ["💰 *Average Order Value*"]
        lines.append(f"Total Orders: {total_orders}")
        lines.append(f"Total Revenue: ${total_revenue:.2f}")
        lines.append(f"AOV: ${aov:.2f}")
        
        if payment_breakdown:
            lines.append(f"\n💳 AOV by Payment Method:")
            for payment in payment_breakdown:
                method = payment.payment_method or "Cash"
                lines.append(f"• {method}: ${float(payment.avg_amount or 0):.2f} ({payment.count} orders)")
        
        return "\n".join(lines)

    # -------------------- Stock Turnover --------------------
    elif report_type == "report_stock_turnover":
        products = db.query(ProductORM).all()
        if not products:
            return "No products found."
        lines = ["📦 *Stock Turnover per Product*"]
        for p in products:
            total_sold = db.query(func.sum(SaleORM.quantity)).filter(SaleORM.product_id == p.product_id).scalar() or 0
            turnover_rate = total_sold / (p.stock + total_sold) if (p.stock + total_sold) > 0 else 0
            lines.append(f"{p.name}: Sold {total_sold}, Stock {p.stock}, Turnover Rate {turnover_rate:.2f}")
        return "\n".join(lines)

    # -------------------- Credit List --------------------
    elif report_type == "report_credits":
        # Only show sales where credit is pending AND customer details were recorded
        sales_with_credit = (
            db.query(SaleORM)
            .join(CustomerORM, SaleORM.customer_id == CustomerORM.customer_id)
            .filter(SaleORM.pending_amount > 0)
            .filter(CustomerORM.name.isnot(None))  # Only customers who provided details
            .order_by(SaleORM.sale_date.desc())
            .all()
        )
        
        if not sales_with_credit:
            return "✅ No outstanding credits (where customer details were recorded)."
        
        lines = ["💳 *Outstanding Credits*"]
        total_credit_outstanding = 0
        
        for sale in sales_with_credit:
            customer_name = sale.customer.name
            contact = sale.customer.contact or "No contact"
            product = db.query(ProductORM).filter(ProductORM.product_id == sale.product_id).first()
            product_name = product.name if product else "Unknown Product"
            
            lines.append(f"• {customer_name} ({contact}): ${float(sale.pending_amount):.2f}")
            lines.append(f"  📦 For: {sale.quantity} × {product_name}")
            lines.append(f"  📅 Date: {sale.sale_date.strftime('%Y-%m-%d')}")
            lines.append("")  # Empty line for readability
            
            total_credit_outstanding += sale.pending_amount
        
        lines.append(f"💰 *Total Credit Outstanding: ${total_credit_outstanding:.2f}*")
        
        return "\n".join(lines)
        
    # -------------------- Change List --------------------
    elif report_type == "report_change":
        # Only show sales where change is due AND customer details were recorded
        sales_with_change = (
            db.query(SaleORM)
            .join(CustomerORM, SaleORM.customer_id == CustomerORM.customer_id)
            .filter(SaleORM.change_left > 0)
            .filter(CustomerORM.name.isnot(None))  # Only customers who provided details
            .order_by(SaleORM.sale_date.desc())
            .all()
        )
        
        if not sales_with_change:
            return "✅ No customers with change due (where details were recorded)."
        
        lines = ["💵 *Change Due to Customers*"]
        total_change_due = 0
        
        for sale in sales_with_change:
            customer_name = sale.customer.name
            contact = sale.customer.contact or "No contact"
            product = db.query(ProductORM).filter(ProductORM.product_id == sale.product_id).first()
            product_name = product.name if product else "Unknown Product"
            
            lines.append(f"• {customer_name} ({contact}): ${float(sale.change_left):.2f}")
            lines.append(f"  📦 For: {sale.quantity} × {product_name}")
            lines.append(f"  📅 Date: {sale.sale_date.strftime('%Y-%m-%d')}")
            lines.append("")  # Empty line for readability
            
            total_change_due += sale.change_left
        
        lines.append(f"💰 *Total Change Due: ${total_change_due:.2f}*")
        
        return "\n".join(lines)
        
    else:
        return "❌ Unknown report type."
        
def generate_report(db: Session, report_type: str):
    """
    Generate tenant-aware reports.
    - db: SQLAlchemy session (already tenant-specific)
    - report_type: report_daily, report_weekly, report_monthly, etc.
    """
    # -------------------- Daily Sales --------------------
    if report_type == "report_daily":
        # Get daily totals with surcharge breakdown
        daily_totals = (
            db.query(
                func.date(SaleORM.sale_date).label("day"),
                func.sum(SaleORM.total_amount).label("total_revenue"),
                func.sum(SaleORM.surcharge_amount).label("total_surcharge"),
                func.count(SaleORM.sale_id).label("total_orders")
            )
            .group_by(func.date(SaleORM.sale_date))
            .order_by(func.date(SaleORM.sale_date).desc())
            .limit(1)
            .first()
        )
        
        if not daily_totals:
            return "No sales data for today."
        
        # Calculate net revenue (without surcharge)
        net_revenue = daily_totals.total_revenue - (daily_totals.total_surcharge or 0)
        
        lines = ["📅 *Daily Sales Report*"]
        lines.append(f"📊 Date: {daily_totals.day}")
        lines.append(f"💰 Gross Revenue: ${float(daily_totals.total_revenue or 0):.2f}")
        
        if daily_totals.total_surcharge and daily_totals.total_surcharge > 0:
            lines.append(f"⚡ Ecocash Surcharge: ${float(daily_totals.total_surcharge or 0):.2f}")
            lines.append(f"💵 Net Revenue (goods): ${float(net_revenue):.2f}")
        
        lines.append(f"🛒 Total Orders: {daily_totals.total_orders}")
        
        # Payment method breakdown WITH surcharge
        payment_breakdown = (
            db.query(
                SaleORM.payment_method,
                func.sum(SaleORM.total_amount).label("amount"),
                func.sum(SaleORM.surcharge_amount).label("surcharge"),
                func.count(SaleORM.sale_id).label("count")
            )
            .filter(func.date(SaleORM.sale_date) == daily_totals.day)
            .group_by(SaleORM.payment_method)
            .all()
        )
        
        if payment_breakdown:
            lines.append(f"\n💳 Payment Methods:")
            for payment in payment_breakdown:
                method = payment.payment_method or "Cash"
                percentage = (payment.amount / daily_totals.total_revenue * 100) if daily_totals.total_revenue > 0 else 0
                surcharge_msg = f" (+${float(payment.surcharge or 0):.2f} surcharge)" if payment.surcharge and payment.surcharge > 0 else ""
                lines.append(f"• {method}: ${float(payment.amount or 0):.2f}{surcharge_msg} ({payment.count} orders, {percentage:.1f}%)")
        
        return "\n".join(lines)

    # -------------------- Weekly Sales (Last 7 Days) --------------------
    elif report_type == "report_weekly":
        # Calculate last 7 days
        today = datetime.utcnow().date()
        week_ago = today - timedelta(days=7)
        
        # Get weekly totals WITH surcharge
        weekly_totals = (
            db.query(
                func.sum(SaleORM.total_amount).label("total_revenue"),
                func.sum(SaleORM.surcharge_amount).label("total_surcharge"),
                func.count(SaleORM.sale_id).label("total_orders")
            )
            .filter(SaleORM.sale_date >= week_ago)
            .first()
        )
        
        if not weekly_totals or not weekly_totals.total_revenue:
            return "No sales data for the past week."
        
        # Calculate net revenue
        net_revenue = weekly_totals.total_revenue - (weekly_totals.total_surcharge or 0)
        
        # Get payment method breakdown WITH surcharge
        payment_breakdown = (
            db.query(
                SaleORM.payment_method,
                func.sum(SaleORM.total_amount).label("amount"),
                func.sum(SaleORM.surcharge_amount).label("surcharge"),
                func.count(SaleORM.sale_id).label("count")
            )
            .filter(SaleORM.sale_date >= week_ago)
            .group_by(SaleORM.payment_method)
            .all()
        )
        
        # Get daily breakdown WITH surcharge
        daily_results = (
            db.query(
                func.date(SaleORM.sale_date).label("day"),
                func.sum(SaleORM.total_amount).label("daily_revenue"),
                func.sum(SaleORM.surcharge_amount).label("daily_surcharge"),
                func.count(SaleORM.sale_id).label("daily_orders")
            )
            .filter(SaleORM.sale_date >= week_ago)
            .group_by(func.date(SaleORM.sale_date))
            .order_by(func.date(SaleORM.sale_date))
            .all()
        )
        
        lines = [f"📆 *Weekly Sales Report - Last 7 Days*"]
        lines.append(f"📅 Period: {week_ago} to {today}")
        lines.append(f"💰 Gross Revenue: ${float(weekly_totals.total_revenue):.2f}")
        
        if weekly_totals.total_surcharge and weekly_totals.total_surcharge > 0:
            lines.append(f"⚡ Ecocash Surcharge: ${float(weekly_totals.total_surcharge):.2f}")
            lines.append(f"💵 Net Revenue (goods): ${float(net_revenue):.2f}")
        
        lines.append(f"🛒 Total Orders: {weekly_totals.total_orders}")
        
        # Payment method breakdown
        if payment_breakdown:
            lines.append(f"\n💳 Payment Methods:")
            for payment in payment_breakdown:
                method = payment.payment_method or "Cash"
                percentage = (payment.amount / weekly_totals.total_revenue * 100) if weekly_totals.total_revenue > 0 else 0
                surcharge_msg = f" (+${float(payment.surcharge or 0):.2f} surcharge)" if payment.surcharge and payment.surcharge > 0 else ""
                lines.append(f"• {method}: ${float(payment.amount):.2f}{surcharge_msg} ({payment.count} orders, {percentage:.1f}%)")
        
        # Daily breakdown WITH surcharge
        lines.append(f"\n📊 Daily Breakdown:")
        
        current_date = week_ago
        while current_date <= today:
            # Find sales for this date
            day_sales = next((r for r in daily_results if r.day == current_date), None)
            
            if day_sales:
                net_daily = day_sales.daily_revenue - (day_sales.daily_surcharge or 0)
                surcharge_msg = f" (+${float(day_sales.daily_surcharge or 0):.2f} surcharge)" if day_sales.daily_surcharge and day_sales.daily_surcharge > 0 else ""
                lines.append(f"• {current_date}: ${float(day_sales.daily_revenue or 0):.2f}{surcharge_msg} ({day_sales.daily_orders} orders)")
            else:
                lines.append(f"• {current_date}: $0.00 (0 orders)")
            
            current_date += timedelta(days=1)
        
        return "\n".join(lines)
        
    # -------------------- Monthly Sales (Current Month) --------------------
    elif report_type == "report_monthly":
        today = datetime.utcnow().date()
        month_start = today.replace(day=1)
        
        # Get monthly totals WITH surcharge
        monthly_totals = (
            db.query(
                func.sum(SaleORM.total_amount).label("total_revenue"),
                func.sum(SaleORM.surcharge_amount).label("total_surcharge"),
                func.count(SaleORM.sale_id).label("total_orders")
            )
            .filter(SaleORM.sale_date >= month_start)
            .first()
        )
        
        if not monthly_totals or not monthly_totals.total_revenue:
            return f"No sales data for {today.strftime('%B %Y')}."
        
        # Calculate net revenue
        net_revenue = monthly_totals.total_revenue - (monthly_totals.total_surcharge or 0)
        
        # Get payment method breakdown WITH surcharge
        payment_breakdown = (
            db.query(
                SaleORM.payment_method,
                func.sum(SaleORM.total_amount).label("amount"),
                func.sum(SaleORM.surcharge_amount).label("surcharge"),
                func.count(SaleORM.sale_id).label("count")
            )
            .filter(SaleORM.sale_date >= month_start)
            .group_by(SaleORM.payment_method)
            .all()
        )
        
        # Get daily results WITH surcharge
        daily_results = (
            db.query(
                func.date(SaleORM.sale_date).label("day"),
                func.sum(SaleORM.total_amount).label("daily_revenue"),
                func.sum(SaleORM.surcharge_amount).label("daily_surcharge"),
                func.count(SaleORM.sale_id).label("daily_orders")
            )
            .filter(SaleORM.sale_date >= month_start)
            .group_by(func.date(SaleORM.sale_date))
            .order_by(func.date(SaleORM.sale_date))
            .all()
        )
        
        lines = [f"📊 *Monthly Sales Report - {today.strftime('%B %Y')}*"]
        lines.append(f"💰 Gross Revenue: ${float(monthly_totals.total_revenue):.2f}")
        
        if monthly_totals.total_surcharge and monthly_totals.total_surcharge > 0:
            lines.append(f"⚡ Ecocash Surcharge: ${float(monthly_totals.total_surcharge):.2f}")
            lines.append(f"💵 Net Revenue (goods): ${float(net_revenue):.2f}")
        
        lines.append(f"🛒 Total Orders: {monthly_totals.total_orders}")
        
        # Payment method breakdown
        if payment_breakdown:
            lines.append(f"\n💳 Payment Methods:")
            for payment in payment_breakdown:
                method = payment.payment_method or "Cash"
                percentage = (payment.amount / monthly_totals.total_revenue * 100) if monthly_totals.total_revenue > 0 else 0
                surcharge_msg = f" (+${float(payment.surcharge or 0):.2f} surcharge)" if payment.surcharge and payment.surcharge > 0 else ""
                lines.append(f"• {method}: ${float(payment.amount or 0):.2f}{surcharge_msg} ({payment.count} orders, {percentage:.1f}%)")
        
        lines.append(f"\n📅 Daily Breakdown:")
        
        for r in daily_results:
            net_daily = r.daily_revenue - (r.daily_surcharge or 0)
            surcharge_msg = f" (+${float(r.daily_surcharge or 0):.2f} surcharge)" if r.daily_surcharge and r.daily_surcharge > 0 else ""
            lines.append(f"• {r.day}: ${float(r.daily_revenue or 0):.2f}{surcharge_msg} ({r.daily_orders} orders)")
        
        return "\n".join(lines)
    
    # -------------------- Payment Method Summary Report --------------------
    elif report_type == "report_payment_summary":
        today = datetime.utcnow().date()
        month_start = today.replace(day=1)
        week_ago = today - timedelta(days=7)
        
        # Today's payment breakdown WITH surcharge
        today_breakdown = (
            db.query(
                SaleORM.payment_method,
                func.sum(SaleORM.total_amount).label("amount"),
                func.sum(SaleORM.surcharge_amount).label("surcharge"),
                func.count(SaleORM.sale_id).label("count")
            )
            .filter(func.date(SaleORM.sale_date) == today)
            .group_by(SaleORM.payment_method)
            .all()
        )
        
        # Weekly payment breakdown WITH surcharge
        weekly_breakdown = (
            db.query(
                SaleORM.payment_method,
                func.sum(SaleORM.total_amount).label("amount"),
                func.sum(SaleORM.surcharge_amount).label("surcharge"),
                func.count(SaleORM.sale_id).label("count")
            )
            .filter(SaleORM.sale_date >= week_ago)
            .group_by(SaleORM.payment_method)
            .all()
        )
        
        # Monthly payment breakdown WITH surcharge
        monthly_breakdown = (
            db.query(
                SaleORM.payment_method,
                func.sum(SaleORM.total_amount).label("amount"),
                func.sum(SaleORM.surcharge_amount).label("surcharge"),
                func.count(SaleORM.sale_id).label("count")
            )
            .filter(SaleORM.sale_date >= month_start)
            .group_by(SaleORM.payment_method)
            .all()
        )
        
        lines = ["💳 *Payment Method Summary*"]
        
        # Today's summary
        lines.append(f"\n📅 Today ({today}):")
        if today_breakdown:
            for payment in today_breakdown:
                method = payment.payment_method or "Cash"
                surcharge_msg = f" (+${float(payment.surcharge or 0):.2f} surcharge)" if payment.surcharge and payment.surcharge > 0 else ""
                lines.append(f"• {method}: ${float(payment.amount):.2f}{surcharge_msg} ({payment.count} orders)")
        else:
            lines.append("• No sales today")
        
        # Weekly summary
        lines.append(f"\n📆 Last 7 Days:")
        if weekly_breakdown:
            for payment in weekly_breakdown:
                method = payment.payment_method or "Cash"
                surcharge_msg = f" (+${float(payment.surcharge or 0):.2f} surcharge)" if payment.surcharge and payment.surcharge > 0 else ""
                lines.append(f"• {method}: ${float(payment.amount):.2f}{surcharge_msg} ({payment.count} orders)")
        
        # Monthly summary
        lines.append(f"\n📊 This Month ({today.strftime('%B')}):")
        if monthly_breakdown:
            for payment in monthly_breakdown:
                method = payment.payment_method or "Cash"
                surcharge_msg = f" (+${float(payment.surcharge or 0):.2f} surcharge)" if payment.surcharge and payment.surcharge > 0 else ""
                lines.append(f"• {method}: ${float(payment.amount):.2f}{surcharge_msg} ({payment.count} orders)")
        
        return "\n".join(lines)
    
        
    # -------------------- Low Stock Products --------------------
    elif report_type == "report_low_stock":
        # Products at or below their individual low stock threshold
        products = db.query(ProductORM).filter(
            ProductORM.stock <= ProductORM.low_stock_threshold
        ).order_by(ProductORM.stock).all()
        
        if not products:
            return "✅ All products have sufficient stock!"
        
        lines = ["⚠️ *Low Stock Alert*"]
        
        # Separate out-of-stock from low stock
        out_of_stock = [p for p in products if p.stock == 0]
        low_stock = [p for p in products if p.stock > 0]
        
        if out_of_stock:
            lines.append("\n🔴 *OUT OF STOCK:*")
            for p in out_of_stock:
                lines.append(f"• {p.name}: 0 {p.unit_type}")
        
        if low_stock:
            lines.append("\n🟡 *LOW STOCK:*")
            for p in low_stock:
                lines.append(f"• {p.name}: {p.stock} {p.unit_type} (threshold: {p.low_stock_threshold})")
        
        # Summary
        lines.append(f"\n📊 Summary: {len(out_of_stock)} out of stock, {len(low_stock)} low stock")
        
        return "\n".join(lines)
        
    # -------------------- Top Products --------------------
    elif report_type == "report_top_products":
        results = (
            db.query(
                ProductORM.name.label("product"),
                func.sum(SaleORM.quantity).label("total_qty"),
                func.sum(SaleORM.total_amount).label("total_revenue")
            )
            .join(SaleORM, ProductORM.product_id == SaleORM.product_id)
            .group_by(ProductORM.name)
            .order_by(func.sum(SaleORM.quantity).desc())
            .limit(5)
            .all()
        )
        if not results:
            return "No sales data."
        lines = ["🏆 *Top Selling Products*"]
        for r in results:
            lines.append(f"{r.product}: {r.total_qty} sold, ${float(r.total_revenue or 0):.2f} revenue")
        return "\n".join(lines)

    # -------------------- Average Order Value --------------------
    elif report_type == "report_aov":
        total_orders = db.query(func.count(SaleORM.sale_id)).scalar() or 0
        total_revenue = db.query(func.sum(SaleORM.total_amount)).scalar() or 0
        aov = round(total_revenue / total_orders, 2) if total_orders > 0 else 0
        return f"💰 *Average Order Value*\nTotal Orders: {total_orders}\nTotal Revenue: ${total_revenue:.2f}\nAOV: ${aov:.2f}"

    # -------------------- Stock Turnover --------------------
    elif report_type == "report_stock_turnover":
        products = db.query(ProductORM).all()
        if not products:
            return "No products found."
        lines = ["📦 *Stock Turnover per Product*"]
        for p in products:
            total_sold = db.query(func.sum(SaleORM.quantity)).filter(SaleORM.product_id == p.product_id).scalar() or 0
            turnover_rate = total_sold / (p.stock + total_sold) if (p.stock + total_sold) > 0 else 0
            lines.append(f"{p.name}: Sold {total_sold}, Stock {p.stock}, Turnover Rate {turnover_rate:.2f}")
        return "\n".join(lines)

    # -------------------- Credit List --------------------
    elif report_type == "report_credits":
        # Only show sales where credit is pending AND customer details were recorded
        sales_with_credit = (
            db.query(SaleORM)
            .join(CustomerORM, SaleORM.customer_id == CustomerORM.customer_id)
            .filter(SaleORM.pending_amount > 0)
            .filter(CustomerORM.name.isnot(None))  # Only customers who provided details
            .order_by(SaleORM.sale_date.desc())
            .all()
        )
        
        if not sales_with_credit:
            return "✅ No outstanding credits (where customer details were recorded)."
        
        lines = ["💳 *Outstanding Credits*"]
        total_credit_outstanding = 0
        
        for sale in sales_with_credit:
            customer_name = sale.customer.name
            contact = sale.customer.contact or "No contact"
            product = db.query(ProductORM).filter(ProductORM.product_id == sale.product_id).first()
            product_name = product.name if product else "Unknown Product"
            
            lines.append(f"• {customer_name} ({contact}): ${float(sale.pending_amount):.2f}")
            lines.append(f"  📦 For: {sale.quantity} × {product_name}")
            lines.append(f"  📅 Date: {sale.sale_date.strftime('%Y-%m-%d')}")
            lines.append("")  # Empty line for readability
            
            total_credit_outstanding += sale.pending_amount
        
        lines.append(f"💰 *Total Credit Outstanding: ${total_credit_outstanding:.2f}*")
        
        return "\n".join(lines)
        
    # -------------------- Change List --------------------
    elif report_type == "report_change":
        # Only show sales where change is due AND customer details were recorded
        sales_with_change = (
            db.query(SaleORM)
            .join(CustomerORM, SaleORM.customer_id == CustomerORM.customer_id)
            .filter(SaleORM.change_left > 0)
            .filter(CustomerORM.name.isnot(None))  # Only customers who provided details
            .order_by(SaleORM.sale_date.desc())
            .all()
        )
        
        if not sales_with_change:
            return "✅ No customers with change due (where details were recorded)."
        
        lines = ["💵 *Change Due to Customers*"]
        total_change_due = 0
        
        for sale in sales_with_change:
            customer_name = sale.customer.name
            contact = sale.customer.contact or "No contact"
            product = db.query(ProductORM).filter(ProductORM.product_id == sale.product_id).first()
            product_name = product.name if product else "Unknown Product"
            
            lines.append(f"• {customer_name} ({contact}): ${float(sale.change_left):.2f}")
            lines.append(f"  📦 For: {sale.quantity} × {product_name}")
            lines.append(f"  📅 Date: {sale.sale_date.strftime('%Y-%m-%d')}")
            lines.append("")  # Empty line for readability
            
            total_change_due += sale.change_left
        
        lines.append(f"💰 *Total Change Due: ${total_change_due:.2f}*")
        
        return "\n".join(lines)
        
    else:
        return "❌ Unknown report type."
        

def report_menu_keyboard(role: str):
    """Build the reports submenu with buttons."""
    if role == "owner":
        kb_dict = {
            "inline_keyboard": [
                [{"text": "📅 Daily Sales", "callback_data": "report_daily"}],
                [{"text": "📆 Weekly Sales", "callback_data": "report_weekly"}],
                [{"text": "📊 Monthly Sales per Product", "callback_data": "report_monthly"}],
                [{"text": "⚠️ Low Stock Products", "callback_data": "report_low_stock"}],
                [{"text": "🏆 Top Products", "callback_data": "report_top_products"}],
                [{"text": "💰 Average Order Value", "callback_data": "report_aov"}],
                [{"text": "📦 Stock Turnover", "callback_data": "report_stock_turnover"}],
                [{"text": "💳 Credit List", "callback_data": "report_credits"}],
                [{"text": "💵 Change List", "callback_data": "report_change"}],
                [{"text": "⬅️ Back to Menu", "callback_data": "back_to_menu"}],
            ]
        }
    elif role == "shopkeeper":
        # Only daily, weekly, monthly + credit/change
        kb_dict = {
            "inline_keyboard": [
                [{"text": "📅 Daily Sales", "callback_data": "report_daily"}],
                [{"text": "📆 Weekly Sales", "callback_data": "report_weekly"}],
                [{"text": "📊 Monthly Sales per Product", "callback_data": "report_monthly"}],
                [{"text": "💳 Credit List", "callback_data": "report_credits"}],
                [{"text": "💵 Change List", "callback_data": "report_change"}],
                [{"text": "⬅️ Back to Menu", "callback_data": "back_to_menu"}],
            ]
        }
    else:
        kb_dict = {"inline_keyboard": [[{"text": "⬅️ Back to Menu", "callback_data": "back_to_menu"}]]}

    return kb_dict

# -------------------- Webhook --------------------
@router.post("/telegram/webhook")
async def telegram_webhook(request: Request, db: Session = Depends(get_db)):
    try:
        data = await request.json()
        print("📩 Incoming Telegram update:", data)

        chat_id = None
        text = ""
        update_type = None
        
        # Determine the update type
        if "message" in data:
            chat_id = data["message"]["chat"]["id"]
            text = data["message"].get("text", "").strip()
            update_type = "message"
        elif "callback_query" in data:
            chat_id = data["callback_query"]["message"]["chat"]["id"]
            text = data["callback_query"]["data"]
            update_type = "callback"
            callback_id = data["callback_query"]["id"]
    
            # ✅ Answer callback immediately
            requests.post(
                f"{TELEGRAM_API_URL}/answerCallbackQuery",
                json={"callback_query_id": callback_id}
            )

        if not chat_id:
            return {"ok": True}

        # 1. Get user from central DB
        user = db.query(User).filter(User.chat_id == chat_id).first()

        # 🔍 DEBUG: Log user info
        if user:
            print(f"🔍 DEBUG: User found - ID: {user.user_id}, Username: {user.username}, Role: {user.role}, Tenant Schema: {user.tenant_schema}")
        else:
            print(f"🔍 DEBUG: No user found for chat_id: {chat_id}")

        # ✅ SECURITY: Fix schema assignment ONLY for owners
        if user and user.role == "owner" and user.tenant_schema:
            expected_schema = f"tenant_{chat_id}"
            if user.tenant_schema != expected_schema:
                logger.error(f"🚨 SECURITY: Owner {user.username} has schema '{user.tenant_schema}' but should have '{expected_schema}'")
        
                # Force correction ONLY for owners
                try:
                    # This should now handle shopkeepers correctly
                    schema_name = create_tenant_db(chat_id, user.role)
                    user.tenant_schema = schema_name
                    db.commit()
                    logger.info(f"✅ Security fix: {user.username} → {schema_name}")
            
                    # Verify connection
                    tenant_db = get_tenant_session(schema_name, chat_id)
                    if tenant_db:
                        product_count = tenant_db.query(ProductORM).count()
                        if product_count > 0:
                            logger.warning(f"⚠️ Found {product_count} products in corrected schema")
                        tenant_db.close()
                except Exception as e:
                    logger.error(f"❌ Security fix failed: {e}")
                        
        # ✅ CRITICAL: Handle callbacks FIRST and RETURN immediately
        if update_type == "callback":
            logger.info(f"🎯 Processing callback: {text} from chat_id={chat_id}")

            # ✅ NEW: Handle user_type selection even when no user exists
            if text.startswith("user_type:"):
                user_type = text.split(":")[1]
        
                if user_type == "owner":
                    # Create new owner with generated credentials
                    generated_username = create_username(f"Owner{chat_id}")
                    generated_password = generate_password()
                    generated_email = f"{chat_id}_{int(time.time())}@example.com"

                    new_user = User(
                        name=f"Owner{chat_id}",
                        username=generated_username,
                        email=generated_email,
                        password_hash=hash_password(generated_password),
                        chat_id=chat_id,
                        role="owner"
                    )
                    db.add(new_user)
                    db.commit()
                    db.refresh(new_user)

                    # Create tenant schema
                    try:
                        schema_name = f"tenant_{chat_id}"
                        tenant_db_url = create_tenant_db(chat_id)
                        new_user.tenant_schema = schema_name
                        db.commit()
                        logger.info(f"✅ New owner created: {generated_username} with schema '{schema_name}'")
                    except Exception as e:
                        logger.error(f"❌ Failed to create tenant schema: {e}")
                        send_message(chat_id, "❌ Could not initialize store database.")
                        return {"ok": True}

                    # Send credentials and start shop setup
                    send_owner_credentials(chat_id, generated_username, generated_password)
                    send_message(chat_id, "🏪 Let's set up your shop! Please enter the shop name:")
                    user_states[chat_id] = {"action": "setup_shop", "step": 1, "data": {}}

                else:  # shopkeeper
                    # Step-by-step shopkeeper login
                    send_message(chat_id, "👤 Please enter your username:")
                    user_states[chat_id] = {"action": "shopkeeper_login", "step": 1, "data": {}}
        
                return {"ok": True}

            # ✅ Check if user exists for other callbacks
            if not user:
                logger.warning(f"⚠️ No user found for chat_id={chat_id}")
                send_message(chat_id, "❌ User not found in system. Please use /start first.")
                return {"ok": True}

            role = user.role
    
            # -------------------- Cancel button --------------------
            if text == "back_to_menu":
                user_states.pop(chat_id, None)
                kb_dict = main_menu(role)
                send_message(chat_id, "🏠 Main Menu:", kb_dict)
                return {"ok": True}

            # -------------------- Unified Shop Management (Owner only) --------------------
            elif text == "manage_shops" and role == "owner":
                tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                if not tenant_db:
                    send_message(chat_id, "❌ Unable to access store database.")
                    return {"ok": True}

                # Get current shops to show status
                shops = tenant_db.query(ShopORM).all()
                has_shops = len(shops) > 0
    
                # Create dynamic menu based on whether shops exist
                if not has_shops:
                    # No shops yet - setup first shop
                    kb_rows = [
                        [{"text": "🏪 Setup First Shop", "callback_data": "setup_first_shop"}],
                        [{"text": "⬅️ Back to Menu", "callback_data": "back_to_menu"}]
                    ]
                    message = "🏪 *Shop Management*\n\nNo shops configured yet. Set up your first shop!"
                else:
                    # Shops exist - show full management menu
                    kb_rows = [
                        [{"text": "🏪 Update Main Shop", "callback_data": "update_main_shop"}],
                        [{"text": "➕ Add New Shop", "callback_data": "add_new_shop"}],
                        [{"text": "📋 View All Shops", "callback_data": "view_all_shops"}],
                        [{"text": "📊 Manage Shop Stock", "callback_data": "manage_shop_stock"}],
                        [{"text": "🔄 Set Default Shop", "callback_data": "set_default_shop"}],
                        [{"text": "📈 Shop Reports", "callback_data": "shop_reports"}],
                        [{"text": "⬅️ Back to Menu", "callback_data": "back_to_menu"}]
                    ]
        
                    # Count shops and show status
                    main_shop = tenant_db.query(ShopORM).filter(ShopORM.is_main == True).first()
                    message = f"🏪 *Shop Management*\n\n"
                    message += f"📊 **Status:** {len(shops)} shop(s) configured\n"
                    if main_shop:
                        message += f"⭐ **Main Shop:** {main_shop.name}\n"
                    message += "\nSelect an option below:"

                send_message(chat_id, message, {"inline_keyboard": kb_rows})
                return {"ok": True}

            # -------------------- Setup First Shop (when no shops exist) --------------------
            elif text == "setup_first_shop" and role == "owner":
                send_message(chat_id, "🏪 Let's set up your first shop!\n\nEnter shop name:")
                user_states[chat_id] = {"action": "setup_shop", "step": 1, "data": {"is_first_shop": True}}
                return {"ok": True}

            # -------------------- Update Main Shop (when shops exist) --------------------
            elif text == "update_main_shop" and role == "owner":
                tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                if not tenant_db:
                    send_message(chat_id, "❌ Unable to access store database.")
                    return {"ok": True}
    
                # Get main shop
                main_shop = tenant_db.query(ShopORM).filter(ShopORM.is_main == True).first()
                if not main_shop:
                    # If no main shop, get first shop
                    main_shop = tenant_db.query(ShopORM).first()
    
                if main_shop:
                    # Pre-fill with existing data
                    user_states[chat_id] = {
                        "action": "update_existing_shop", 
                        "step": 1, 
                        "data": {
                            "shop_id": main_shop.shop_id,
                            "current_name": main_shop.name,
                            "current_location": main_shop.location or "",
                            "current_contact": main_shop.contact or ""
                        }
                    }
                    send_message(chat_id, f"✏️ Updating Main Shop: {main_shop.name}\n\nEnter NEW shop name (or '-' to keep current):")
                else:
                    send_message(chat_id, "❌ No shops found. Please set up your first shop.")
    
                return {"ok": True}
    
            # -------------------- Create Shopkeeper --------------------
            elif text == "create_shopkeeper":
                if role != "owner":
                    send_message(chat_id, "❌ Only owners can create shopkeepers.")
                    return {"ok": True}

                user_states[chat_id] = {"action": "create_shopkeeper", "step": 1, "data": {}}
                send_message(chat_id, "👤 Enter a username for the new shopkeeper:")
                return {"ok": True}

            # -------------------- Add Product --------------------
            elif text == "add_product":
                send_message(chat_id, "➕ Add a new product! 🛒\n\nEnter product name:")
                user_states[chat_id] = {"action": "awaiting_product", "step": 1, "data": {}}
                return {"ok": True}
        
            # -------------------- Approval Callbacks --------------------
            elif text.startswith("approve_action:"):
                try:
                    approval_id = int(text.split(":")[1])
                    # Handle approval logic
                    if handle_approval_action(chat_id, approval_id, "approved"):
                        send_message(chat_id, "✅ Action approved successfully!")
                    else:
                        send_message(chat_id, "❌ Failed to approve action.")
                except (ValueError, IndexError):
                    send_message(chat_id, "❌ Invalid approval action.")

            elif text.startswith("reject_action:"):
                try:
                    approval_id = int(text.split(":")[1])
                    # Handle rejection logic
                    if handle_approval_action(chat_id, approval_id, "rejected"):
                        send_message(chat_id, "❌ Action rejected.")
                    else:
                        send_message(chat_id, "❌ Failed to reject action.")
                except (ValueError, IndexError):
                    send_message(chat_id, "❌ Invalid rejection action.")

            elif text.startswith("view_approval:"):
                try:
                    approval_id = int(text.split(":")[1])
                    # Show approval details
                    show_approval_details(chat_id, approval_id)
                except (ValueError, IndexError):
                    send_message(chat_id, "❌ Invalid approval ID.")
                return {"ok": True}  # ← ADD THIS LINE
                
            
            # -------------------- Quick Stock Update --------------------
            elif text == "quick_stock_update":
                user_states[chat_id] = {"action": "quick_stock_update", "step": 1, "data": {}}
                send_message(chat_id, "🔍 Enter product name to search:")
                return {"ok": True}
                
            # -------------------- Quick Stock Update Callbacks --------------------
            elif text.startswith("select_stock_product:"):
                try:
                    product_id = int(text.split(":")[1])
        
                    current_state = user_states.get(chat_id, {})
                    current_data = current_state.get("data", {})
        
                    # Find the selected product from search results
                    selected_product = None
                    for product in current_data.get("search_results", []):
                        if product["product_id"] == product_id:
                            selected_product = product
                            break
        
                    if selected_product:
                        current_data["selected_product"] = selected_product
                        user_states[chat_id] = {"action": "quick_stock_update", "step": 2, "data": current_data}
            
                        send_message(chat_id, f"📦 Selected: {selected_product['name']}\nCurrent stock: {selected_product['current_stock']}\n\nEnter quantity to ADD to stock:")
                    else:
                        send_message(chat_id, "❌ Product selection failed. Please try again.")
                        user_states.pop(chat_id, None)
    
                except (ValueError, IndexError):
                    send_message(chat_id, "❌ Invalid product selection.")
                    user_states.pop(chat_id, None)
    
                return {"ok": True}

            elif text == "cancel_quick_stock":
                user_states.pop(chat_id, None)
                send_message(chat_id, "❌ Quick stock update cancelled.")
                kb = main_menu(user.role)
                send_message(chat_id, "🏠 Main Menu:", keyboard=kb)
                return {"ok": True}
                    
            elif text.startswith("approve_stock:"):
                try:
                    approval_id = int(text.split(":")[1])
                    if handle_stock_approval_action(chat_id, approval_id, "approved"):
                        send_message(chat_id, "✅ Stock update approved successfully!")
                    else:
                        send_message(chat_id, "❌ Failed to approve stock update.")
                except (ValueError, IndexError):
                    send_message(chat_id, "❌ Invalid stock approval action.")

            elif text.startswith("reject_stock:"):
                try:
                    approval_id = int(text.split(":")[1])
                    if handle_stock_approval_action(chat_id, approval_id, "rejected"):
                        send_message(chat_id, "❌ Stock update rejected.")
                    else:
                        send_message(chat_id, "❌ Failed to reject stock update.")
                except (ValueError, IndexError):
                    send_message(chat_id, "❌ Invalid stock rejection action.")
        
            elif text == "add_new_shop":
                if user.role != "owner":
                    send_message(chat_id, "❌ Only store owners can add shops.")
                    return {"ok": True}

                user_states[chat_id] = {"action": "add_shop", "step": 1, "data": {}}
                send_message(chat_id, "🏪 Enter name for new shop:")
                return {"ok": True}

            elif text == "view_all_shops":
                tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                if not tenant_db:
                    send_message(chat_id, "❌ Unable to access store database.")
                    return {"ok": True}

                shops = tenant_db.query(ShopORM).all()
                if not shops:
                    send_message(chat_id, "🏪 No shops found. Use 'Add New Shop' to create your first shop.")
                    return {"ok": True}

                shop_list = "🏪 *Your Shops:*\n\n"
                for shop in shops:
                    shop_list += f"• *{shop.name}*\n"
                    shop_list += f"  📍 {shop.location or 'No location'}\n"
                    shop_list += f"  📞 {shop.contact or 'No contact'}\n"
                    shop_list += f"  {'⭐ MAIN SHOP' if shop.is_main else ''}\n"
                    shop_list += f"  ID: {shop.shop_id}\n\n"

                # Add management buttons
                kb_rows = [
                    [{"text": "➕ Add Stock to Shop", "callback_data": "add_shop_stock"}],
                    [{"text": "📊 View Shop Stock", "callback_data": "view_shop_stock"}],
                    [{"text": "⬅️ Back", "callback_data": "manage_shops"}]
                ]
                send_message(chat_id, shop_list, {"inline_keyboard": kb_rows})
                return {"ok": True}

            # -------------------- Shop Stock Management --------------------
            elif text == "add_shop_stock":
                user_states[chat_id] = {"action": "add_shop_stock", "step": 1, "data": {}}
    
                # First, show shops to select
                tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                if not tenant_db:
                    send_message(chat_id, "❌ Unable to access store database.")
                    return {"ok": True}

                shops = tenant_db.query(ShopORM).all()
                if not shops:
                    send_message(chat_id, "❌ No shops found. Please add a shop first.")
                    return {"ok": True}

                kb_rows = []
                for shop in shops:
                    kb_rows.append([{"text": f"🏪 {shop.name}", "callback_data": f"select_shop_for_stock:{shop.shop_id}"}])
                kb_rows.append([{"text": "⬅️ Cancel", "callback_data": "view_all_shops"}])
    
                send_message(chat_id, "🏪 Select a shop to add stock:", {"inline_keyboard": kb_rows})
                return {"ok": True}

            elif text.startswith("select_shop_for_stock:"):
                try:
                    shop_id = int(text.split(":")[1])
        
                    current_state = user_states.get(chat_id, {})
                    current_data = current_state.get("data", {})
                    current_data["selected_shop_id"] = shop_id
        
                    user_states[chat_id] = {"action": "add_shop_stock", "step": 2, "data": current_data}
                    send_message(chat_id, "📦 Enter product name to search:")
        
                except (ValueError, IndexError):
                    send_message(chat_id, "❌ Invalid shop selection.")
    
                return {"ok": True}

            elif text == "view_shop_stock":
                tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                if not tenant_db:
                    send_message(chat_id, "❌ Unable to access store database.")
                    return {"ok": True}

                # Show shops to select
                shops = tenant_db.query(ShopORM).all()
                if not shops:
                    send_message(chat_id, "❌ No shops found.")
                    return {"ok": True}

                kb_rows = []
                for shop in shops:
                    kb_rows.append([{"text": f"📊 {shop.name} Stock", "callback_data": f"view_stock_for_shop:{shop.shop_id}"}])
                kb_rows.append([{"text": "⬅️ Back", "callback_data": "view_all_shops"}])
    
                send_message(chat_id, "🏪 Select a shop to view stock:", {"inline_keyboard": kb_rows})
                return {"ok": True}

            elif text.startswith("view_stock_for_shop:"):
                try:
                    shop_id = int(text.split(":")[1])
        
                    tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                    if not tenant_db:
                        send_message(chat_id, "❌ Unable to access store database.")
                        return {"ok": True}

                    # Get shop info
                    shop = tenant_db.query(ShopORM).filter(ShopORM.shop_id == shop_id).first()
                    if not shop:
                        send_message(chat_id, "❌ Shop not found.")
                        return {"ok": True}

                    # Get stock for this shop
                    stock_items = tenant_db.query(ProductShopStockORM).filter(
                        ProductShopStockORM.shop_id == shop_id
                    ).all()

                    if not stock_items:
                        message = f"🏪 *{shop.name}*\n\n"
                        message += "📦 No stock assigned to this shop yet.\n"
                        message += "Use 'Add Stock to Shop' to assign products."
                    else:
                        message = f"🏪 *{shop.name} - Stock Report*\n\n"
                        for item in stock_items:
                            product = tenant_db.query(ProductORM).filter(
                                ProductORM.product_id == item.product_id
                            ).first()
                
                            if product:
                                status = "🟢" if item.stock > item.low_stock_threshold else "🔴" if item.stock == 0 else "🟡"
                                message += f"{status} *{product.name}*\n"
                                message += f"  📊 Stock: {item.stock} {product.unit_type}\n"
                                message += f"  ⚠️ Low Stock Alert: {item.low_stock_threshold}\n"
                                message += f"  📦 Min Stock: {item.min_stock_level}\n"
                                if item.stock <= item.low_stock_threshold:
                                    message += f"  ⚠️ *LOW STOCK!*\n"
                                message += "\n"

                    kb_rows = [
                        [{"text": "➕ Add More Stock", "callback_data": f"add_stock_to_existing_shop:{shop_id}"}],
                        [{"text": "📈 Update Stock", "callback_data": f"update_shop_stock:{shop_id}"}],
                        [{"text": "⬅️ Back to Shops", "callback_data": "view_all_shops"}]
                    ]
        
                    send_message(chat_id, message, {"inline_keyboard": kb_rows})
        
                except (ValueError, IndexError):
                    send_message(chat_id, "❌ Invalid shop selection.")
    
                return {"ok": True}

            # -------------------- Update Product --------------------
            elif text == "update_product":
                tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                if not tenant_db:
                    send_message(chat_id, "⚠️ Tenant database not linked. Please restart with /start.")
                    return {"ok": True}

                logger.debug(f"🧩 In update_product flow, tenant_db ready for chat_id={chat_id}")
                user_states[chat_id] = {"action": "awaiting_update", "step": 1, "data": {}}
                send_message(chat_id, "✏️ Enter the product name to update:")
                return {"ok": True}

            # -------------------- Paginated Product List --------------------
            elif text.startswith("products_page:"):
                try:
                    page = int(text.split(":")[1])
                except (IndexError, ValueError):
                    page = 1

                tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                if not tenant_db:
                    send_message(chat_id, "⚠️ Tenant database not linked. Please restart with /start.")
                    return {"ok": True}

                text_msg, kb = products_page_view(tenant_db, page=page)
                send_message(chat_id, text_msg, kb)
                return {"ok": True}

            # -------------------- Product Selection for Update --------------------
            elif text.startswith("select_update:"):
                logger.info(f"🧩 Processing select_update callback: {text}")
                
                # Extract product ID
                try:
                    product_id = int(text.split(":")[1])
                except (IndexError, ValueError):
                    send_message(chat_id, "⚠️ Invalid product selection.")
                    return {"ok": True}

                # Create tenant session
                tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                if not tenant_db:
                    send_message(chat_id, "❌ Unable to access tenant database.")
                    return {"ok": True}

                # Fetch product
                product = tenant_db.query(ProductORM).filter(ProductORM.product_id == product_id).first()
                
                if not product:
                    logger.error(f"❌ Product {product_id} not found in callback")
                    send_message(chat_id, f"❌ Product ID {product_id} not found.")
                    return {"ok": True}

                # Start update flow
                user_states[chat_id] = {
                    "action": "awaiting_update",
                    "step": 2,
                    "data": {"product_id": product_id}
                }

                text_msg = (
                    f"✏️ Updating: {product.name}\n\n"
                    f"💰 Price: ${product.price}\n"
                    f"📦 Stock: {product.stock} {product.unit_type}\n\n"
                    "Enter NEW NAME (or '-' to keep current):"
                )

                send_message(chat_id, text_msg)
                return {"ok": True}

            # -------------------- Record Sale --------------------
            elif text == "record_sale":
                tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                if not tenant_db:
                    send_message(chat_id, "⚠️ Cannot record sale: tenant DB unavailable.")
                    return {"ok": True}

                # Get available shops
                shops = tenant_db.query(ShopORM).all()
    
                if not shops:
                    send_message(chat_id, "❌ No shops found. Please set up shops first in 'Manage Shops'.")
                    return {"ok": True}
    
                if len(shops) == 1:
                    # Only one shop - use it automatically
                    user_states[chat_id] = {
                        "action": "awaiting_sale", 
                        "step": 1, 
                        "data": {"selected_shop_id": shops[0].shop_id}
                    }
                    send_message(chat_id, f"🏪 Shop: {shops[0].name}\n💰 Record a new sale!\nEnter product name:")
                else:
                    # Multiple shops - ask user to select
                    kb_rows = []
                    for shop in shops:
                        kb_rows.append([{
                            "text": f"🏪 {shop.name} {'⭐' if shop.is_main else ''}",
                            "callback_data": f"select_shop_for_sale:{shop.shop_id}"
                        }])
        
                    send_message(chat_id, "🏪 Select shop for sale:", {"inline_keyboard": kb_rows})
                return {"ok": True}

            # -------------------- Shop Selection for Sale --------------------
            elif text.startswith("select_shop_for_sale:"):
                try:
                    shop_id = int(text.split(":")[1])
        
                    tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                    if not tenant_db:
                        send_message(chat_id, "❌ Unable to access store database.")
                        return {"ok": True}

                    shop = tenant_db.query(ShopORM).filter(ShopORM.shop_id == shop_id).first()
                    if not shop:
                        send_message(chat_id, "❌ Shop not found.")
                        return {"ok": True}

                    user_states[chat_id] = {"action": "awaiting_sale", "step": 1, "data": {"selected_shop_id": shop_id}}
                    send_message(chat_id, f"🏪 Shop: {shop.name}\n💰 Record a new sale!\nEnter product name:")
        
                except (ValueError, IndexError):
                    send_message(chat_id, "❌ Invalid shop selection.")
    
                return {"ok": True}
    
            # -------------------- Product Selection for Sale --------------------
            elif text.startswith("select_sale:"):
                try:
                    product_id = int(text.split(":")[1])
        
                    # ✅ CRITICAL: Get current state to preserve cart
                    current_state = user_states.get(chat_id, {})
                    current_data = current_state.get("data", {})
        
                    # Debug logging
                    logger.info(f"🔍 CART DEBUG [select_sale] - Chat: {chat_id}, Items: {len(current_data.get('cart', []))}")
        
                    # Ensure tenant session is available
                    tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                    if tenant_db is None:
                        send_message(chat_id, "❌ Unable to access tenant database.")
                        return {"ok": True}
        
                    # Find the selected product
                    product = tenant_db.query(ProductORM).filter(ProductORM.product_id == product_id).first()
                    if not product:
                        send_message(chat_id, "❌ Product not found. Please try again.")
                        return {"ok": True}
        
                    # Store selected product and preserve existing cart
                    current_data["current_product"] = {
                        "product_id": product.product_id,
                        "name": product.name,
                        "price": float(product.price),
                        "unit_type": product.unit_type,
                        "available_stock": product.stock
                    }
        
                    # Update state with preserved cart and new product
                    user_states[chat_id] = {
                        "action": "awaiting_sale", 
                        "step": 2, 
                        "data": current_data  # This preserves the cart!
                    }
        
                    send_message(chat_id, f"📦 Selected {product.name} ({product.unit_type}). Enter quantity to add:")
        
                except (ValueError, IndexError):
                    send_message(chat_id, "❌ Invalid product selection.")
    
                return {"ok": True}
        
            # -------------------- Cart Management Callbacks --------------------
            elif text == "add_another_item":
                logger.info(f"🎯 Processing callback: add_another_item from chat_id={chat_id}")
    
                # ✅ FIX: Get current state from user_states, not callback data
                current_state = user_states.get(chat_id, {})
                current_data = current_state.get("data", {})
    
                logger.info(f"🔍 CART DEBUG [add_another_item] - Chat: {chat_id}, Items: {len(current_data.get('cart', []))}")
    
                # Preserve existing cart and data
                user_states[chat_id] = {
                    "action": "awaiting_sale", 
                    "step": 1, 
                    "data": current_data  # This preserves the cart!
                }
                send_message(chat_id, "➕ Add another item. Enter product name:")
                return {"ok": True}
    
            elif text == "view_cart":
                # ✅ FIX: Get cart from current state, not callback data
                current_state = user_states.get(chat_id, {})
                current_data = current_state.get("data", {})
                cart = current_data.get("cart", [])
    
                logger.info(f"🔍 CART DEBUG [view_cart] - Chat: {chat_id}, Items: {len(cart)}")
    
                cart_summary = get_cart_summary(cart)
                kb_rows = [
                    [{"text": "➕ Add Item", "callback_data": "add_another_item"}],
                    [{"text": "🗑 Remove Item", "callback_data": "remove_item"}],
                    [{"text": "✅ Checkout", "callback_data": "checkout_cart"}],
                    [{"text": "❌ Cancel Sale", "callback_data": "cancel_sale"}]
                ]
                send_message(chat_id, cart_summary, {"inline_keyboard": kb_rows})
                return {"ok": True}
    
            elif text == "remove_item":
                # ✅ FIX: Get cart from current state
                current_state = user_states.get(chat_id, {})
                current_data = current_state.get("data", {})
                cart = current_data.get("cart", [])
    
                logger.info(f"🔍 CART DEBUG [remove_item] - Chat: {chat_id}, Items: {len(cart)}")
    
                if not cart:
                    send_message(chat_id, "🛒 Cart is empty. Add items first.")
                    return {"ok": True}
    
                kb_rows = []
                for i, item in enumerate(cart, 1):
                    kb_rows.append([{"text": f"Remove: {item['name']} ({item['quantity']})", "callback_data": f"remove_cart_item:{i-1}"}])
                kb_rows.append([{"text": "⬅️ Back to Cart", "callback_data": "view_cart"}])
    
                send_message(chat_id, "🗑 Select item to remove:", {"inline_keyboard": kb_rows})
                return {"ok": True}

            elif text == "checkout_cart":
                logger.info(f"🎯 Processing callback: checkout_cart from chat_id={chat_id}")
    
                # ✅ FIX: Get cart from current state
                current_state = user_states.get(chat_id, {})
                current_data = current_state.get("data", {})
                cart = current_data.get("cart", [])
    
                logger.info(f"🔍 CART DEBUG [checkout_cart] - Chat: {chat_id}, Items: {len(cart)}")
    
                if not cart:
                    send_message(chat_id, "❌ Cart is empty! Add items first.")
                    return {"ok": True}
    
                # Move to checkout step
                user_states[chat_id] = {
                    "action": "awaiting_sale", 
                    "step": 3, 
                    "data": current_data  # Preserve cart for checkout
                }
    
                # Show payment options - UPDATED: Cash, Ecocash, Swipe
                kb_rows = [
                    [{"text": "💵 Cash", "callback_data": "payment_method:cash"}],
                    [{"text": "📱 Ecocash", "callback_data": "payment_method:ecocash"}],
                    [{"text": "💳 Swipe", "callback_data": "payment_method:swipe"}],
                    [{"text": "⬅️ Back to Cart", "callback_data": "view_cart"}]
                ]
    
                cart_summary = get_cart_summary(cart)
                total = sum(item["subtotal"] for item in cart)
                message = f"🛒 Checkout\n\n{cart_summary}\n💰 Total: ${total:.2f}\n\n💳 Select payment method:"
    
                send_message(chat_id, message, {"inline_keyboard": kb_rows})
                return {"ok": True}

            elif text == "cancel_sale":
                logger.info(f"🎯 Processing callback: cancel_sale from chat_id={chat_id}")
    
                current_state = user_states.get(chat_id, {})
                current_data = current_state.get("data", {})
                cart = current_data.get("cart", [])
                logger.info(f"🔍 CART DEBUG [cancel_sale] - Chat: {chat_id}, Items: {len(cart)}")
    
                user_states.pop(chat_id, None)
                send_message(chat_id, "❌ Sale cancelled.")
                kb = main_menu(user.role)
                send_message(chat_id, "🏠 Main Menu:", keyboard=kb)
                return {"ok": True}

            # Handle remove cart item callbacks
            elif text.startswith("remove_cart_item:"):
                # ✅ FIX: Get cart from current state
                current_state = user_states.get(chat_id, {})
                current_data = current_state.get("data", {})
                cart = current_data.get("cart", [])
    
                logger.info(f"🔍 CART DEBUG [before_remove] - Chat: {chat_id}, Items: {len(cart)}")
    
                try:
                    index = int(text.split(":")[1])
                    if 0 <= index < len(cart):
                        removed_item = cart.pop(index)
            
                        # Update the state with modified cart
                        user_states[chat_id] = {
                            "action": "awaiting_sale",
                            "step": 1, 
                            "data": current_data
                        }
            
                        logger.info(f"🔍 CART DEBUG [after_remove] - Chat: {chat_id}, Items: {len(cart)}")
            
                        send_message(chat_id, f"✅ Removed: {removed_item['name']}")
            
                        # Show updated cart
                        cart_summary = get_cart_summary(cart)
                        kb_rows = [
                            [{"text": "➕ Add Item", "callback_data": "add_another_item"}],
                            [{"text": "🗑 Remove Item", "callback_data": "remove_item"}],
                            [{"text": "✅ Checkout", "callback_data": "checkout_cart"}],
                            [{"text": "❌ Cancel Sale", "callback_data": "cancel_sale"}]
                        ]
                        send_message(chat_id, cart_summary, {"inline_keyboard": kb_rows})
                    else:
                        send_message(chat_id, "❌ Invalid item selection.")
                except (ValueError, IndexError):
                    send_message(chat_id, "❌ Error removing item.")
                return {"ok": True}

            # ✅ NEW: Handle product selection from multiple matches
            elif text.startswith("select_sale:"):
                try:
                    product_id = int(text.split(":")[1])
        
                    # ✅ CRITICAL: Get current state to preserve cart
                    current_state = user_states.get(chat_id, {})
                    current_data = current_state.get("data", {})
        
                    logger.info(f"🔍 CART DEBUG [select_sale] - Chat: {chat_id}, Items: {len(current_data.get('cart', []))}")
        
                    # Ensure tenant session is available
                    tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                    if tenant_db is None:
                        send_message(chat_id, "❌ Unable to access tenant database.")
                        return {"ok": True}
        
                    # Find the selected product
                    product = tenant_db.query(ProductORM).filter(ProductORM.product_id == product_id).first()
                    if not product:
                        send_message(chat_id, "❌ Product not found. Please try again.")
                        return {"ok": True}
        
                    # Store selected product and preserve existing cart
                    current_data["current_product"] = {
                        "product_id": product.product_id,
                        "name": product.name,
                        "price": float(product.price),
                        "unit_type": product.unit_type,
                        "available_stock": product.stock
                    }
        
                    # Update state with preserved cart and new product
                    user_states[chat_id] = {
                        "action": "awaiting_sale", 
                        "step": 2, 
                        "data": current_data  # This preserves the cart!
                    }
        
                    send_message(chat_id, f"📦 Selected {product.name} ({product.unit_type}). Enter quantity to add:")
        
                except (ValueError, IndexError):
                    send_message(chat_id, "❌ Invalid product selection.")
    
                return {"ok": True}

            # ✅ UPDATED: Payment method selection with Ecocash surcharge
            elif text.startswith("payment_method:"):
                payment_method = text.split(":")[1]
    
                # Get current state
                current_state = user_states.get(chat_id, {})
                current_data = current_state.get("data", {})
    
                logger.info(f"🔍 CART DEBUG [payment_method] - Chat: {chat_id}, Items: {len(current_data.get('cart', []))}, Method: {payment_method}")
    
                current_data["payment_method"] = payment_method
    
                # Calculate cart total
                cart_total = sum(item["subtotal"] for item in current_data["cart"])
    
                if payment_method == "cash":
                    # For cash, ask for sale type (cash/credit)
                    user_states[chat_id] = {
                        "action": "awaiting_sale", 
                        "step": 3.1, 
                        "data": current_data
                    }
        
                    kb_rows = [
                        [{"text": "💵 Cash Sale", "callback_data": "sale_type:cash"}],
                        [{"text": "🔄 Credit Sale", "callback_data": "sale_type:credit"}],
                        [{"text": "⬅️ Back", "callback_data": "view_cart"}]
                    ]
        
                    send_message(chat_id, f"💰 Cart Total: ${cart_total:.2f}\n\n💳 Select sale type:", {"inline_keyboard": kb_rows})
    
                elif payment_method == "ecocash":
                    # ✅ Apply 10% surcharge for Ecocash
                    surcharge = cart_total * 0.10
                    final_total = cart_total + surcharge
                    current_data["original_total"] = cart_total  # Store original for receipt
                    current_data["surcharge"] = surcharge
                    current_data["final_total"] = final_total
        
                    # For Ecocash, it's always full payment with surcharge
                    current_data["sale_type"] = "cash"
                    current_data["payment_type"] = "full"
                    current_data["amount_paid"] = final_total
                    current_data["pending_amount"] = 0
                    current_data["change_left"] = 0
        
                    user_states[chat_id] = {
                        "action": "awaiting_sale", 
                        "step": 6, 
                        "data": current_data
                    }
        
                    # Show surcharge breakdown
                    message = f"📱 *Ecocash Payment*\n\n"
                    message += get_cart_summary(current_data["cart"])
                    message += f"💰 Subtotal: ${cart_total:.2f}\n"
                    message += f"⚡ Surcharge (10%): ${surcharge:.2f}\n"
                    message += f"💳 *Final Amount: ${final_total:.2f}*\n\n"
                    message += "✅ Ecocash payment confirmed.\n\nConfirm sale? (yes/no)"
        
                    send_message(chat_id, message)
    
                else:  # swipe
                    # For Swipe, it's always full payment (no surcharge)
                    current_data["sale_type"] = "cash"
                    current_data["payment_type"] = "full"
                    current_data["amount_paid"] = cart_total
                    current_data["pending_amount"] = 0
                    current_data["change_left"] = 0
        
                    user_states[chat_id] = {
                        "action": "awaiting_sale", 
                        "step": 6, 
                        "data": current_data
                    }
                    send_message(chat_id, f"💰 Cart Total: ${cart_total:.2f}\n✅ {payment_method.title()} payment confirmed.\n\nConfirm sale? (yes/no)")
    
                return {"ok": True}
    
            # ✅ NEW: Sale type selection for cash
            elif text.startswith("sale_type:"):
                sale_type = text.split(":")[1]
    
                current_state = user_states.get(chat_id, {})
                current_data = current_state.get("data", {})
    
                current_data["sale_type"] = sale_type
    
                if sale_type == "cash":
                    # For cash sales, ask for amount tendered
                    current_data["payment_type"] = "full"
                    user_states[chat_id] = {
                        "action": "awaiting_sale", 
                        "step": 4, 
                        "data": current_data
                    }
        
                    cart_total = sum(item["subtotal"] for item in current_data["cart"])
                    send_message(chat_id, f"💰 Cart Total: ${cart_total:.2f}\n💵 Enter cash amount tendered by customer:")
    
                else:  # credit
                    # For credit sales, ask for credit type
                    user_states[chat_id] = {
                        "action": "awaiting_sale", 
                        "step": 3.2, 
                        "data": current_data
                    }
        
                    kb_rows = [
                        [{"text": "💰 Full Credit", "callback_data": "credit_type:full"}],
                        [{"text": "📋 Partial Credit", "callback_data": "credit_type:partial"}],
                        [{"text": "⬅️ Back", "callback_data": "view_cart"}]
                    ]
        
                    cart_total = sum(item["subtotal"] for item in current_data["cart"])
                    send_message(chat_id, f"💰 Cart Total: ${cart_total:.2f}\n\n💳 Select credit type:", {"inline_keyboard": kb_rows})
    
                return {"ok": True}

            # ✅ NEW: Credit type selection
            elif text.startswith("credit_type:"):
                credit_type = text.split(":")[1]
    
                current_state = user_states.get(chat_id, {})
                current_data = current_state.get("data", {})
    
                current_data["payment_type"] = credit_type
    
                if credit_type == "full":
                    # Full credit - no payment, go to customer details
                    current_data["amount_paid"] = 0
                    current_data["pending_amount"] = sum(item["subtotal"] for item in current_data["cart"])
                    current_data["change_left"] = 0
        
                    user_states[chat_id] = {
                        "action": "awaiting_sale", 
                        "step": 5, 
                        "data": current_data
                    }
                    send_message(chat_id, "🔄 Full credit sale.\n👤 Enter customer name for credit follow-up:")
    
                else:  # partial
                    # Partial credit - ask for amount paid
                    user_states[chat_id] = {
                        "action": "awaiting_sale", 
                        "step": 4, 
                        "data": current_data
                    }
        
                    cart_total = sum(item["subtotal"] for item in current_data["cart"])
                    send_message(chat_id, f"💰 Cart Total: ${cart_total:.2f}\n💵 Enter amount paid now (remaining will be credit):")
    
                return {"ok": True}

            # ✅ NEW: Change availability check
            elif text.startswith("has_change:"):
                has_change = text.split(":")[1]
    
                current_state = user_states.get(chat_id, {})
                current_data = current_state.get("data", {})
    
                if has_change == "yes":
                    # Has change - no customer details needed
                    user_states[chat_id] = {
                        "action": "awaiting_sale", 
                        "step": 6, 
                        "data": current_data
                    }
                    send_message(chat_id, "✅ Change ready. Confirm sale? (yes/no)")
                else:
                    # No change - need customer details
                    user_states[chat_id] = {
                        "action": "awaiting_sale", 
                        "step": 5, 
                        "data": current_data
                    }
                    send_message(chat_id, "👤 Enter customer name (for change follow-up):")
    
                return {"ok": True}
                    
            # -------------------- View Stock --------------------
            elif text == "view_stock":
                # 🔍 ADD DEBUG INFO
                print(f"🔍 VIEW STOCK DEBUG: User {user.username} (role: {user.role})")
                print(f"🔍 VIEW STOCK DEBUG: Tenant schema: {user.tenant_schema}")
                print(f"🔍 VIEW STOCK DEBUG: Chat ID: {chat_id}")
    
                tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                if not tenant_db:
                    print(f"❌ VIEW STOCK DEBUG: No tenant DB for schema: {user.tenant_schema}")
                    send_message(chat_id, "⚠️ Cannot view stock: tenant DB unavailable.")
                    return {"ok": True}
    
                # 🔍 Check if products exist
                product_count = tenant_db.query(ProductORM).count()
                print(f"🔍 VIEW STOCK DEBUG: Found {product_count} products in schema {user.tenant_schema}")
    
                stock_list = get_stock_list(tenant_db)
                kb_dict = {"inline_keyboard": [[{"text": "⬅️ Back to Menu", "callback_data": "back_to_menu"}]]}
                send_message(chat_id, stock_list, kb_dict)
                return {"ok": True}
    
            # -------------------- Reports Menu --------------------
            elif text == "report_menu":
                kb_dict = report_menu_keyboard(role)
                send_message(chat_id, "📊 Select a report:", kb_dict)
                return {"ok": True}

            # -------------------- Report Callbacks --------------------
            elif text in ["report_daily", "report_weekly", "report_monthly", "report_low_stock", 
                          "report_top_products", "report_aov", "report_stock_turnover", 
                          "report_credits", "report_change"]:
    
                logger.info(f"🎯 Processing callback: {text} from chat_id={chat_id}")
    
                tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                if tenant_db is None:
                    send_message(chat_id, "❌ Unable to access tenant database.")
                    return {"ok": True}
    
                try:
                    # Use your existing generate_report function
                    report = generate_report(tenant_db, text)
                    send_message(chat_id, report)
                except Exception as e:
                    logger.error(f"❌ {text} failed: {e}")
                    send_message(chat_id, f"❌ Failed to generate {text.replace('_', ' ')}.")
    
                return {"ok": True}

            # Handle back to menu
            elif text == "back_to_menu":
                logger.info(f"🎯 Processing callback: back_to_menu from chat_id={chat_id}")
    
                kb = main_menu(user.role)
                send_message(chat_id, "🏠 Main Menu:", keyboard=kb)
                return {"ok": True}
    
            # -------------------- Help --------------------
            elif text == "help":
                help_text = (
                    "❓ *Help & FAQs*\n\n"
                    "📌 *Getting Started*\n"
                    "• Owners: setup shop and add products.\n"
                    "• Shopkeepers: record sales, check stock.\n\n"
                    "🛒 *Managing Products*\n"
                    "• Owners can add/update all product fields.\n"
                    "• Shopkeepers can suggest new products or update quantity/unit only.\n\n"
                    "📦 *Stock Management*\n"
                    "• Check View Stock before recording sales.\n"
                    "• Low stock alerts will appear automatically to owners.\n\n"
                    "📊 *Reports*\n"
                    "• Owners: full reports\n"
                    "• Shopkeepers: limited access\n\n"
                    "⚠️ *Common Issues*\n"
                    "• Bot unresponsive → /start\n"
                    "• Always follow input formats.\n\n"
                    "👨‍💻 Contact support for more help."
                )
                kb_dict = {"inline_keyboard": [[{"text": "⬅️ Back to Menu", "callback_data": "back_to_menu"}]]}
                send_message(chat_id, help_text, kb_dict)
                return {"ok": True}

            else:
                logger.warning(f"⚠️ Unknown callback action received: {text}")
                send_message(chat_id, f"⚠️ Unknown action: {text}")
                return {"ok": True}

        # ✅ Only process messages if it's not a callback
        elif update_type == "message":
            # =====================================================
            # COPY ALL YOUR EXISTING MESSAGE HANDLING CODE HERE
            # This includes:
            # - /start command logic
            # - Login flow (user_states handling)
            # - Shop setup flow  
            # - Add product flow
            # - Update product flow (awaiting_update steps)
            # - Record sale flow (awaiting_sale steps)
            # =====================================================
            
            # -------------------- /start --------------------
            if text == "/start":
                user = db.query(User).filter(User.chat_id == chat_id).first()

                if user:
                    # ✅ EXISTING USER - Direct to password prompt
                    if user.role == "owner":
                        send_message(chat_id, "🔐 Welcome back, Owner! Please enter your password:")
                    else:  # shopkeeper
                        send_message(chat_id, "🔐 Welcome back! Please enter your password:")
        
                    user_states[chat_id] = {"action": "login", "step": 1, "data": {}}

                    # Ensure tenant schema for owners
                    if user.role == "owner":
                        try:
                            schema_name = f"tenant_{chat_id}"
                            tenant_db_url = create_tenant_db(chat_id)
                            user.tenant_schema = schema_name
                            db.commit()
                            logger.info(f"✅ Tenant schema ensured for {user.username}: {schema_name}")
                        except Exception as e:
                            logger.error(f"❌ Failed to ensure tenant schema for {user.username}: {e}")

                else:
                    # ✅ NEW USER - Ask for role selection
                    kb_rows = [
                        [{"text": "🏪 I'm a Shop Owner", "callback_data": "user_type:owner"}],
                        [{"text": "👤 I'm a Shopkeeper", "callback_data": "user_type:shopkeeper"}]
                    ]
                    send_message(chat_id, "👋 Welcome! Please select your role:", {"inline_keyboard": kb_rows})

                return {"ok": True}

            # -------------------- User Type Selection Callback --------------------
            elif text.startswith("user_type:"):
                user_type = text.split(":")[1]
    
                if user_type == "owner":
                    # Create new owner with generated credentials
                    generated_username = create_username(f"Owner{chat_id}")
                    generated_password = generate_password()
                    generated_email = f"{chat_id}_{int(time.time())}@example.com"

                    new_user = User(
                        name=f"Owner{chat_id}",
                        username=generated_username,
                        email=generated_email,
                        password_hash=hash_password(generated_password),
                        chat_id=chat_id,
                        role="owner"
                    )
                    db.add(new_user)
                    db.commit()
                    db.refresh(new_user)

                    # Create tenant schema
                    try:
                        schema_name = f"tenant_{chat_id}"
                        tenant_db_url = create_tenant_db(chat_id)
                        new_user.tenant_schema = schema_name
                        db.commit()
                        logger.info(f"✅ New owner created: {generated_username} with schema '{schema_name}'")
                    except Exception as e:
                        logger.error(f"❌ Failed to create tenant schema: {e}")
                        send_message(chat_id, "❌ Could not initialize store database.")
                        return {"ok": True}

                    # Send credentials and start shop setup
                    send_owner_credentials(chat_id, generated_username, generated_password)
                    send_message(chat_id, "🏪 Let's set up your shop! Please enter the shop name:")
                    user_states[chat_id] = {"action": "setup_shop", "step": 1, "data": {}}

                else:  # shopkeeper
                    # Step-by-step shopkeeper login
                    send_message(chat_id, "👤 Please enter your username:")
                    user_states[chat_id] = {"action": "shopkeeper_login", "step": 1, "data": {}}
    
                return {"ok": True}

            # -------------------- Login Flow --------------------
            if chat_id in user_states:
                state = user_states[chat_id]
                action = state.get("action")
                step = state.get("step", 1)
                data = state.get("data", {})

                # ✅ REGULAR LOGIN (for existing users - both owners and shopkeepers)
                if action == "login" and step == 1:
                    entered_password = text.strip()
                    user = db.query(User).filter(User.chat_id == chat_id).first()

                    if not user:
                        send_message(chat_id, "❌ User not found. Please try /start again.")
                        user_states.pop(chat_id, None)
                        return {"ok": True}

                    # Verify password
                    if not verify_password(entered_password, user.password_hash):
                        send_message(chat_id, "❌ Incorrect password. Please try again:")
                        return {"ok": True}

                    # ✅ Login successful - NO schema changes for anyone
                    send_message(chat_id, f"✅ Login successful! Welcome, {user.name}.")
                    user_states.pop(chat_id, None)

                    # Verify tenant connection using EXISTING schema
                    try:
                        if not user.tenant_schema:
                            send_message(chat_id, "❌ User not properly linked to a store. Contact support.")
                            return {"ok": True}
            
                        tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                        if tenant_db is None:
                            logger.error(f"❌ Tenant DB connection failed for {user.username}")
                            send_message(chat_id, "❌ Unable to access store database. Please contact support.")
                            return {"ok": True}
        
                        logger.info(f"✅ Tenant DB connection successful for {user.username}")

                    except Exception as e:
                        logger.error(f"❌ Tenant setup failed for {user.username}: {e}")
                        send_message(chat_id, "❌ Database initialization failed. Please contact support.")
                        return {"ok": True}

                    # Show appropriate menu
                    kb = main_menu(user.role)
                    send_message(chat_id, "🏠 Main Menu:", keyboard=kb)
                    return {"ok": True}
    
                # ✅ SHOPKEEPER LOGIN (for new shopkeepers - first time linking chat_id)
                elif action == "shopkeeper_login":
                    if step == 1:  # Enter Username
                        username = text.strip()
                        if not username:
                            send_message(chat_id, "❌ Username cannot be empty. Please enter your username:")
                            return {"ok": True}
            
                        # Check if username exists and is a shopkeeper
                        candidate = db.query(User).filter(User.username == username, User.role == "shopkeeper").first()
            
                        if not candidate:
                            send_message(chat_id, "❌ Username not found or not a shopkeeper. Please try again:")
                            return {"ok": True}
            
                        # Store username and move to password step
                        data["username"] = username
                        data["candidate_user_id"] = candidate.user_id
                        user_states[chat_id] = {"action": "shopkeeper_login", "step": 2, "data": data}
                        send_message(chat_id, "🔐 Please enter your password:")
            
                    # (password verification)
                    elif step == 2:  # Enter Password
                        password = text.strip()
                        if not password:
                            send_message(chat_id, "❌ Password cannot be empty. Please enter your password:")
                            return {"ok": True}
    
                        # Get the candidate user
                        candidate = db.query(User).filter(User.user_id == data["candidate_user_id"]).first()
    
                        if not candidate:
                            send_message(chat_id, "❌ User not found. Please start over with /start")
                            user_states.pop(chat_id, None)
                            return {"ok": True}
    
                        # Verify password
                        if not verify_password(password, candidate.password_hash):
                            send_message(chat_id, "❌ Incorrect password. Please try again:")
                            return {"ok": True}
    
                        # ✅ ONLY update chat_id - preserve everything else
                        candidate.chat_id = chat_id
                        db.commit()
    
                        send_message(chat_id, f"✅ Login successful! Welcome, {candidate.name}.")
                        user_states.pop(chat_id, None)
    
                        # Verify tenant connection using EXISTING tenant_schema
                        try:
                            if not candidate.tenant_schema:
                                send_message(chat_id, "❌ Shopkeeper not properly linked to a store. Contact the owner.")
                                return {"ok": True}
            
                            tenant_db = get_tenant_session(candidate.tenant_schema, chat_id)
                            if tenant_db is None:
                                send_message(chat_id, "❌ Unable to access store database. Contact the store owner.")
                                return {"ok": True}
                        except Exception as e:
                            logger.error(f"❌ Tenant connection failed: {e}")
                            send_message(chat_id, "❌ Database access failed. Contact the store owner.")
                            return {"ok": True}
    
                        # Show shopkeeper menu
                        kb = main_menu(candidate.role)
                        send_message(chat_id, "🏠 Shopkeeper Menu:", keyboard=kb)
            
                    return {"ok": True}
                
                # -------------------- Unified Shop Setup/Update (Owner only) --------------------
                elif action == "setup_shop" and user.role == "owner":
                    if step == 1:  # Shop Name
                        shop_name = text.strip()
                        if not shop_name:
                            send_message(chat_id, "❌ Shop name cannot be empty. Please enter your shop name:")
                            return {"ok": True}
                        data["name"] = shop_name
                        user_states[chat_id] = {"action": action, "step": 2, "data": data}
                        send_message(chat_id, "📍 Now enter the shop location:")

                    elif step == 2:  # Shop Location
                        location = text.strip()
                        if location:
                            data["location"] = location
                        user_states[chat_id] = {"action": action, "step": 3, "data": data}
                        send_message(chat_id, "📞 Enter the shop contact number (optional):")

                    elif step == 3:  # Shop Contact (optional)
                        contact = text.strip()
                        if contact:
                            data["contact"] = contact

                        # Save the shop
                        tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                        if not tenant_db:
                            send_message(chat_id, "❌ Unable to access database.")
                            user_states.pop(chat_id, None)
                            return {"ok": True}

                        try:
                            # Check if this is the first shop
                            existing_shops = tenant_db.query(ShopORM).count()
                            is_main = existing_shops == 0 or data.get("is_first_shop", False)

                            new_shop = ShopORM(
                                name=data["name"],
                                location=data.get("location", ""),
                                contact=data.get("contact", ""),
                                is_main=is_main
                            )
                            tenant_db.add(new_shop)
                            tenant_db.commit()
                            tenant_db.refresh(new_shop)

                            success_msg = f"✅ Shop {'created' if is_main else 'added'} successfully!\n\n"
                            success_msg += f"🏪 *{new_shop.name}*\n"
                            if data.get("location"):
                                success_msg += f"📍 {data['location']}\n"
                            if data.get("contact"):
                                success_msg += f"📞 {data['contact']}\n"
                            if new_shop.is_main:
                                success_msg += f"⭐ *Set as Main Store*\n"

                            send_message(chat_id, success_msg)

                        except Exception as e:
                            logger.error(f"❌ Error saving shop: {e}")
                            send_message(chat_id, "❌ Failed to save shop. Please try again.")

                        # Clear state and return to menu
                        user_states.pop(chat_id, None)
                        kb = main_menu(user.role)
                        send_message(chat_id, "🏠 Main Menu:", keyboard=kb)

                    return {"ok": True}

                # -------------------- Update Existing Shop --------------------
                elif action == "update_existing_shop" and user.role == "owner":
                    tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                    if not tenant_db:
                        send_message(chat_id, "❌ Unable to access database.")
                        user_states.pop(chat_id, None)
                        return {"ok": True}

                    shop_id = data.get("shop_id")
                    if not shop_id:
                        send_message(chat_id, "❌ Shop ID not found.")
                        user_states.pop(chat_id, None)
                        return {"ok": True}

                    shop = tenant_db.query(ShopORM).filter(ShopORM.shop_id == shop_id).first()
                    if not shop:
                        send_message(chat_id, "❌ Shop not found.")
                        user_states.pop(chat_id, None)
                        return {"ok": True}

                    if step == 1:  # New Name
                        new_name = text.strip()
                        if new_name != "-":
                            shop.name = new_name
                        user_states[chat_id] = {"action": action, "step": 2, "data": data}

                    if step == 2:  # New Location
                        new_location = text.strip()
                        if new_location != "-":
                            shop.location = new_location
                        user_states[chat_id] = {"action": action, "step": 3, "data": data}

                    if step == 3:  # New Contact
                        new_contact = text.strip()
                        if new_contact != "-":
                            shop.contact = new_contact

                        # Save changes
                        tenant_db.commit()
        
                        success_msg = f"✅ Shop updated successfully!\n\n"
                        success_msg += f"🏪 *{shop.name}*\n"
                        if shop.location:
                            success_msg += f"📍 {shop.location}\n"
                        if shop.contact:
                            success_msg += f"📞 {shop.contact}\n"
                        if shop.is_main:
                            success_msg += f"⭐ Main Store\n"

                        send_message(chat_id, success_msg)
        
                        # Clear state and return to menu
                        user_states.pop(chat_id, None)
                        kb = main_menu(user.role)
                        send_message(chat_id, "🏠 Main Menu:", keyboard=kb)

                    return {"ok": True}
    
                # -------------------- Shopkeeper Creation / Management --------------------
                elif action == "create_shopkeeper" and user.role == "owner":
                    if step == 1:  # Enter Shopkeeper Name
                        shopkeeper_name = text.strip()
                        if shopkeeper_name:
                            data["name"] = shopkeeper_name
                            user_states[chat_id] = {"action": action, "step": 2, "data": data}
                            send_message(chat_id, "👤 Enter shopkeeper phone number or email:")
                        else:
                            send_message(chat_id, "❌ Name cannot be empty. Enter shopkeeper name:")

                    elif step == 2:  # Enter Shopkeeper Contact
                        contact = text.strip()
                        if not contact:
                            send_message(chat_id, "❌ Contact cannot be empty. Enter shopkeeper phone or email:")
                            return {"ok": False}

                        data["contact"] = contact

                        # Generate Credentials
                        username = create_username(f"SK{int(time.time())}")
                        password = generate_password()
                        password_hash = hash_password(password)

                        # Save Shopkeeper (using tenant_schema for relationship inference)
                        new_sk = User(
                            name=data["name"],
                            username=username,
                            password_hash=password_hash,
                            email=contact if "@" in contact else None,
                            chat_id=None,  # will link on Telegram login
                            role="shopkeeper",
                            tenant_schema=user.tenant_schema  # ✅ Shopkeepers share owner's tenant schema
                        )
                        db.add(new_sk)
                        db.commit()
                        db.refresh(new_sk)

                        # Notify Owner
                        send_message(
                            chat_id,
                            f"✅ Shopkeeper created successfully!\n\n"
                            f"👤 Name: {data['name']}\n"
                            f"🔑 Username: {username}\n"
                            f"🔑 Password: {password}\n"
                            f"📞 Contact: {contact}\n"
                            f"🏪 Tenant Schema: {user.tenant_schema}\n\n"
                            f"Share these credentials with the shopkeeper for login."
                        )

                        # Reset & Show Menu
                        user_states.pop(chat_id, None)
                        kb_dict = main_menu(user.role)
                        send_message(chat_id, "🏠 Main Menu:", kb_dict)
                        return {"ok": True}
        

                # -------------------- Add Shop Flow --------------------
                elif action == "add_shop" and user.role == "owner":
                    if step == 1:  # Shop Name
                        shop_name = text.strip()
                        if not shop_name:
                            send_message(chat_id, "❌ Shop name cannot be empty. Please enter shop name:")
                            return {"ok": True}
                        data["name"] = shop_name
                        user_states[chat_id] = {"action": action, "step": 2, "data": data}
                        send_message(chat_id, "📍 Enter shop location:")

                    elif step == 2:  # Shop Location
                        location = text.strip()
                        if not location:
                            send_message(chat_id, "❌ Location cannot be empty. Please enter shop location:")
                            return {"ok": True}
                        data["location"] = location
                        user_states[chat_id] = {"action": action, "step": 3, "data": data}
                        send_message(chat_id, "📞 Enter shop contact number:")

                    elif step == 3:  # Shop Contact
                        contact = text.strip()
                        if not contact:
                            send_message(chat_id, "❌ Contact cannot be empty. Please enter contact number:")
                            return {"ok": True}
                        data["contact"] = contact

                        # Save the shop
                        tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                        if not tenant_db:
                            send_message(chat_id, "❌ Unable to access database.")
                            user_states.pop(chat_id, None)
                            return {"ok": True}

                        try:
                            # Check if this is the first shop (make it main)
                            existing_shops = tenant_db.query(ShopORM).count()
                            is_main = existing_shops == 0

                            new_shop = ShopORM(
                                name=data["name"],
                                location=data["location"],
                                contact=data["contact"],
                                is_main=is_main
                            )
                            tenant_db.add(new_shop)
                            tenant_db.commit()
                            tenant_db.refresh(new_shop)

                            success_msg = f"✅ Shop added successfully!\n\n"
                            success_msg += f"🏪 *{new_shop.name}*\n"
                            success_msg += f"📍 {new_shop.location}\n"
                            success_msg += f"📞 {new_shop.contact}\n"
                            if new_shop.is_main:
                                success_msg += f"⭐ *Main Store*\n"

                            send_message(chat_id, success_msg)

                        except Exception as e:
                            logger.error(f"❌ Error adding shop: {e}")
                            send_message(chat_id, "❌ Failed to add shop. Please try again.")

                        # Clear state and return to menu
                        user_states.pop(chat_id, None)
                        kb = main_menu(user.role)
                        send_message(chat_id, "🏠 Main Menu:", keyboard=kb)

                    return {"ok": True}

                # -------------------- Add Product --------------------
                elif action == "awaiting_product":
                    # -------------------- Ensure tenant DB --------------------
                    tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                    if tenant_db is None:
                        send_message(chat_id, "❌ Unable to access tenant database.")
                        return {"ok": True}

                    data = state.get("data", {})
  
                    # -------------------- Step Handling --------------------
                    if step == 1:  # Product Name
                        product_name = text.strip()
                        if not product_name:
                            send_message(chat_id, "❌ Product name cannot be empty. Please enter a valid product name:")
                            return {"ok": True}
                        data["name"] = product_name
                        user_states[chat_id] = {"action": action, "step": 2, "data": data}
                        send_message(chat_id, "📦 Enter quantity:")
                        return {"ok": True}

                    elif step == 2:  # Quantity
                        qty_text = text.strip()
                        if not qty_text:
                            send_message(chat_id, "❌ Quantity cannot be empty. Please enter a valid quantity:")
                            return {"ok": True}
                        try:
                            qty = int(qty_text)
                            if qty < 0:
                                send_message(chat_id, "❌ Quantity cannot be negative. Please enter a positive number:")
                                return {"ok": True}
                            data["quantity"] = qty
                            user_states[chat_id] = {"action": action, "step": 3, "data": data}
                            send_message(chat_id, "📏 Enter unit type (e.g., piece, pack, box, carton):")
                        except ValueError:
                            send_message(chat_id, "❌ Invalid quantity. Please enter a positive number:")
                        return {"ok": True}

                    elif step == 3:  # Unit Type
                        unit_type = text.strip().lower()
                        if not unit_type:
                            send_message(chat_id, "❌ Unit type cannot be empty. Please enter a valid unit type (e.g., piece, box, carton, kg, liter, pack):")
                            return {"ok": True}
    
                        # Validate unit type is not a number
                        try:
                            float(unit_type)  # If this works, it's a number (wrong!)
                            send_message(chat_id, "❌ Unit type must be text, not a number. Please enter like: piece, box, carton, kg, liter, pack:")
                            return {"ok": True}
                        except ValueError:
                            # Good, it's not a number
                            pass
    
                        # Common valid unit types
                        valid_units = ['piece', 'pieces', 'box', 'boxes', 'carton', 'cartons', 'kg', 'kgs', 'kilogram', 'kilograms', 'liter', 'liters', 'pack', 'packs', 'bottle', 'bottles', 'bag', 'bags']
    
                        # Allow any text but warn if not in common list
                        if unit_type not in valid_units:
                            send_message(chat_id, f"⚠️ '{unit_type}' is not a common unit type. Common types: piece, box, carton, kg, liter, pack. Continue anyway? (yes/no)")
                            user_states[chat_id] = {"action": "awaiting_product", "step": 3.5, "data": data}
                            return {"ok": True}
    
                        data["unit_type"] = unit_type
                        # ... continue with flow

                    elif step == 3.5:  # Confirm unusual unit type
                        confirmation = text.strip().lower()
                        if confirmation != "yes":
                            send_message(chat_id, "❌ Unit type rejected. Please enter a valid unit type (e.g., piece, box, carton):")
                            user_states[chat_id] = {"action": "awaiting_product", "step": 3, "data": data}
                            return {"ok": True}
    
                        data["unit_type"] = data.get("unit_type", "")
                        user_states[chat_id] = {"action": "awaiting_product", "step": 4, "data": data}
                        if user.role == "owner":
                            send_message(chat_id, "💲 Enter product price:")
                        else:
                            # Shopkeeper: save for approval and notify owner
                            if add_product_pending_approval(tenant_db, chat_id, data):
                                send_message(chat_id, f"✅ Product *{data['name']}* added for approval. Owner will review shortly.")
                                # Notify owner immediately
                                notify_owner_of_pending_approval(chat_id, "add_product", data['name'], user.name)
                            else:
                                send_message(chat_id, "❌ Failed to submit product for approval. Please try again.")
        
                            user_states.pop(chat_id, None)
                        return {"ok": True}

                    elif step == 4:  # Price (Owner only)
                        price_text = text.strip()
                        if not price_text:
                            send_message(chat_id, "❌ Price cannot be empty. Please enter a valid price:")
                            return {"ok": True}
                        try:
                            price = float(price_text)
                            if price <= 0:
                                send_message(chat_id, "❌ Price must be greater than 0. Please enter a positive number:")
                                return {"ok": True}
                            data["price"] = price
                            user_states[chat_id] = {"action": action, "step": 5, "data": data}
                            send_message(chat_id, "📊 Enter minimum stock level (e.g., 10):")
                        except ValueError:
                            send_message(chat_id, "❌ Invalid price. Please enter a positive number:")
                        return {"ok": True}

                    elif step == 5:  # Min Stock Level (Owner)
                        min_stock_text = text.strip()
                        if not min_stock_text:
                            send_message(chat_id, "❌ Minimum stock level cannot be empty. Please enter a valid number:")
                            return {"ok": True}
                        try:
                            min_stock = int(min_stock_text)
                            if min_stock < 0:
                                send_message(chat_id, "❌ Minimum stock cannot be negative. Please enter a valid number:")
                                return {"ok": True}
                            data["min_stock_level"] = min_stock
                            user_states[chat_id] = {"action": action, "step": 6, "data": data}
                            send_message(chat_id, "⚠️ Enter low stock threshold (alert level):")
                        except ValueError:
                            send_message(chat_id, "❌ Invalid number. Please enter a valid minimum stock level:")
                        return {"ok": True}

                    elif step == 6:  # Low Stock Threshold (Owner)
                        threshold_text = text.strip()
                        if not threshold_text:
                            send_message(chat_id, "❌ Low stock threshold cannot be empty. Please enter a valid number:")
                            return {"ok": True}
                        try:
                            threshold = int(threshold_text)
                            if threshold < 0:
                                send_message(chat_id, "❌ Low stock threshold cannot be negative. Please enter a valid number:")
                                return {"ok": True}
                            data["low_stock_threshold"] = threshold

                            # Save product
                            add_product(tenant_db, chat_id, data)
                            tenant_db.commit()
                            send_message(chat_id, f"✅ Product *{data['name']}* added successfully.")
                            user_states.pop(chat_id, None)
                            
                            # ✅ Return to main menu
                            kb = main_menu(user.role)
                            send_message(chat_id, "🏠 Main Menu:", keyboard=kb)
                        except ValueError:
                            send_message(chat_id, "❌ Invalid number. Please enter a valid low stock threshold:")
                        return {"ok": True}
                        

                # -------------------- Quick Stock Update Flow --------------------
                elif action == "quick_stock_update":
                    # Ensure tenant session is available
                    tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                    if tenant_db is None:
                        send_message(chat_id, "❌ Unable to access tenant database.")
                        return {"ok": True}

                    data = state.get("data", {})

                    # STEP 1: Search for product
                    if step == 1:
                        product_name = text.strip()
                        if not product_name:
                            send_message(chat_id, "❌ Product name cannot be empty. Please enter a product name:")
                            return {"ok": True}

                        # Search for products
                        matches = tenant_db.query(ProductORM).filter(
                            ProductORM.name.ilike(f"%{product_name}%")
                        ).all()

                        if not matches:
                            send_message(chat_id, "❌ No products found with that name. Try again:")
                            return {"ok": True}

                        if len(matches) == 1:
                            # Single match - proceed directly to quantity
                            product = matches[0]
                            data["selected_product"] = {
                                "product_id": product.product_id,
                                "name": product.name,
                                "current_stock": product.stock
                            }
                            user_states[chat_id] = {"action": "quick_stock_update", "step": 2, "data": data}
                            send_message(chat_id, f"📦 Selected: {product.name}\nCurrent stock: {product.stock}\n\nEnter quantity to ADD to stock:")
                        else:
                            # Multiple matches - show selection
                            data["search_results"] = [
                                {
                                    "product_id": p.product_id,
                                    "name": p.name,
                                    "current_stock": p.stock
                                } for p in matches
                            ]
                            user_states[chat_id] = {"action": "quick_stock_update", "step": 1.5, "data": data}
                            
                            kb_rows = []
                            for product in matches:
                                kb_rows.append([{
                                    "text": f"{product.name} (Stock: {product.stock})",
                                    "callback_data": f"select_stock_product:{product.product_id}"
                                }])
                            kb_rows.append([{"text": "❌ Cancel", "callback_data": "cancel_quick_stock"}])
                            
                            send_message(chat_id, "🔍 Multiple products found. Select one:", {"inline_keyboard": kb_rows})
                        return {"ok": True}

                    # STEP 2: Enter quantity to add
                    elif step == 2:  # Enter quantity to add
                        quantity_text = text.strip()
                        if not quantity_text:
                            send_message(chat_id, "❌ Quantity cannot be empty. Enter quantity to add:")
                            return {"ok": True}

                        try:
                            quantity_to_add = int(quantity_text)
                            if quantity_to_add <= 0:
                                send_message(chat_id, "❌ Quantity must be greater than 0. Enter a positive number:")
                                return {"ok": True}

                            product = data["selected_product"]
            
                            if user.role == "owner":
                                # Owner can update directly
                                db_product = tenant_db.query(ProductORM).filter(
                                    ProductORM.product_id == product["product_id"]
                                ).first()
                
                                if db_product:
                                    old_stock = db_product.stock
                                    new_stock = old_stock + quantity_to_add
                                    db_product.stock = new_stock
                                    tenant_db.commit()
                    
                                    # Success message for owner
                                    success_msg = f"✅ Stock updated successfully!\n\n"
                                    success_msg += f"📦 Product: {product['name']}\n"
                                    success_msg += f"📊 Old Stock: {old_stock}\n"
                                    success_msg += f"📈 Added: +{quantity_to_add}\n"
                                    success_msg += f"🆕 New Stock: {new_stock}\n"
                    
                                    send_message(chat_id, success_msg)
                    
                                    # Return to main menu
                                    user_states.pop(chat_id, None)
                                    kb = main_menu(user.role)
                                    send_message(chat_id, "🏠 Main Menu:", keyboard=kb)
                                else:
                                    send_message(chat_id, "❌ Product not found in database.")
                                    user_states.pop(chat_id, None)
                    
                            else:
                                # Shopkeeper: request approval for stock update
                                db_product = tenant_db.query(ProductORM).filter(
                                    ProductORM.product_id == product["product_id"]
                                ).first()
                
                                if db_product:
                                    old_stock = db_product.stock
                                    new_stock = old_stock + quantity_to_add
                    
                                    # Save stock update for approval
                                    stock_data = {
                                        "product_id": product["product_id"],
                                        "product_name": product["name"],
                                        "old_stock": old_stock,
                                        "new_stock": new_stock,
                                        "quantity_added": quantity_to_add
                                    }
                    
                                    # Create pending approval for stock update
                                    from app.core import SessionLocal
                                    central_db = SessionLocal()
                                    shopkeeper_user = central_db.query(User).filter(User.chat_id == chat_id).first()
                    
                                    if shopkeeper_user:
                                        pending_stock = PendingApprovalORM(
                                            action_type='stock_update',
                                            shopkeeper_id=shopkeeper_user.user_id,
                                            shopkeeper_name=shopkeeper_user.name,
                                            product_data=json.dumps(stock_data),
                                            status='pending'
                                        )
                        
                                        tenant_db.add(pending_stock)
                                        tenant_db.commit()
                                        tenant_db.refresh(pending_stock)
                        
                                        # Notify owner
                                        owner = central_db.query(User).filter(
                                            User.tenant_schema == shopkeeper_user.tenant_schema,
                                            User.role == 'owner'
                                        ).first()
                        
                                        if owner:
                                            from app.telegram_notifications import notify_owner_of_stock_update_request
                                            notify_owner_of_stock_update_request(
                                                owner.chat_id,
                                                product["name"],
                                                old_stock,
                                                new_stock,
                                                shopkeeper_user.name,
                                                pending_stock.approval_id
                                            )
                        
                                        central_db.close()
                        
                                        send_message(chat_id, f"✅ Stock update request submitted for approval. Owner will review adding +{quantity_to_add} to {product['name']}.")
                                    else:
                                        send_message(chat_id, "❌ Failed to submit stock update request.")
                                        central_db.close()
                    
                                    user_states.pop(chat_id, None)
                                else:
                                    send_message(chat_id, "❌ Product not found in database.")
                                    user_states.pop(chat_id, None)

                        except ValueError:
                            send_message(chat_id, "❌ Invalid quantity. Enter a valid number:")
                        return {"ok": True}
        
                        
                # -------------------- Add Shop Stock Flow --------------------
                elif action == "add_shop_stock":
                    tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                    if not tenant_db:
                        send_message(chat_id, "❌ Unable to access store database.")
                        return {"ok": True}

                    data = state.get("data", {})

                    if step == 2:  # Search product
                        product_name = text.strip()
                        if not product_name:
                            send_message(chat_id, "❌ Product name cannot be empty. Please enter product name:")
                            return {"ok": True}

                        # Search for products
                        matches = tenant_db.query(ProductORM).filter(
                            ProductORM.name.ilike(f"%{product_name}%")
                        ).all()

                        if not matches:
                            send_message(chat_id, "❌ No products found. Please try again:")
                            return {"ok": True}

                        if len(matches) == 1:
                            product = matches[0]
                            data["selected_product_id"] = product.product_id
                            user_states[chat_id] = {"action": "add_shop_stock", "step": 3, "data": data}
                            send_message(chat_id, f"📦 Selected: {product.name}\nEnter initial stock quantity:")
                        else:
                            data["search_results"] = [
                                {
                                    "product_id": p.product_id,
                                    "name": p.name,
                                    "price": p.price
                                } for p in matches
                            ]
                            user_states[chat_id] = {"action": "add_shop_stock", "step": 2.5, "data": data}
            
                            kb_rows = []
                            for product in matches:
                                kb_rows.append([{
                                    "text": f"{product.name} (${product.price})",
                                    "callback_data": f"select_product_for_shop_stock:{product.product_id}"
                                }])
                            kb_rows.append([{"text": "❌ Cancel", "callback_data": "view_all_shops"}])
            
                            send_message(chat_id, "🔍 Multiple products found. Select one:", {"inline_keyboard": kb_rows})

                    elif step == 3:  # Enter stock quantity
                        quantity_text = text.strip()
                        if not quantity_text:
                            send_message(chat_id, "❌ Quantity cannot be empty. Enter initial stock quantity:")
                            return {"ok": True}

                        try:
                            quantity = int(quantity_text)
                            if quantity < 0:
                                send_message(chat_id, "❌ Quantity cannot be negative. Enter a positive number:")
                                return {"ok": True}

                            shop_id = data.get("selected_shop_id")
                            product_id = data.get("selected_product_id")

                            if not shop_id or not product_id:
                                send_message(chat_id, "❌ Missing shop or product selection. Please start over.")
                                user_states.pop(chat_id, None)
                                return {"ok": True}

                            # Check if stock record already exists
                            existing_stock = tenant_db.query(ProductShopStockORM).filter(
                                ProductShopStockORM.shop_id == shop_id,
                                ProductShopStockORM.product_id == product_id
                            ).first()

                            if existing_stock:
                                # Update existing stock
                                existing_stock.stock += quantity
                                message = f"✅ Stock updated!\nAdded {quantity} to existing stock."
                            else:
                                # Create new stock record
                                new_stock = ProductShopStockORM(
                                    shop_id=shop_id,
                                    product_id=product_id,
                                    stock=quantity,
                                    min_stock_level=0,
                                    low_stock_threshold=10,
                                    reorder_quantity=0
                                )
                                tenant_db.add(new_stock)
                                message = f"✅ Stock added!\nInitial stock: {quantity}"

                            tenant_db.commit()

                            # Get product and shop names for confirmation
                            product = tenant_db.query(ProductORM).filter(ProductORM.product_id == product_id).first()
                            shop = tenant_db.query(ShopORM).filter(ShopORM.shop_id == shop_id).first()

                            if product and shop:
                                message += f"\n\n🏪 *{shop.name}*\n"
                                message += f"📦 *{product.name}*\n"
                                message += f"📊 New stock level: {quantity}"

                            send_message(chat_id, message)

                        except ValueError:
                            send_message(chat_id, "❌ Invalid quantity. Enter a valid number:")
                            return {"ok": True}

                        # Clear state and return to menu
                        user_states.pop(chat_id, None)
                        kb = main_menu(user.role)
                        send_message(chat_id, "🏠 Main Menu:", keyboard=kb)

                    return {"ok": True}

                # -------------------- Update Product (owner only, step-by-step) --------------------
                elif action == "awaiting_update" and user.role == "owner":
                    # ✅ Use the SAME method as callback
                    tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                    if not tenant_db:
                        send_message(chat_id, "❌ Unable to access tenant database.")
                        return {"ok": True}

                    data = state.get("data", {})
                    step = state.get("step", 1)

                    # -------------------- STEP 1: Search by product name --------------------
                    if step == 1:
                        if not text or not text.strip():
                            send_message(chat_id, "⚠️ Please enter a product name to search:")
                            return {"ok": True}

                        query_text = text.strip()
                    
                        # DEBUG: Check what we're working with
                        logger.info(f"🔍 SEARCH DEBUG: Using tenant_schema: {user.tenant_schema}")
                    
                        matches = tenant_db.query(ProductORM).filter(ProductORM.name.ilike(f"%{query_text}%")).all()
                    
                        logger.info(f"🔍 SEARCH DEBUG: Found {len(matches)} products: {[f'ID:{m.product_id} {m.name}' for m in matches]}")

                        if not matches:
                            send_message(chat_id, f"⚠️ No products found matching '{query_text}'.")
                            user_states[chat_id] = {}  # reset state
                            return {"ok": True}

                        if len(matches) == 1:
                            selected = matches[0]
                            data["product_id"] = selected.product_id
                            user_states[chat_id] = {"action": "awaiting_update", "step": 2, "data": data}
                            send_message(chat_id, f"✏️ Updating {selected.name}.\nEnter NEW name (or '-' to keep current):")
                            return {"ok": True}

                        # Multiple matches → inline keyboard
                        kb_rows = [
                            [{"text": f"{p.name} — Stock: {p.stock} ({p.unit_type})",
                              "callback_data": f"select_update:{p.product_id}"}] for p in matches
                        ]
                        kb_rows.append([{"text": "⬅️ Cancel", "callback_data": "back_to_menu"}])
                        send_message(chat_id, "🔹 Multiple products found. Please select:", {"inline_keyboard": kb_rows})
                        return {"ok": True}

                    # -------------------- STEP 2+: update fields --------------------
                    if step >= 2:
                        product_id = data.get("product_id")
                        if not product_id:
                            send_message(chat_id, "⚠️ No product selected. Please start again from Update Product.")
                            user_states.pop(chat_id, None)
                            return {"ok": True}

                        product = tenant_db.query(ProductORM).filter(ProductORM.product_id == product_id).first()
                        if not product:
                            send_message(chat_id, "⚠️ Product not found. Please start again.")
                            user_states.pop(chat_id, None)
                            return {"ok": True}

                        # --- Proceed step-by-step: name → price → quantity → unit → min → low threshold ---
                        if step == 2:  # new name
                            val = text.strip()
                            if val == "":
                                send_message(chat_id, "⚠️ Please enter a valid name or '-' to keep current:")
                                return {"ok": True}
                            if val != "-":
                                data["new_name"] = val
                            user_states[chat_id] = {"action": "awaiting_update", "step": 3, "data": data}
                            send_message(chat_id, "💲 Enter new price (or send `-` to keep current):")
                            return {"ok": True}

                        if step == 3:  # price
                            val = text.strip()
                            if val == "":
                                send_message(chat_id, "⚠️ Please enter a valid price or '-' to keep current:")
                                return {"ok": True}
                            if val != "-":
                                try:
                                    price_val = float(val)
                                    if price_val <= 0:
                                        send_message(chat_id, "❌ Price must be greater than 0. Enter a valid price:")
                                        return {"ok": True}
                                    data["new_price"] = price_val
                                except ValueError:
                                    send_message(chat_id, "❌ Invalid price. Enter a number or `-` to skip:")
                                    return {"ok": True}
                            user_states[chat_id] = {"action": "awaiting_update", "step": 4, "data": data}
                            send_message(chat_id, "🔢 Enter new quantity (or send `-` to keep current):")
                            return {"ok": True}

                        if step == 4:  # quantity
                            val = text.strip()
                            if val == "":
                                send_message(chat_id, "⚠️ Please enter a valid quantity or '-' to keep current:")
                                return {"ok": True}
                            if val != "-":
                                try:
                                    qty_val = int(val)
                                    if qty_val < 0:
                                        send_message(chat_id, "❌ Quantity cannot be negative. Enter a valid number:")
                                        return {"ok": True}
                                    data["new_quantity"] = qty_val
                                except ValueError:
                                    send_message(chat_id, "❌ Invalid quantity. Enter a number or `-` to skip:")
                                    return {"ok": True}
                            user_states[chat_id] = {"action": "awaiting_update", "step": 5, "data": data}
                            send_message(chat_id, "📦 Enter new unit type (or send `-` to keep current):")
                            return {"ok": True}

                        if step == 5:  # unit
                            val = text.strip()
                            if val == "":
                                send_message(chat_id, "⚠️ Please enter a valid unit type or '-' to keep current:")
                                return {"ok": True}
                            if val != "-":
                                data["new_unit"] = val
                            user_states[chat_id] = {"action": "awaiting_update", "step": 6, "data": data}
                            send_message(chat_id, "📊 Enter new minimum stock level (or send `-` to keep current):")
                            return {"ok": True}

                        if step == 6:  # min stock
                            val = text.strip()
                            if val == "":
                                send_message(chat_id, "⚠️ Please enter a valid minimum stock level or '-' to keep current:")
                                return {"ok": True}
                            if val != "-":
                                try:
                                    min_stock_val = int(val)
                                    if min_stock_val < 0:
                                        send_message(chat_id, "❌ Minimum stock cannot be negative. Enter a valid number:")
                                        return {"ok": True}
                                    data["new_min_stock"] = min_stock_val
                                except ValueError:
                                    send_message(chat_id, "❌ Invalid number. Enter an integer or `-` to skip:")
                                    return {"ok": True}
                            user_states[chat_id] = {"action": "awaiting_update", "step": 7, "data": data}
                            send_message(chat_id, "⚠️ Enter new low stock threshold (or send `-` to keep current):")
                            return {"ok": True}

                        if step == 7:  # low threshold
                            val = text.strip()
                            if val == "":
                                send_message(chat_id, "⚠️ Please enter a valid low stock threshold or '-' to keep current:")
                                return {"ok": True}
                            if val != "-":
                                try:
                                    threshold_val = int(val)
                                    if threshold_val < 0:
                                        send_message(chat_id, "❌ Low stock threshold cannot be negative. Enter a valid number:")
                                        return {"ok": True}
                                    data["new_low_threshold"] = threshold_val
                                except ValueError:
                                    send_message(chat_id, "❌ Invalid number. Enter an integer or `-` to skip:")
                                    return {"ok": True}

                            # ✅ Update product in DB
                            update_product(tenant_db, chat_id, product, data)
                            tenant_db.commit()
                            send_message(chat_id, f"✅ Product *{product.name}* updated successfully.")
                            
                            # ✅ Return to main menu
                            user_states.pop(chat_id, None)  # Clear state
                            kb = main_menu(user.role)  # Get role-based menu
                            send_message(chat_id, "🏠 Main Menu:", keyboard=kb)
                            return {"ok": True}
                            

                # -------------------- Record Sale (Cart-based system) --------------------
                elif action == "awaiting_sale":
                    # Ensure tenant session is available
                    tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                    if tenant_db is None:
                        send_message(chat_id, "❌ Unable to access tenant database.")
                        return {"ok": True}

                    data = state.get("data", {})
    
                    # Initialize cart if not exists
                    if "cart" not in data:
                        data["cart"] = []
    
                    # ✅ DEBUG: Log cart state at the start of each sale interaction
                    logger.info(f"🔍 CART DEBUG [sale_start] - Chat: {chat_id}, Items: {len(data['cart'])}")
        
                    # STEP 1: search by product name (Add to cart)
                    if step == 1:
                        if not text or not text.strip():
                            send_message(chat_id, "⚠️ Please enter a product name to add to cart:")
                            return {"ok": True}

                        matches = tenant_db.query(ProductORM).filter(ProductORM.name.ilike(f"%{text}%")).all()
                        if not matches:
                            send_message(chat_id, "⚠️ No products found with that name. Try again:")
                            return {"ok": True}

                        if len(matches) == 1:
                            selected = matches[0]
                            data["current_product"] = {
                                "product_id": selected.product_id,
                                "name": selected.name,
                                "price": float(selected.price),
                                "unit_type": selected.unit_type,
                                "available_stock": selected.stock
                            }
                            user_states[chat_id] = {"action": "awaiting_sale", "step": 2, "data": data}
                            send_message(chat_id, f"📦 Selected {selected.name} ({selected.unit_type}). Enter quantity to add:")
                            return {"ok": True}

                        # multiple matches -> show inline keyboard for user to pick
                        kb_rows = [
                            [{"text": f"{p.name} — Stock: {p.stock} ({p.unit_type})", "callback_data": f"select_sale:{p.product_id}"}]
                            for p in matches
                        ]
                        kb_rows.append([{"text": "🛒 View Cart", "callback_data": "view_cart"}])
                        send_message(chat_id, "🔹 Multiple products found. Please select:", {"inline_keyboard": kb_rows})
                        return {"ok": True}

                    # STEP 2: quantity for current product
                    elif step == 2:
                        logger.info(f"🔍 DEBUG: Current product data: {data.get('current_product')}")  # Debug
                        logger.info(f"🔍 DEBUG: Full data: {data}")  # Debug
                        
                        qty_text = text.strip()
                        if not qty_text:
                            send_message(chat_id, "❌ Quantity cannot be empty. Please enter a valid quantity:")
                            return {"ok": True}
                        try:
                            qty = int(qty_text)
                            if qty <= 0:
                                send_message(chat_id, "❌ Quantity must be greater than 0. Please enter a positive number:")
                                return {"ok": True}
                            
                            current_product = data.get("current_product")
                            if not current_product:
                                logger.error(f"❌ No current_product found in data: {data}")  # More detailed error
                                send_message(chat_id, "❌ No product selected. Please start over.")
                                user_states.pop(chat_id, None)
                                return {"ok": True}
                                
                            # Check stock availability
                            if qty > current_product["available_stock"]:
                                send_message(chat_id, f"❌ Insufficient stock. Available: {current_product['available_stock']}")
                                return {"ok": True}
                            
                            # Add to cart
                            cart_item = {
                                "product_id": current_product["product_id"],
                                "name": current_product["name"],
                                "price": current_product["price"],
                                "quantity": qty,
                                "unit_type": current_product["unit_type"],
                                "subtotal": current_product["price"] * qty
                            }
                            data["cart"].append(cart_item)
                            
                            # Show FULL cart summary (all items)
                            cart_summary = get_cart_summary(data["cart"])
                            kb_rows = [
                                [{"text": "➕ Add Another Item", "callback_data": "add_another_item"}],
                                [{"text": "🗑 Remove Item", "callback_data": "remove_item"}],
                                [{"text": "✅ Checkout", "callback_data": "checkout_cart"}],
                                [{"text": "❌ Cancel Sale", "callback_data": "cancel_sale"}]
                            ]
                            send_message(chat_id, f"✅ Item added to cart!\n\n{cart_summary}", {"inline_keyboard": kb_rows})
                            
                            # ✅ CRITICAL: Update state with cart data preserved
                            data.pop("current_product", None)  # Clear current product
                            user_states[chat_id] = {"action": "awaiting_sale", "step": 1, "data": data}  # Stay at step 1 but with updated cart
                            
                        except ValueError:
                            send_message(chat_id, "❌ Invalid quantity. Enter a positive integer:")
                        return {"ok": True}

                    # STEP 3: checkout - payment method
                    elif step == 3:
                        payment_method = text.strip().lower()
                        if not payment_method:
                            send_message(chat_id, "❌ Payment method cannot be empty. Choose: cash, ecocash, swipe:")
                            return {"ok": True}
                        if payment_method not in ["cash", "ecocash", "swipe"]:
                            send_message(chat_id, "❌ Invalid method. Choose: cash, ecocash, swipe:")
                            return {"ok": True}

                        data["payment_method"] = payment_method
    
                        # Calculate cart_total from cart
                        cart_total = sum(item["subtotal"] for item in data["cart"])
                        data["cart_total"] = cart_total
    
                        # If payment method is CASH, ask for sale type (cash/credit)
                        if payment_method == "cash":
                            user_states[chat_id] = {"action": "awaiting_sale", "step": 3.1, "data": data}
        
                            kb_rows = [
                                [{"text": "💵 Cash Sale", "callback_data": "sale_type:cash"}],
                                [{"text": "🔄 Credit Sale", "callback_data": "sale_type:credit"}],
                                [{"text": "⬅️ Back", "callback_data": "view_cart"}]
                            ]
        
                            send_message(chat_id, f"💰 Cart Total: ${cart_total:.2f}\n\n💳 Select sale type:", {"inline_keyboard": kb_rows})
                        else:
                            # For Ecocash/Swipe, it's always full payment
                            data["sale_type"] = "cash"
                            data["payment_type"] = "full"
                            data["amount_paid"] = cart_total
                            data["pending_amount"] = 0
                            data["change_left"] = 0
        
                            user_states[chat_id] = {"action": "awaiting_sale", "step": 6, "data": data}
                            send_message(chat_id, f"💰 Cart Total: ${cart_total:.2f}\n✅ {payment_method.title()} payment confirmed.\n\nConfirm sale? (yes/no)")
    
                        return {"ok": True}
                            
                    # STEP 3.1: Cash sale type selection (callback handler)
                    elif text.startswith("sale_type:"):
                        sale_type = text.split(":")[1]
    
                        # Get current state
                        current_state = user_states.get(chat_id, {})
                        current_data = current_state.get("data", {})
    
                        current_data["sale_type"] = sale_type
    
                        if sale_type == "cash":
                            # For cash sales, ask for amount tendered
                            current_data["payment_type"] = "full"  # Cash sales are always full payment
                            user_states[chat_id] = {"action": "awaiting_sale", "step": 4, "data": current_data}
        
                            cart_total = sum(item["subtotal"] for item in current_data["cart"])
                            send_message(chat_id, f"💰 Cart Total: ${cart_total:.2f}\n💵 Enter cash amount tendered by customer:")
    
                        else:  # credit
                            # For credit sales, ask for payment type (full/partial credit)
                            user_states[chat_id] = {"action": "awaiting_sale", "step": 3.2, "data": current_data}
        
                            kb_rows = [
                                [{"text": "💰 Full Credit", "callback_data": "credit_type:full"}],
                                [{"text": "📋 Partial Credit", "callback_data": "credit_type:partial"}],
                                [{"text": "⬅️ Back", "callback_data": "view_cart"}]
                            ]
        
                            cart_total = sum(item["subtotal"] for item in current_data["cart"])
                            send_message(chat_id, f"💰 Cart Total: ${cart_total:.2f}\n\n💳 Select credit type:", {"inline_keyboard": kb_rows})
    
                        return {"ok": True}
    
                    # STEP 3.2: Credit type selection (callback handler)
                    elif text.startswith("credit_type:"):
                        credit_type = text.split(":")[1]
    
                        # Get current state
                        current_state = user_states.get(chat_id, {})
                        current_data = current_state.get("data", {})
    
                        current_data["payment_type"] = credit_type  # full or partial
    
                        if credit_type == "full":
                            # Full credit - no payment, go straight to customer details
                            current_data["amount_paid"] = 0
                            current_data["pending_amount"] = current_data["cart_total"]
                            current_data["change_left"] = 0
        
                            user_states[chat_id] = {"action": "awaiting_sale", "step": 5, "data": current_data}
                            send_message(chat_id, "🔄 Full credit sale.\n👤 Enter customer name for credit follow-up:")
    
                        else:  # partial
                            # Partial credit - ask for amount paid
                            user_states[chat_id] = {"action": "awaiting_sale", "step": 4, "data": current_data}
        
                            cart_total = sum(item["subtotal"] for item in current_data["cart"])
                            send_message(chat_id, f"💰 Cart Total: ${cart_total:.2f}\n💵 Enter amount paid now (remaining will be credit):")
    
                        return {"ok": True}
    
                    # STEP 4: amount tendered
                    elif step == 4:
                        amount_text = text.strip()
                        if not amount_text:
                            send_message(chat_id, "❌ Amount cannot be empty. Please enter a valid amount:")
                            return {"ok": True}
                        try:
                            amount_paid = float(amount_text)
                            if amount_paid < 0:
                                send_message(chat_id, "❌ Amount cannot be negative. Please enter a valid amount:")
                                return {"ok": True}
        
                            # Calculate cart_total from cart
                            cart_total = sum(item["subtotal"] for item in data["cart"])
        
                            data["amount_paid"] = amount_paid
                            data["cart_total"] = cart_total
        
                            # Calculate based on sale type
                            if data.get("sale_type") == "credit":
                                # Credit sale with partial payment
                                data["pending_amount"] = max(cart_total - amount_paid, 0)
                                data["change_left"] = 0  # No change for credit sales
            
                                # Always ask for customer details for credit sales
                                user_states[chat_id] = {"action": "awaiting_sale", "step": 5, "data": data}
                                send_message(chat_id, f"📋 Partial credit sale.\nAmount paid: ${amount_paid:.2f}\nPending: ${data['pending_amount']:.2f}\n\n👤 Enter customer name:")
            
                            else:  # cash sale
                                data["pending_amount"] = 0
                                data["change_left"] = max(amount_paid - cart_total, 0)
            
                                # Show payment summary
                                summary_msg = f"💵 Payment Summary:\n"
                                summary_msg += get_cart_summary(data["cart"])
                                summary_msg += f"💰 Total: ${cart_total:.2f}\n"
                                summary_msg += f"💵 Tendered: ${amount_paid:.2f}\n"
            
                                if data["change_left"] > 0:
                                    summary_msg += f"🪙 Change Due: ${data['change_left']:.2f}\n\n"
                                    # Ask if shopkeeper has change
                                    kb_rows = [
                                        [{"text": "✅ Yes, I have change", "callback_data": "has_change:yes"}],
                                        [{"text": "❌ No, need customer details", "callback_data": "has_change:no"}]
                                    ]
                                    summary_msg += "Do you have change for the customer?"
                                    send_message(chat_id, summary_msg, {"inline_keyboard": kb_rows})
                                    user_states[chat_id] = {"action": "awaiting_sale", "step": 4.1, "data": data}
                                else:
                                    # No change due - go straight to confirmation
                                    summary_msg += "✅ Exact amount received.\n\nConfirm sale? (yes/no)"
                                    user_states[chat_id] = {"action": "awaiting_sale", "step": 6, "data": data}
                                    logger.info(f"🔍 STEP 4 → STEP 6 - No change due, awaiting confirmation. Chat: {chat_id}, Customer Name: {data.get('customer_name')}")
                                    send_message(chat_id, summary_msg)
                                            
                        except ValueError:
                            send_message(chat_id, "❌ Invalid number. Enter a valid amount:")
                        return {"ok": True}
        
                    # STEP 4.1: Change availability check (callback handler)
                    elif text.startswith("has_change:"):
                        has_change = text.split(":")[1]
    
                        # Get current state
                        current_state = user_states.get(chat_id, {})
                        current_data = current_state.get("data", {})
    
                        if has_change == "yes":
                            # Has change - no customer details needed
                            user_states[chat_id] = {"action": "awaiting_sale", "step": 6, "data": current_data}
                            send_message(chat_id, "✅ Change ready. Confirm sale? (yes/no)")
                        else:
                            # No change - need customer details for follow-up
                            user_states[chat_id] = {"action": "awaiting_sale", "step": 5, "data": current_data}
                            send_message(chat_id, "👤 Enter customer name (for change follow-up):")
    
                        return {"ok": True}
                        
                    # STEP 5: customer name (ONLY when needed - credit or no change)
                    elif step == 5:
                        customer_name = text.strip()
                        if not customer_name:
                            send_message(chat_id, "❌ Customer name cannot be empty. Please enter customer name:")
                            return {"ok": True}
                        data["customer_name"] = customer_name
    
                        # Only ask for contact if it's a credit sale (optional for change due)
                        if data.get("sale_type") == "credit":
                            user_states[chat_id] = {"action": "awaiting_sale", "step": 5.1, "data": data}
                            send_message(chat_id, "📞 Enter customer contact number (optional for credit follow-up):")
                        else:
                            # For change due, contact is optional
                            user_states[chat_id] = {"action": "awaiting_sale", "step": 6, "data": data}
                            send_message(chat_id, "📞 Enter customer contact number (optional for change follow-up) or type 'skip':")
                        return {"ok": True}

                    # STEP 5.1: Customer contact (optional)
                    elif step == 5.1:
                        customer_contact = text.strip()
                        if customer_contact.lower() == "skip":
                            customer_contact = ""
    
                        data["customer_contact"] = customer_contact
                        user_states[chat_id] = {"action": "awaiting_sale", "step": 6, "data": data}
                        send_message(chat_id, f"✅ Customer info recorded. Confirm sale? (yes/no)")
                        return {"ok": True}
    
                    # STEP 5.1: Customer contact (optional)
                    elif step == 5.1:
                        customer_contact = text.strip()
                        if customer_contact.lower() == "skip":
                            customer_contact = ""
    
                        data["customer_contact"] = customer_contact
                        user_states[chat_id] = {"action": "awaiting_sale", "step": 6, "data": data}
                        send_message(chat_id, f"✅ Customer info recorded. Confirm sale? (yes/no)")
                        return {"ok": True}
    
                    # STEP 6: customer contact OR confirmation
                    elif step == 6:
                        logger.info(f"🔍 STEP 6 ENTERED - Chat: {chat_id}, Customer Name: {data.get('customer_name')}, Text: '{text}'")
                        
                        # Check if we need customer contact (credit sales or change due)
                        if data.get("customer_name"):  # We're collecting customer details
                            logger.info(f"🔍 STEP 6 → Collecting contact - Chat: {chat_id}")
                            customer_contact = text.strip()
                            if not customer_contact:
                                send_message(chat_id, "❌ Contact cannot be empty. Enter customer contact number:")
                                return {"ok": True}
                            data["customer_contact"] = customer_contact
                            user_states[chat_id] = {"action": "awaiting_sale", "step": 7, "data": data}
                            send_message(chat_id, f"✅ Customer info recorded. Confirm sale? (yes/no)")
                        else:
                            # No customer details needed - this is confirmation for cash sales with no change
                            logger.info(f"🔍 STEP 6 → Processing confirmation - Chat: {chat_id}")
                            confirmation = text.strip().lower()
                            if not confirmation:
                                send_message(chat_id, "⚠️ Please confirm with 'yes' or 'no':")
                                return {"ok": True}
                            if confirmation != "yes":
                                send_message(chat_id, "❌ Sale cancelled.")
                                user_states.pop(chat_id, None)
                                kb = main_menu(user.role)
                                send_message(chat_id, "🏠 Main Menu:", keyboard=kb)
                                return {"ok": True}
                            
                            logger.info(f"✅ STEP 6 → Recording sale - Chat: {chat_id}")
                            # Record sale without customer details
                            record_sale_result = record_cart_sale(tenant_db, chat_id, data)
                            if record_sale_result:
                                logger.info(f"🎉 STEP 6 → Sale recorded successfully - Chat: {chat_id}")
                                user_states.pop(chat_id, None)
                                kb = main_menu(user.role)
                                send_message(chat_id, "🏠 Main Menu:", keyboard=kb)
                            else:
                                logger.error(f"❌ STEP 6 → Sale recording failed - Chat: {chat_id}")
                                send_message(chat_id, "❌ Failed to record sale. Please try again.")
                                user_states.pop(chat_id, None)
                        return {"ok": True}
                        
                    # STEP 7: final confirmation (ONLY when customer details were collected)
                    elif step == 7:
                        confirmation = text.strip().lower()
                        if not confirmation:
                            send_message(chat_id, "⚠️ Please confirm with 'yes' or 'no':")
                            return {"ok": True}
                        if confirmation != "yes":
                            send_message(chat_id, "❌ Sale cancelled.")
                            user_states.pop(chat_id, None)
                            kb = main_menu(user.role)
                            send_message(chat_id, "🏠 Main Menu:", keyboard=kb)
                            return {"ok": True}
                        
                        # Record sale with customer details
                        record_sale_result = record_cart_sale(tenant_db, chat_id, data)
                        if record_sale_result:
                            user_states.pop(chat_id, None)
                            kb = main_menu(user.role)
                            send_message(chat_id, "🏠 Main Menu:", keyboard=kb)
                        else:
                            send_message(chat_id, "❌ Failed to record sale. Please try again.")
                            user_states.pop(chat_id, None)
                        return {"ok": True}
                
                # Reports
                elif text == "📊 Reports":
                    kb_dict = report_menu_keyboard(user.role)
                    send_message(chat_id, "📊 Select a report:", kb_dict)
                    return {"ok": True}
                                                            
        return {"ok": True}

    except Exception as e:
        print("❌ Webhook crashed with error:", str(e))
        traceback.print_exc()
        return {"status": "error", "detail": str(e)}
