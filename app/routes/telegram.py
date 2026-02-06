# app/routes/telegram.py

import json 
import traceback
import secrets    # For secure password generation
import string     # For password character sets
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
from app.tenant_db import get_tenant_session, create_tenant_db, ensure_tenant_tables, ensure_tenant_session, create_initial_shop, create_additional_shop, create_shop_users
import random
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
from app.shop_utils import (
    create_shop_user,
    get_shop_users,
    delete_shop_user,
    reset_shop_user_password,
    hash_password,
    verify_password
)
# Add these imports
from app.user_management import (
    create_default_users,          # For default user creation
    get_users_for_shop,           # For user management
    delete_user,                  # For user deletion
    reset_user_password,          # For password reset
    generate_username,            # For username generation
    generate_password,            # For password generation
    update_user_role,             # For role changes
    get_role_based_menu,          # For role-based menus
    hash_password,                # Password hashing
    verify_password,              # Password verification
    format_user_credentials_message,  # For displaying credentials
    create_custom_user,            # For custom user creation
    is_user_allowed_for_action,    # For checking user actions
    get_user_by_username,          # For user search
    get_user_by_chat_id            # For user search
)

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
    """Generate main menu based on user role (Owner/Admin/Shopkeeper)."""
    
    # Base menu for all shop users (admin + shopkeeper)
    shop_user_menu = [
        ("📦 View Stock", "view_stock"),
        ("💰 Record Sale", "record_sale"),
        ("📊 Reports", "report_menu"),
        ("➕ Add Product", "add_product"),
        ("✏️ Update Product", "update_product"),
        ("📈 Quick Stock Update", "quick_stock_update")
    ]
    
    # Admin-specific additions (on top of shop_user_menu)
    admin_additions = [
        ("👥 Manage Users", "manage_users_admin"),
        ("📋 View All Products", "view_all_products_admin")
    ]
    
    # Owner menu (full access)
    owner_menu = [
        ("👑 Owner Dashboard", "owner_dashboard"),
        ("🏪 Manage Shops", "manage_shops"),
        ("👥 Manage Users", "manage_users"),
        ("➕ Add Product", "add_product"),
        ("✏️ Update Product", "update_product"),
        ("📈 Quick Stock Update", "quick_stock_update"),
        ("📦 View Stock", "view_stock"),
        ("💰 Record Sale", "record_sale"),
        ("📊 Reports", "report_menu"),
        ("⚙️ Settings", "shop_settings")
    ]
    
    # Build keyboard based on role
    if role == "owner":
        menu_items = owner_menu
    elif role == "admin":
        menu_items = shop_user_menu + admin_additions
    elif role == "shopkeeper":
        menu_items = shop_user_menu
    else:
        return {"inline_keyboard": []}
    
    # Create keyboard with 2 buttons per row
    keyboard = []
    for i in range(0, len(menu_items), 2):
        row = []
        if i < len(menu_items):
            text1, callback1 = menu_items[i]
            row.append({"text": text1, "callback_data": callback1})
        if i + 1 < len(menu_items):
            text2, callback2 = menu_items[i + 1]
            row.append({"text": text2, "callback_data": callback2})
        if row:
            keyboard.append(row)
    
    # Add help button at the bottom
    keyboard.append([{"text": "❓ Help", "callback_data": "help"}])
    
    return {"inline_keyboard": keyboard}
        
            
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
        
        # Get total stock across all shops
        total_stock = 0
        shop_stocks = tenant_db.query(ProductShopStockORM).filter(
            ProductShopStockORM.product_id == p.product_id
        ).all()
        
        if shop_stocks:
            total_stock = sum(stock.stock for stock in shop_stocks)
        
        # FIXED: Use total_stock instead of p.stock
        lines.append(f"ID {p.product_id}: {p.name} — ${price:.2f} — Total Stock: {total_stock}")

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
            schema_name, _ = create_tenant_db(chat_id)
            
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
def get_stock_list(tenant_db, shop_id=None):
    """
    Get stock list from tenant database.
    If shop_id is provided, shows stock for that specific shop.
    Otherwise shows all products.
    """
    try:
        lines = []
        
        if shop_id:
            # Get shop-specific stock
            shop = tenant_db.query(ShopORM).filter(ShopORM.shop_id == shop_id).first()
            if not shop:
                return "❌ Shop not found."
            
            lines.append(f"🏪 *{shop.name} - Stock Report*\n")
            
            # Get products with stock for this shop
            stock_items = tenant_db.query(ProductShopStockORM).filter(
                ProductShopStockORM.shop_id == shop_id
            ).all()
            
            if not stock_items:
                lines.append("📦 No stock assigned to this shop yet.")
            else:
                for item in stock_items:
                    product = tenant_db.query(ProductORM).filter(
                        ProductORM.product_id == item.product_id
                    ).first()
                    
                    if product:
                        status = "🟢" if item.stock > item.low_stock_threshold else "🔴" if item.stock == 0 else "🟡"
                        lines.append(f"{status} *{product.name}*")
                        lines.append(f"  📊 Stock: {item.stock} {product.unit_type}")
                        lines.append(f"  💰 Price: ${product.price:.2f}")
                        lines.append(f"  ⚠️ Low Stock Alert: {item.low_stock_threshold}")
                        if item.stock <= item.low_stock_threshold:
                            lines.append(f"  ⚠️ *LOW STOCK!*")
                        lines.append("")
        else:
            # Get all products (for backward compatibility)
            lines.append("📦 *All Products*\n")
            
            products = tenant_db.query(ProductORM).all()
            if not products:
                lines.append("No products found.")
            else:
                for product in products:
                    # Try to get stock from ProductShopStockORM
                    stock_items = tenant_db.query(ProductShopStockORM).filter(
                        ProductShopStockORM.product_id == product.product_id
                    ).all()
                    
                    if stock_items:
                        # Product has shop-specific stock
                        for item in stock_items:
                            shop = tenant_db.query(ShopORM).filter(ShopORM.shop_id == item.shop_id).first()
                            shop_name = shop.name if shop else f"Shop {item.shop_id}"
                            status = "🟢" if item.stock > item.low_stock_threshold else "🔴" if item.stock == 0 else "🟡"
                            lines.append(f"{status} *{product.name}* ({shop_name})")
                            lines.append(f"  📊 Stock: {item.stock} {product.unit_type}")
                            lines.append(f"  💰 Price: ${product.price:.2f}")
                            lines.append("")
                    else:
                        # Product has no shop-specific stock yet
                        lines.append(f"⚪ *{product.name}*")
                        lines.append(f"  📊 Stock: 0 {product.unit_type}")
                        lines.append(f"  💰 Price: ${product.price:.2f}")
                        lines.append(f"  ℹ️ No shop stock assigned")
                        lines.append("")
        
        if not lines:
            return "📦 No stock data available."
            
        return "\n".join(lines)
        
    except Exception as e:
        logger.error(f"❌ Error getting stock list: {e}")
        return f"❌ Error loading stock: {str(e)}"
        
def add_product(db: Session, chat_id: int, data: dict):
    """
    Add a product in a tenant-aware way using structured `data` collected step by step.
    The `db` session is already connected to the tenant's DB.
    Returns: None on success, or error message if something goes wrong.
    """
    try:
        name = data.get("name")
        price = float(data.get("price", 0))
        stock = int(data.get("quantity", 0))  # This is for shop stock, not product stock
        unit_type = data.get("unit_type", "unit")
        min_stock_level = int(data.get("min_stock_level", 0))
        low_stock_threshold = int(data.get("low_stock_threshold", 0))
        shop_id = data.get("shop_id")

        if not name:
            raise ValueError("Missing product name.")
        if price <= 0:
            raise ValueError("Price must be greater than 0.")
        if stock < 0:
            raise ValueError("Stock cannot be negative.")
    except Exception as e:
        send_message(chat_id, f"❌ Invalid product data: {str(e)}")
        return str(e)  # Return error message

    # Check for existing product
    query = db.query(ProductORM).filter(func.lower(ProductORM.name) == name.lower())
    if shop_id:
        query = query.filter(ProductORM.shop_id == shop_id)
    
    existing = query.first()
    if existing:
        send_message(chat_id, f"❌ Product '{name}' already exists{' for this shop' if shop_id else ''}.")
        return "Product already exists"

    # ✅ FIXED: Create product with ONLY basic fields
    new_product = ProductORM(
        name=name,
        price=price,
        unit_type=unit_type,
        shop_id=shop_id
        # ❌ REMOVED: stock, min_stock_level, low_stock_threshold
    )

    try:
        db.add(new_product)
        db.commit()
        db.refresh(new_product)
        
        # ✅ Create shop-specific stock record with ALL stock-related fields
        if shop_id:
            shop_stock = ProductShopStockORM(
                product_id=new_product.product_id,
                shop_id=shop_id,
                stock=stock,  # Stock goes here
                min_stock_level=min_stock_level,  # Min stock goes here
                low_stock_threshold=low_stock_threshold,  # Low threshold goes here
                reorder_quantity=0
            )
            db.add(shop_stock)
            db.commit()
        else:
            # If no shop_id (global product), handle differently
            # For now, just create a basic product without stock info
            pass
        
    except Exception as e:
        db.rollback()
        send_message(chat_id, f"❌ Database error: {str(e)}")
        return str(e)

    # Product added successfully - send success message
    shop_info = f" for shop {data.get('shop_name', '')}" if shop_id else ""
    send_message(
        chat_id,
        f"✅ Product added{shop_info}: *{name}*\n"
        f"💲 Price: ${price:.2f}\n"
        f"📦 Stock: {stock} {unit_type}\n"
        f"📊 Min Level: {min_stock_level}\n"
        f"⚠️ Low Stock Alert: {low_stock_threshold}"
    )
    
    return None  # Success - returns None    

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
    
    NOTE: Stock, min_stock_level, and low_stock_threshold are now handled in 
    ProductShopStockORM (shop-specific). This function only updates ProductORM fields.
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

        # -------------------- Unit Type --------------------
        if "new_unit" in data and data["new_unit"] != "-":
            product.unit_type = data["new_unit"].strip()

        # -------------------- REMOVED: Quantity, Min Stock, Low Threshold --------------------
        # These are now handled in ProductShopStockORM (shop-specific stock)
        # Do NOT update product.stock, product.min_stock_level, product.low_stock_threshold
        # as these attributes don't exist on ProductORM anymore
        
        # -------------------- Commit --------------------
        db.commit()
        db.refresh(product)
        
        # Get total stock across all shops (for informational display)
        total_stock = 0
        shop_stocks = db.query(ProductShopStockORM).filter(
            ProductShopStockORM.product_id == product.product_id
        ).all()
        
        if shop_stocks:
            total_stock = sum(stock.stock for stock in shop_stocks)
        
        # Also get low stock thresholds from shops (if any)
        low_thresholds = []
        for stock in shop_stocks:
            if stock.low_stock_threshold:
                # Get shop name
                shop = db.query(ShopORM).filter(ShopORM.shop_id == stock.shop_id).first()
                shop_name = shop.name if shop else f"Shop {stock.shop_id}"
                low_thresholds.append(f"{shop_name}: {stock.low_stock_threshold}")

        # Build success message
        success_msg = f"✅ Product updated successfully:\n"
        success_msg += f"📦 {product.name}\n"
        success_msg += f"💲 Price: ${product.price:.2f}\n"
        success_msg += f"📦 Unit: {product.unit_type}\n"
        success_msg += f"📊 Total Stock (all shops): {total_stock}\n"
        
        if low_thresholds:
            success_msg += f"⚠️ Low Stock Alerts: {', '.join(low_thresholds)}\n"
        else:
            success_msg += "⚠️ No low stock thresholds set\n"
            
        # Show which shop stocks were updated (if any)
        if "shop_stocks" in data and data["shop_stocks"]:
            success_msg += "\n🏪 Updated shop stocks:\n"
            for stock_info in data["shop_stocks"]:
                if "new_stock" in stock_info:
                    shop = db.query(ShopORM).filter(ShopORM.shop_id == stock_info["shop_id"]).first()
                    shop_name = shop.name if shop else f"Shop {stock_info['shop_id']}"
                    success_msg += f"  • {shop_name}: {stock_info.get('current_stock', '?')} → {stock_info['new_stock']}\n"

        send_message(chat_id, success_msg)

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
    from datetime import datetime, timedelta  # <-- ADD THIS HERE
    
    try:
        # ✅ FIX: Check for recent duplicate sales BEFORE processing
        shop_id = None  # Will be determined later
        customer_id = data.get("customer_id")
        cart_total = sum(item["subtotal"] for item in data.get("cart", []))
        
        # Only check for duplicates if we have a customer_id
        if customer_id:
            from datetime import datetime, timedelta
            five_minutes_ago = datetime.utcnow() - timedelta(minutes=5)
            
            # We need to get the shop_id first to check duplicates
            from app.core import SessionLocal
            central_db_temp = SessionLocal()
            current_user_temp = central_db_temp.query(User).filter(User.chat_id == chat_id).first()
            
            if current_user_temp:
                # Determine tentative shop_id for duplicate check
                if current_user_temp.role in ["admin", "shopkeeper"]:
                    shop_id_for_check = current_user_temp.shop_id
                elif current_user_temp.role == "owner":
                    shop_id_for_check = data.get("selected_shop_id")
                else:
                    shop_id_for_check = None
                
                if shop_id_for_check:
                    recent_sales = tenant_db.query(SaleORM).filter(
                        SaleORM.customer_id == customer_id,
                        SaleORM.shop_id == shop_id_for_check,
                        SaleORM.sale_date >= five_minutes_ago
                    ).all()
                    
                    if recent_sales:
                        # Check if any recent sale matches current cart total
                        for sale in recent_sales:
                            if abs(sale.total_amount - cart_total) < 0.01:  # Within 1 cent
                                logger.warning(f"⚠️ Duplicate sale detected and prevented: Sale ID {sale.sale_id}, Amount: ${sale.total_amount}")
                                central_db_temp.close()
                                send_message(chat_id, "⚠️ A similar sale was recently recorded. Please wait a few minutes or verify this is a new sale.")
                                return False
            central_db_temp.close()
        
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
        
        # ✅ UPDATED: Get current user to check shop assignment
        from app.core import SessionLocal
        central_db = SessionLocal()
        current_user = central_db.query(User).filter(User.chat_id == chat_id).first()
        central_db.close()
        
        if not current_user:
            logger.error(f"❌ User not found for chat_id: {chat_id}")
            send_message(chat_id, "❌ User not found. Please login again.")
            return False
        
        # ✅ UPDATED: Determine shop ID based on user role and selection
        shop_id = None
        shop_name = "Unknown Shop"
        
        if current_user.role in ["admin", "shopkeeper"]:
            # Admin/Shopkeeper MUST use their assigned shop
            shop_id = current_user.shop_id
            shop_name = current_user.shop_name or f"Shop {shop_id}"
            
            logger.info(f"🛒 Non-owner sale: User {current_user.username} recording sale for shop {shop_id}")
            
        elif current_user.role == "owner":
            # Owner can choose which shop
            shop_id = data.get("selected_shop_id")
            
            if not shop_id:
                # If no shop specified, find main shop
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
        
        else:
            logger.error(f"❌ Invalid user role: {current_user.role}")
            send_message(chat_id, "❌ Invalid user role.")
            return False
        
        # ✅ Validate shop assignment for non-owner users
        if current_user.role in ["admin", "shopkeeper"] and current_user.shop_id != shop_id:
            logger.error(f"❌ Security violation: User {current_user.username} tried to record sale for shop {shop_id} but is assigned to shop {current_user.shop_id}")
            send_message(chat_id, "❌ You can only record sales for your assigned shop.")
            return False
        
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
        
        # ✅ Check for low stock alerts for this specific shop
        for item in data["cart"]:
            product = tenant_db.query(ProductORM).filter(
                ProductORM.product_id == item["product_id"]
            ).first()
            
            if product:
                # Get shop-specific stock
                shop_stock = tenant_db.query(ProductShopStockORM).filter(
                    ProductShopStockORM.product_id == item["product_id"],
                    ProductShopStockORM.shop_id == shop_id
                ).first()
                
                if shop_stock and shop_stock.stock <= shop_stock.low_stock_threshold:
                    # Send low stock alert for this specific shop
                    from app.telegram_notifications import notify_low_stock
                    notify_low_stock(tenant_db, product, shop_id)
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Cart sale recording failed: {e}")
        import traceback
        traceback.print_exc()
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
                

# Add this function somewhere in your telegram.py file
def generate_comparison_report(tenant_db, report_type, shop_ids=None, shop_names=None):
    """
    Generate comparison report for multiple shops
    """
    try:
        if not shop_ids:
            return "❌ No shops selected for comparison."
        
        report = f"📈 *Shop Comparison Report*\n\n"
        
        if report_type == "report_daily":
            # Example: Daily sales comparison
            for shop_id, shop_name in zip(shop_ids, shop_names):
                daily_sales = tenant_db.query(SaleORM).filter(
                    func.date(SaleORM.sale_date) == func.current_date(),
                    SaleORM.shop_id == shop_id
                ).all()
                
                total = sum(sale.total_amount for sale in daily_sales)
                report += f"🏪 *{shop_name}*\n"
                report += f"   📊 Today's Sales: ${total:.2f}\n"
                report += f"   📈 Transactions: {len(daily_sales)}\n\n"
        
        elif report_type == "report_top_products":
            # Top products comparison
            for shop_id, shop_name in zip(shop_ids, shop_names):
                top_products = tenant_db.query(
                    SaleORM.product_id,
                    ProductORM.name,
                    func.sum(SaleORM.quantity).label('total_qty'),
                    func.sum(SaleORM.total_amount).label('total_amount')
                ).join(ProductORM, ProductORM.product_id == SaleORM.product_id
                ).filter(
                    SaleORM.shop_id == shop_id
                ).group_by(SaleORM.product_id, ProductORM.name
                ).order_by(func.sum(SaleORM.total_amount).desc()
                ).limit(5).all()
                
                report += f"🏪 *{shop_name} - Top Products*\n"
                for i, (product_id, name, qty, amount) in enumerate(top_products, 1):
                    report += f"   {i}. {name}: {qty} sold (${amount:.2f})\n"
                report += "\n"
        
        elif report_type == "report_low_stock":
            # Low stock comparison
            for shop_id, shop_name in zip(shop_ids, shop_names):
                low_stock = tenant_db.query(ProductShopStockORM).filter(
                    ProductShopStockORM.shop_id == shop_id,
                    ProductShopStockORM.stock <= ProductShopStockORM.low_stock_threshold
                ).count()
                
                report += f"🏪 *{shop_name}*\n"
                report += f"   ⚠️ Low Stock Items: {low_stock}\n\n"
        
        return report
        
    except Exception as e:
        logger.error(f"❌ Comparison report error: {e}")
        return f"❌ Error generating comparison report: {str(e)}"
        
# -------------------- Clean Tenant-Aware Reports --------------------      
def generate_report(tenant_db, report_type, shop_id=None, shop_name=None):
    """
    Generate various reports with shop filtering support
    
    Parameters:
    - tenant_db: Tenant database session
    - report_type: Type of report to generate
    - shop_id: Optional shop ID to filter by (None for all shops)
    - shop_name: Optional shop name for display
    """
    
    from datetime import datetime, timedelta
    
    try:
        # Get current date for time-based reports
        today = datetime.now().date()
        week_ago = today - timedelta(days=7)
        month_ago = today - timedelta(days=30)
        
        report = ""
        
        # ---------- DAILY SALES REPORT ----------
        if report_type == "report_daily":
            # Build query with shop filtering
            query = tenant_db.query(SaleORM).filter(
                func.date(SaleORM.sale_date) == today
            )
            
            if shop_id:
                query = query.filter(SaleORM.shop_id == shop_id)
            
            daily_sales = query.all()
            
            # Get shop info
            if shop_id:
                shop = tenant_db.query(ShopORM).filter(ShopORM.shop_id == shop_id).first()
                shop_display = f" for {shop.name}" if shop else f" for Shop {shop_id}"
            else:
                shop_display = ""
            
            report = f"📅 *Daily Sales Report{shop_display}*\n"
            report += f"📅 Date: {today.strftime('%Y-%m-%d')}\n\n"
            
            if not daily_sales:
                report += "No sales recorded today.\n"
            else:
                total_amount = sum(sale.total_amount for sale in daily_sales)
                total_quantity = sum(sale.quantity for sale in daily_sales)
                total_surcharge = sum(sale.surcharge_amount for sale in daily_sales)
                
                report += f"📊 **Summary:**\n"
                report += f"• Total Sales: ${total_amount:.2f}\n"
                if total_surcharge > 0:
                    report += f"• Total Surcharge: ${total_surcharge:.2f}\n"
                report += f"• Total Items Sold: {total_quantity}\n"
                report += f"• Number of Transactions: {len(daily_sales)}\n\n"
                
                # Group by product
                product_sales = {}
                for sale in daily_sales:
                    product = tenant_db.query(ProductORM).filter(
                        ProductORM.product_id == sale.product_id
                    ).first()
                    if product:
                        product_name = product.name
                        if product_name not in product_sales:
                            product_sales[product_name] = {"quantity": 0, "amount": 0}
                        product_sales[product_name]["quantity"] += sale.quantity
                        product_sales[product_name]["amount"] += sale.total_amount
                
                if product_sales:
                    report += f"📦 **Top Products Today:**\n"
                    for product_name, data in sorted(
                        product_sales.items(), 
                        key=lambda x: x[1]["amount"], 
                        reverse=True
                    )[:5]:
                        report += f"• {product_name}: {data['quantity']} sold (${data['amount']:.2f})\n"
        
        # ---------- WEEKLY SALES REPORT ----------
        elif report_type == "report_weekly":
            # Build query with shop filtering
            query = tenant_db.query(SaleORM).filter(
                SaleORM.sale_date >= week_ago
            )
            
            if shop_id:
                query = query.filter(SaleORM.shop_id == shop_id)
            
            weekly_sales = query.all()
            
            if shop_id:
                shop = tenant_db.query(ShopORM).filter(ShopORM.shop_id == shop_id).first()
                shop_display = f" for {shop.name}" if shop else f" for Shop {shop_id}"
            else:
                shop_display = ""
            
            report = f"📊 *Weekly Sales Report{shop_display}*\n"
            report += f"📅 Period: {week_ago.strftime('%Y-%m-%d')} to {today.strftime('%Y-%m-%d')}\n\n"
            
            if not weekly_sales:
                report += "No sales recorded this week.\n"
            else:
                total_amount = sum(sale.total_amount for sale in weekly_sales)
                total_quantity = sum(sale.quantity for sale in weekly_sales)
                
                report += f"📊 **Weekly Summary:**\n"
                report += f"• Total Sales: ${total_amount:.2f}\n"
                report += f"• Total Items Sold: {total_quantity}\n"
                report += f"• Number of Transactions: {len(weekly_sales)}\n\n"
                
                # Daily breakdown
                daily_totals = {}
                for sale in weekly_sales:
                    sale_date = sale.sale_date.date()
                    if sale_date not in daily_totals:
                        daily_totals[sale_date] = {"amount": 0, "count": 0}
                    daily_totals[sale_date]["amount"] += sale.total_amount
                    daily_totals[sale_date]["count"] += 1
                
                report += f"📅 **Daily Breakdown:**\n"
                for date in sorted(daily_totals.keys()):
                    report += f"• {date.strftime('%Y-%m-%d')}: ${daily_totals[date]['amount']:.2f} ({daily_totals[date]['count']} sales)\n"
        
        # ---------- MONTHLY SALES REPORT ----------
        elif report_type == "report_monthly":
            # Build query with shop filtering
            query = tenant_db.query(SaleORM).filter(
                SaleORM.sale_date >= month_ago
            )
            
            if shop_id:
                query = query.filter(SaleORM.shop_id == shop_id)
            
            monthly_sales = query.all()
            
            if shop_id:
                shop = tenant_db.query(ShopORM).filter(ShopORM.shop_id == shop_id).first()
                shop_display = f" for {shop.name}" if shop else f" for Shop {shop_id}"
            else:
                shop_display = ""
            
            report = f"📈 *Monthly Sales Report{shop_display}*\n"
            report += f"📅 Period: {month_ago.strftime('%Y-%m-%d')} to {today.strftime('%Y-%m-%d')}\n\n"
            
            if not monthly_sales:
                report += "No sales recorded this month.\n"
            else:
                total_amount = sum(sale.total_amount for sale in monthly_sales)
                total_quantity = sum(sale.quantity for sale in monthly_sales)
                
                report += f"📊 **Monthly Summary:**\n"
                report += f"• Total Sales: ${total_amount:.2f}\n"
                report += f"• Total Items Sold: {total_quantity}\n"
                report += f"• Number of Transactions: {len(monthly_sales)}\n\n"
                
                # Weekly breakdown
                weekly_totals = {}
                for sale in monthly_sales:
                    week_num = sale.sale_date.isocalendar()[1]  # Week number
                    if week_num not in weekly_totals:
                        weekly_totals[week_num] = {"amount": 0, "count": 0}
                    weekly_totals[week_num]["amount"] += sale.total_amount
                    weekly_totals[week_num]["count"] += 1
                
                report += f"📅 **Weekly Breakdown:**\n"
                for week_num in sorted(weekly_totals.keys()):
                    report += f"• Week {week_num}: ${weekly_totals[week_num]['amount']:.2f} ({weekly_totals[week_num]['count']} sales)\n"
        
        # ---------- LOW STOCK REPORT (FIXED!) ----------
        elif report_type == "report_low_stock":
            # FIXED: Use ProductShopStockORM instead of ProductORM
            # Build query with shop filtering
            query = tenant_db.query(ProductShopStockORM).join(
                ProductORM, ProductORM.product_id == ProductShopStockORM.product_id
            ).filter(
                ProductShopStockORM.stock <= ProductShopStockORM.low_stock_threshold
            )
            
            if shop_id:
                query = query.filter(ProductShopStockORM.shop_id == shop_id)
            
            low_stock_items = query.all()
            
            if shop_id:
                shop = tenant_db.query(ShopORM).filter(ShopORM.shop_id == shop_id).first()
                shop_display = f" for {shop.name}" if shop else f" for Shop {shop_id}"
            else:
                shop_display = ""
            
            report = f"📦 *Low Stock Report{shop_display}*\n"
            report += f"📅 Generated: {today.strftime('%Y-%m-%d %H:%M')}\n\n"
            
            if not low_stock_items:
                report += "✅ All stock levels are good!\n"
            else:
                report += f"⚠️ **Low Stock Items ({len(low_stock_items)}):**\n\n"
                
                for stock_item in low_stock_items:
                    product = stock_item.product
                    shop_info = tenant_db.query(ShopORM).filter(ShopORM.shop_id == stock_item.shop_id).first()
                    shop_name = shop_info.name if shop_info else f"Shop {stock_item.shop_id}"
                    
                    status = "🔴" if stock_item.stock == 0 else "🟡"
                    report += f"{status} *{product.name}*\n"
                    report += f"   🏪 Shop: {shop_name}\n"
                    report += f"   📊 Current Stock: {stock_item.stock} {product.unit_type}\n"
                    report += f"   ⚠️ Low Stock Threshold: {stock_item.low_stock_threshold}\n"
                    report += f"   📦 Minimum Stock: {stock_item.min_stock_level}\n"
                    
                    if stock_item.stock == 0:
                        report += f"   ❌ **OUT OF STOCK!**\n"
                    elif stock_item.stock <= stock_item.min_stock_level:
                        report += f"   ⚠️ **AT MINIMUM LEVEL!**\n"
                    
                    # Calculate how many to order
                    reorder_qty = max(stock_item.reorder_quantity, 
                                     stock_item.low_stock_threshold - stock_item.stock)
                    if reorder_qty > 0:
                        report += f"   📝 Suggested Reorder: {reorder_qty} {product.unit_type}\n"
                    
                    report += "\n"
        
        # ---------- TOP PRODUCTS REPORT ----------
        elif report_type == "report_top_products":
            # Build query with shop filtering
            query = tenant_db.query(
                SaleORM.product_id,
                func.sum(SaleORM.quantity).label('total_quantity'),
                func.sum(SaleORM.total_amount).label('total_amount')
            ).group_by(SaleORM.product_id)
            
            if shop_id:
                query = query.filter(SaleORM.shop_id == shop_id)
            
            top_products = query.order_by(func.sum(SaleORM.total_amount).desc()).limit(10).all()
            
            if shop_id:
                shop = tenant_db.query(ShopORM).filter(ShopORM.shop_id == shop_id).first()
                shop_display = f" for {shop.name}" if shop else f" for Shop {shop_id}"
            else:
                shop_display = ""
            
            report = f"🏆 *Top Products Report{shop_display}*\n"
            report += f"📅 Generated: {today.strftime('%Y-%m-%d %H:%M')}\n\n"
            
            if not top_products:
                report += "No sales data available.\n"
            else:
                report += "📊 **Top 10 Products by Revenue:**\n\n"
                
                for i, (product_id, quantity, amount) in enumerate(top_products, 1):
                    product = tenant_db.query(ProductORM).filter(
                        ProductORM.product_id == product_id
                    ).first()
                    
                    if product:
                        product_name = product.name
                        report += f"{i}. *{product_name}*\n"
                        report += f"   📦 Sold: {quantity} {product.unit_type}\n"
                        report += f"   💰 Revenue: ${amount:.2f}\n"
                        
                        # Calculate average price
                        avg_price = amount / quantity if quantity > 0 else 0
                        report += f"   💲 Avg Price: ${avg_price:.2f}\n\n"
        
        # ---------- AVERAGE ORDER VALUE REPORT ----------
        elif report_type == "report_aov":
            # Build query with shop filtering
            query = tenant_db.query(
                func.avg(SaleORM.total_amount).label('avg_amount'),
                func.count(SaleORM.sale_id).label('total_sales')
            )
            
            if shop_id:
                query = query.filter(SaleORM.shop_id == shop_id)
            
            result = query.first()
            
            if shop_id:
                shop = tenant_db.query(ShopORM).filter(ShopORM.shop_id == shop_id).first()
                shop_display = f" for {shop.name}" if shop else f" for Shop {shop_id}"
            else:
                shop_display = ""
            
            report = f"💰 *Average Order Value Report{shop_display}*\n"
            report += f"📅 Generated: {today.strftime('%Y-%m-%d %H:%M')}\n\n"
            
            if not result or result.total_sales == 0:
                report += "No sales data available.\n"
            else:
                avg_amount = result.avg_amount or 0
                total_sales = result.total_sales
                
                report += f"📊 **Statistics:**\n"
                report += f"• Average Order Value: ${avg_amount:.2f}\n"
                report += f"• Total Orders: {total_sales}\n"
                
                # Get distribution
                order_ranges = [
                    ("<$10", tenant_db.query(SaleORM).filter(SaleORM.total_amount < 10).count()),
                    ("$10-$50", tenant_db.query(SaleORM).filter(
                        SaleORM.total_amount >= 10, 
                        SaleORM.total_amount <= 50
                    ).count()),
                    ("$50-$100", tenant_db.query(SaleORM).filter(
                        SaleORM.total_amount > 50, 
                        SaleORM.total_amount <= 100
                    ).count()),
                    (">$100", tenant_db.query(SaleORM).filter(SaleORM.total_amount > 100).count())
                ]
                
                report += f"\n📈 **Order Value Distribution:**\n"
                for range_name, count in order_ranges:
                    percentage = (count / total_sales * 100) if total_sales > 0 else 0
                    report += f"• {range_name}: {count} orders ({percentage:.1f}%)\n"
        
        # ---------- STOCK TURNOVER REPORT ----------
        elif report_type == "report_stock_turnover":
            # FIXED: Use ProductShopStockORM
            # Get all stock items with sales data
            query = tenant_db.query(ProductShopStockORM).join(
                ProductORM, ProductORM.product_id == ProductShopStockORM.product_id
            )
            
            if shop_id:
                query = query.filter(ProductShopStockORM.shop_id == shop_id)
            
            stock_items = query.all()
            
            if shop_id:
                shop = tenant_db.query(ShopORM).filter(ShopORM.shop_id == shop_id).first()
                shop_display = f" for {shop.name}" if shop else f" for Shop {shop_id}"
            else:
                shop_display = ""
            
            report = f"🔄 *Stock Turnover Report{shop_display}*\n"
            report += f"📅 Generated: {today.strftime('%Y-%m-%d %H:%M')}\n\n"
            
            if not stock_items:
                report += "No stock data available.\n"
            else:
                report += "📊 **Stock Status Summary:**\n\n"
                
                for stock_item in stock_items[:20]:  # Limit to first 20 items
                    product = stock_item.product
                    shop_info = tenant_db.query(ShopORM).filter(ShopORM.shop_id == stock_item.shop_id).first()
                    shop_name = shop_info.name if shop_info else f"Shop {stock_item.shop_id}"
                    
                    # Get sales for this product
                    sales_query = tenant_db.query(SaleORM).filter(
                        SaleORM.product_id == product.product_id
                    )
                    
                    if shop_id:
                        sales_query = sales_query.filter(SaleORM.shop_id == shop_id)
                    
                    monthly_sales = sales_query.filter(
                        SaleORM.sale_date >= month_ago
                    ).all()
                    
                    total_sold = sum(sale.quantity for sale in monthly_sales)
                    
                    status = "🟢" if stock_item.stock > stock_item.low_stock_threshold else "🔴" if stock_item.stock == 0 else "🟡"
                    report += f"{status} *{product.name}*\n"
                    report += f"   🏪 Shop: {shop_name}\n"
                    report += f"   📊 Current Stock: {stock_item.stock}\n"
                    report += f"   📈 Sold (30 days): {total_sold}\n"
                    
                    # Calculate turnover rate
                    if stock_item.stock > 0:
                        turnover_rate = (total_sold / stock_item.stock) * 100
                        report += f"   🔄 Turnover Rate: {turnover_rate:.1f}%\n"
                    
                    report += "\n"
                
                if len(stock_items) > 20:
                    report += f"... and {len(stock_items) - 20} more items\n"
        
        # ---------- PAYMENT SUMMARY REPORT ----------
        elif report_type == "report_payment_summary":
            # Build query with shop filtering
            query = tenant_db.query(SaleORM)
    
            if shop_id:
                query = query.filter(SaleORM.shop_id == shop_id)
    
            all_sales = query.all()
    
            if shop_id:
                shop = tenant_db.query(ShopORM).filter(ShopORM.shop_id == shop_id).first()
                shop_display = f" for {shop.name}" if shop else f" for Shop {shop_id}"
            else:
                shop_display = ""
    
            report = f"💳 *Payment Summary Report{shop_display}*\n"
            report += f"📅 Generated: {today.strftime('%Y-%m-%d %H:%M')}\n\n"
    
            if not all_sales:
                report += "No sales data available.\n"
            else:
                # Payment method breakdown
                payment_methods = {}
                payment_types = {}
                total_amount = 0
                total_ecocash_surcharge = 0
        
                for sale in all_sales:
                    method = sale.payment_method or "unknown"
                    ptype = sale.payment_type or "full"
            
                    if method not in payment_methods:
                        payment_methods[method] = {"count": 0, "amount": 0, "surcharge": 0}
                    payment_methods[method]["count"] += 1
                    payment_methods[method]["amount"] += sale.total_amount
                    payment_methods[method]["surcharge"] += sale.surcharge_amount or 0
            
                    if ptype not in payment_types:
                        payment_types[ptype] = {"count": 0, "amount": 0}
                    payment_types[ptype]["count"] += 1
                    payment_types[ptype]["amount"] += sale.total_amount
            
                    total_amount += sale.total_amount
                    total_ecocash_surcharge += sale.surcharge_amount or 0
        
                report += f"📊 **Payment Methods:**\n"
                for method, data in payment_methods.items():
                    percentage = (data["amount"] / total_amount * 100) if total_amount > 0 else 0
                    report += f"• {method.title()}: {data['count']} sales (${data['amount']:.2f}, {percentage:.1f}%)\n"
                    if method == "ecocash" and data["surcharge"] > 0:
                        report += f"  ⚡ Surcharge: ${data['surcharge']:.2f}\n"
        
                report += f"\n📊 **Payment Types:**\n"
                for ptype, data in payment_types.items():
                    percentage = (data["amount"] / total_amount * 100) if total_amount > 0 else 0
                    report += f"• {ptype.title()}: {data['count']} sales (${data['amount']:.2f}, {percentage:.1f}%)\n"
        
                # Ecocash surcharge summary
                ecocash_sales = [s for s in all_sales if s.payment_method == "ecocash"]
                if ecocash_sales:
                    total_surcharge = sum(s.surcharge_amount or 0 for s in ecocash_sales)
                    total_ecocash_amount = sum(s.total_amount for s in ecocash_sales)
                    report += f"\n📱 **Ecocash Summary:**\n"
                    report += f"• Total Ecocash Sales: {len(ecocash_sales)}\n"
                    report += f"• Total Ecocash Amount: ${total_ecocash_amount:.2f}\n"
                    report += f"• Total Surcharge Collected: ${total_surcharge:.2f}\n"
                    if total_ecocash_amount > 0:
                        surcharge_percentage = (total_surcharge / total_ecocash_amount * 100)
                        report += f"• Surcharge Rate: {surcharge_percentage:.1f}%\n"
        
                # Add credit summary
                credit_sales = [s for s in all_sales if s.pending_amount and s.pending_amount > 0.01]
                if credit_sales:
                    total_credit_pending = sum(s.pending_amount for s in credit_sales)
                    total_credit_sales_amount = sum(s.total_amount for s in credit_sales)
                    total_credit_paid = sum(s.amount_paid for s in credit_sales)
            
                    report += f"\n🔄 **Credit Sales Summary:**\n"
                    report += f"• Total Credit Transactions: {len(credit_sales)}\n"
                    report += f"• Total Credit Sales Amount: ${total_credit_sales_amount:.2f}\n"
                    report += f"• Total Amount Paid: ${total_credit_paid:.2f}\n"
                    report += f"• Total Pending Amount: ${total_credit_pending:.2f}\n"
            
                    # Breakdown by credit type
                    full_credit = [s for s in credit_sales if s.payment_type == "credit"]
                    partial_credit = [s for s in credit_sales if s.payment_type == "partial"]
            
                    if full_credit:
                        full_total = sum(s.total_amount for s in full_credit)
                        full_pending = sum(s.pending_amount for s in full_credit)
                        report += f"\n📋 **Full Credit Sales:**\n"
                        report += f"  • Transactions: {len(full_credit)}\n"
                        report += f"  • Total Amount: ${full_total:.2f}\n"
                        report += f"  • Pending: ${full_pending:.2f}\n"
            
                    if partial_credit:
                        partial_total = sum(s.total_amount for s in partial_credit)
                        partial_paid = sum(s.amount_paid for s in partial_credit)
                        partial_pending = sum(s.pending_amount for s in partial_credit)
                        report += f"\n📋 **Partial Credit Sales:**\n"
                        report += f"  • Transactions: {len(partial_credit)}\n"
                        report += f"  • Total Amount: ${partial_total:.2f}\n"
                        report += f"  • Amount Paid: ${partial_paid:.2f}\n"
                        report += f"  • Pending: ${partial_pending:.2f}\n"
            
                    # Recent credit sales
                    recent_credits = sorted(credit_sales, key=lambda x: x.sale_date, reverse=True)[:5]
                    if recent_credits:
                        report += f"\n📅 **Recent Credit Sales (Last 5):**\n"
                        for sale in recent_credits:
                            product = tenant_db.query(ProductORM).filter(
                                ProductORM.product_id == sale.product_id
                            ).first()
                            product_name = product.name if product else f"Product {sale.product_id}"
                            report += f"  • {sale.sale_date.strftime('%Y-%m-%d')}: {product_name}\n"
                            report += f"    ${sale.total_amount:.2f} (Paid: ${sale.amount_paid:.2f}, Pending: ${sale.pending_amount:.2f})\n"
        
                # Add change due summary
                change_sales = [s for s in all_sales if s.change_left and s.change_left > 0.01]
                if change_sales:
                    total_change_due = sum(s.change_left for s in change_sales)
            
                    report += f"\n🪙 **Change Due Summary:**\n"
                    report += f"• Transactions with Change Due: {len(change_sales)}\n"
                    report += f"• Total Change Due: ${total_change_due:.2f}\n"
                    report += f"• Average Change Due: ${total_change_due/len(change_sales):.2f}\n"
            
                    # Amount breakdown
                    small_change = [s for s in change_sales if s.change_left < 1.00]
                    medium_change = [s for s in change_sales if 1.00 <= s.change_left < 5.00]
                    large_change = [s for s in change_sales if s.change_left >= 5.00]
            
                    if small_change:
                        total_small = sum(s.change_left for s in small_change)
                        report += f"\n📊 **Change Breakdown:**\n"
                        report += f"  • < $1.00: {len(small_change)} (${total_small:.2f})\n"
                    if medium_change:
                        total_medium = sum(s.change_left for s in medium_change)
                        report += f"  • $1.00-$5.00: {len(medium_change)} (${total_medium:.2f})\n"
                    if large_change:
                        total_large = sum(s.change_left for s in large_change)
                        report += f"  • ≥ $5.00: {len(large_change)} (${total_large:.2f})\n"
            
                    # Recent change due
                    recent_changes = sorted(change_sales, key=lambda x: x.sale_date, reverse=True)[:5]
                    if recent_changes:
                        report += f"\n📅 **Recent Change Due (Last 5):**\n"
                        for sale in recent_changes:
                            product = tenant_db.query(ProductORM).filter(
                                ProductORM.product_id == sale.product_id
                            ).first()
                            product_name = product.name if product else f"Product {sale.product_id}"
                            report += f"  • {sale.sale_date.strftime('%Y-%m-%d')}: {product_name}\n"
                            report += f"    Change Due: ${sale.change_left:.2f}\n"
        
                # 🆕 ADDED: Payment Records Summary
                # Query payment records with shop filtering
                payment_records_query = tenant_db.query(PaymentRecordORM)
                if shop_id:
                    payment_records_query = payment_records_query.filter(PaymentRecordORM.shop_id == shop_id)
        
                all_payment_records = payment_records_query.all()
        
                if all_payment_records:
                    # Credit payments
                    credit_payments = [p for p in all_payment_records if p.payment_type == "credit_payment"]
                    if credit_payments:
                        total_credit_collected = sum(p.amount for p in credit_payments)
                
                        # Group by payment method
                        credit_by_method = {}
                        for payment in credit_payments:
                            method = payment.payment_method or "unknown"
                            if method not in credit_by_method:
                                credit_by_method[method] = {"count": 0, "amount": 0}
                            credit_by_method[method]["count"] += 1
                            credit_by_method[method]["amount"] += payment.amount
                
                        report += f"\n💰 **Credit Payments Collected:**\n"
                        report += f"• Total Credit Payments: {len(credit_payments)}\n"
                        report += f"• Total Amount Collected: ${total_credit_collected:.2f}\n"
                
                        if credit_by_method:
                            report += f"• Breakdown by Method:\n"
                            for method, data in credit_by_method.items():
                                report += f"  • {method.title()}: {data['count']} payments (${data['amount']:.2f})\n"
                
                        # Recent credit payments
                        recent_credit_payments = sorted(credit_payments, key=lambda x: x.recorded_at, reverse=True)[:5]
                        if recent_credit_payments:
                            report += f"• Recent Payments (Last 5):\n"
                            for payment in recent_credit_payments:
                                report += f"  • {payment.recorded_at.strftime('%Y-%m-%d')}: ${payment.amount:.2f} from {payment.customer_name}\n"
                                if payment.payment_method:
                                    report += f"    Method: {payment.payment_method}\n"
            
                    # Change collections
                    change_collections = [p for p in all_payment_records if p.payment_type == "change_collection"]
                    if change_collections:
                        total_change_collected = sum(p.amount for p in change_collections)
                
                        report += f"\n🪙 **Change Collected:**\n"
                        report += f"• Total Change Collections: {len(change_collections)}\n"
                        report += f"• Total Amount Collected: ${total_change_collected:.2f}\n"
                        report += f"• Average Collection: ${total_change_collected/len(change_collections):.2f}\n"
                
                        # Recent change collections
                        recent_change_collections = sorted(change_collections, key=lambda x: x.recorded_at, reverse=True)[:5]
                        if recent_change_collections:
                            report += f"• Recent Collections (Last 5):\n"
                            for collection in recent_change_collections:
                                report += f"  • {collection.recorded_at.strftime('%Y-%m-%d')}: ${collection.amount:.2f} from {collection.customer_name}\n"
            
                    # Overall payment records summary
                    report += f"\n📈 **Payment Records Summary:**\n"
                    report += f"• Total Payment Records: {len(all_payment_records)}\n"
                    report += f"• Total Amount Processed: ${sum(p.amount for p in all_payment_records):.2f}\n"
            
                    # Monthly breakdown
                    monthly_totals = {}
                    for payment in all_payment_records:
                        month_key = payment.recorded_at.strftime("%Y-%m")
                        if month_key not in monthly_totals:
                            monthly_totals[month_key] = {"count": 0, "amount": 0}
                        monthly_totals[month_key]["count"] += 1
                        monthly_totals[month_key]["amount"] += payment.amount
            
                    if monthly_totals:
                        report += f"• Monthly Breakdown:\n"
                        for month, data in sorted(monthly_totals.items(), reverse=True)[:3]:  # Last 3 months
                            report += f"  • {month}: {data['count']} records (${data['amount']:.2f})\n"
        
                # Overall summary
                report += f"\n📈 **Overall Summary:**\n"
                report += f"• Total Sales: {len(all_sales)}\n"
                report += f"• Total Revenue: ${total_amount:.2f}\n"
                if total_ecocash_surcharge > 0:
                    report += f"• Total Surcharge: ${total_ecocash_surcharge:.2f}\n"
                if credit_sales:
                    report += f"• Credit Sales: {len(credit_sales)} (${sum(s.total_amount for s in credit_sales):.2f})\n"
                if change_sales:
                    report += f"• Change Due: {len(change_sales)} (${sum(s.change_left for s in change_sales):.2f})\n"
        
                # 🆕 ADDED: Include payment records totals in overall summary
                if all_payment_records:
                    total_payments_collected = sum(p.amount for p in all_payment_records)
                    report += f"• Total Payments Collected: ${total_payments_collected:.2f}\n"
            
                    # Collection efficiency
                    if credit_sales:
                        total_credit_outstanding = sum(s.pending_amount for s in credit_sales)
                        total_credit_collected = sum(p.amount for p in credit_payments) if credit_payments else 0
                        collection_rate = (total_credit_collected / (total_credit_collected + total_credit_outstanding) * 100) if (total_credit_collected + total_credit_outstanding) > 0 else 0
                        report += f"• Credit Collection Rate: {collection_rate:.1f}%\n"
            
                    if change_sales:
                        total_change_outstanding = sum(s.change_left for s in change_sales)
                        total_change_collected = sum(p.amount for p in change_collections) if change_collections else 0
                        collection_rate = (total_change_collected / (total_change_collected + total_change_outstanding) * 100) if (total_change_collected + total_change_outstanding) > 0 else 0
                        report += f"• Change Collection Rate: {collection_rate:.1f}%\n"
    
            return report
            
        # ---------- CREDIT SALES REPORT ----------
        elif report_type == "report_credits":
            # Check for credit sales properly
            from sqlalchemy import or_
    
            query = tenant_db.query(SaleORM).filter(
                or_(
                    SaleORM.payment_type.in_(["credit", "partial"]),
                    SaleORM.pending_amount > 0.01
                )
            )
    
            if shop_id:
                query = query.filter(SaleORM.shop_id == shop_id)
    
            credit_sales = query.all()
    
            if shop_id:
                shop = tenant_db.query(ShopORM).filter(ShopORM.shop_id == shop_id).first()
                shop_display = f" for {shop.name}" if shop else f" for Shop {shop_id}"
            else:
                shop_display = ""
    
            report = f"🔄 *Credit Sales Report{shop_display}*\n"
            report += f"📅 Generated: {today.strftime('%Y-%m-%d %H:%M')}\n\n"
    
            if not credit_sales:
                report += "No credit sales recorded.\n"
            else:
                total_pending = sum(sale.pending_amount for sale in credit_sales)
                total_credit_sales = sum(sale.total_amount for sale in credit_sales)
                total_paid = sum(sale.amount_paid for sale in credit_sales)
        
                report += f"📊 **Credit Summary:**\n"
                report += f"• Total Credit Sales: ${total_credit_sales:.2f}\n"
                report += f"• Amount Paid: ${total_paid:.2f}\n"
                report += f"• Total Pending Amount: ${total_pending:.2f}\n"
                report += f"• Number of Credit Transactions: {len(credit_sales)}\n\n"
        
                # Breakdown by payment type
                full_credit = [s for s in credit_sales if s.payment_type == "credit"]
                partial_credit = [s for s in credit_sales if s.payment_type == "partial"]
        
                if full_credit:
                    total_full = sum(s.pending_amount for s in full_credit)
                    report += f"📋 **Full Credit Sales:** {len(full_credit)} (${total_full:.2f} pending)\n"
        
                if partial_credit:
                    total_partial = sum(s.pending_amount for s in partial_credit)
                    report += f"📋 **Partial Credit Sales:** {len(partial_credit)} (${total_partial:.2f} pending)\n"
        
                # Get all customers with credit
                customer_credits = {}
                for sale in credit_sales:
                    if sale.customer_id:
                        customer = tenant_db.query(CustomerORM).filter(
                            CustomerORM.customer_id == sale.customer_id
                        ).first()
                        customer_name = customer.name if customer else f"Customer {sale.customer_id}"
                    else:
                        customer_name = "Unknown Customer"
            
                    if customer_name not in customer_credits:
                        customer_credits[customer_name] = {
                            "customer_id": sale.customer_id,
                            "count": 0, 
                            "pending": 0, 
                            "last_date": sale.sale_date,
                            "total_amount": 0
                        }
                    customer_credits[customer_name]["count"] += 1
                    customer_credits[customer_name]["pending"] += sale.pending_amount
                    customer_credits[customer_name]["total_amount"] += sale.total_amount
                    # Keep the most recent date
                    if sale.sale_date > customer_credits[customer_name]["last_date"]:
                        customer_credits[customer_name]["last_date"] = sale.sale_date
        
                if customer_credits:
                    report += f"\n👥 **Customers with Pending Credit:**\n"
                    sorted_customers = sorted(
                        customer_credits.items(), 
                        key=lambda x: x[1]["pending"], 
                        reverse=True
                    )[:10]  # Top 10 customers
            
                    for customer_name, data in sorted_customers:
                        days_ago = (today - data["last_date"].date()).days
                        report += f"\n• **{customer_name}**\n"
                        report += f"  📊 Total Credit: ${data['total_amount']:.2f}\n"
                        report += f"  💰 Pending: ${data['pending']:.2f}\n"
                        report += f"  📈 Transactions: {data['count']}\n"
                        report += f"  📅 Last Credit: {data['last_date'].strftime('%Y-%m-%d')} ({days_ago} days ago)\n"
                
                        # 🆕 ADDED: Payment history for this customer
                        if data["customer_id"]:
                            payment_records = tenant_db.query(PaymentRecordORM).filter(
                                PaymentRecordORM.customer_id == data["customer_id"],
                                PaymentRecordORM.payment_type == "credit_payment"
                            ).order_by(PaymentRecordORM.recorded_at.desc()).limit(3).all()
                    
                            if payment_records:
                                report += f"  📋 **Recent Payments (Last 3):**\n"
                                for pr in payment_records:
                                    method_display = f"via {pr.payment_method}" if pr.payment_method else ""
                                    report += f"    • {pr.recorded_at.strftime('%Y-%m-%d')}: ${pr.amount:.2f} {method_display}\n"
                                    if pr.notes:
                                        report += f"      Note: {pr.notes}\n"
        
                # Show recent credit sales (last 10)
                recent_credits = sorted(credit_sales, key=lambda x: x.sale_date, reverse=True)[:10]
                if recent_credits:
                    report += f"\n📅 **Recent Credit Sales (Last 10):**\n"
                    for sale in recent_credits:
                        product = tenant_db.query(ProductORM).filter(
                            ProductORM.product_id == sale.product_id
                        ).first()
                        product_name = product.name if product else f"Product {sale.product_id}"
                
                        report += f"• {sale.sale_date.strftime('%Y-%m-%d')}: {product_name}\n"
                        report += f"  Amount: ${sale.total_amount:.2f}, Paid: ${sale.amount_paid:.2f}, Pending: ${sale.pending_amount:.2f}\n"
                        if sale.customer_id:
                            customer = tenant_db.query(CustomerORM).filter(
                                CustomerORM.customer_id == sale.customer_id
                            ).first()
                            if customer:
                                report += f"  Customer: {customer.name}\n"
    
            return report
    
        # ---------- CHANGE DUE REPORT ----------
        elif report_type == "report_change":
            # Check change_left properly
            query = tenant_db.query(SaleORM).filter(
                SaleORM.change_left > 0.01
            )
    
            if shop_id:
                query = query.filter(SaleORM.shop_id == shop_id)
    
            change_sales = query.all()
    
            if shop_id:
                shop = tenant_db.query(ShopORM).filter(ShopORM.shop_id == shop_id).first()
                shop_display = f" for {shop.name}" if shop else f" for Shop {shop_id}"
            else:
                shop_display = ""
    
            report = f"🪙 *Change Due Report{shop_display}*\n"
            report += f"📅 Generated: {today.strftime('%Y-%m-%d %H:%M')}\n\n"
    
            if not change_sales:
                report += "✅ No change due recorded. All customers received their change.\n"
            else:
                total_change = sum(sale.change_left for sale in change_sales)
        
                report += f"📊 **Change Due Summary:**\n"
                report += f"• Total Change Due: ${total_change:.2f}\n"
                report += f"• Number of Transactions: {len(change_sales)}\n"
                report += f"• Average Change Due: ${total_change/len(change_sales):.2f}\n"
        
                # Breakdown by amount ranges
                small_change = [s for s in change_sales if s.change_left < 1.00]
                medium_change = [s for s in change_sales if 1.00 <= s.change_left < 5.00]
                large_change = [s for s in change_sales if s.change_left >= 5.00]
        
                report += f"\n📈 **Breakdown by Amount:**\n"
                if small_change:
                    total_small = sum(s.change_left for s in small_change)
                    report += f"• < $1.00: {len(small_change)} transactions (${total_small:.2f})\n"
                if medium_change:
                    total_medium = sum(s.change_left for s in medium_change)
                    report += f"• $1.00 - $5.00: {len(medium_change)} transactions (${total_medium:.2f})\n"
                if large_change:
                    total_large = sum(s.change_left for s in large_change)
                    report += f"• ≥ $5.00: {len(large_change)} transactions (${total_large:.2f})\n"
        
                # Get all customers with change due
                customer_changes = {}
                for sale in change_sales:
                    if sale.customer_id:
                        customer = tenant_db.query(CustomerORM).filter(
                            CustomerORM.customer_id == sale.customer_id
                        ).first()
                        customer_name = customer.name if customer else f"Customer {sale.customer_id}"
                    else:
                        customer_name = "Walk-in Customer"
            
                    if customer_name not in customer_changes:
                        customer_changes[customer_name] = {
                            "customer_id": sale.customer_id,
                            "count": 0, 
                            "change": 0, 
                            "last_date": sale.sale_date,
                            "total_sales": 0
                        }
                    customer_changes[customer_name]["count"] += 1
                    customer_changes[customer_name]["change"] += sale.change_left
                    customer_changes[customer_name]["total_sales"] += sale.total_amount
                    # Keep the most recent date
                    if sale.sale_date > customer_changes[customer_name]["last_date"]:
                        customer_changes[customer_name]["last_date"] = sale.sale_date
        
                if customer_changes:
                    report += f"\n👥 **Customers Owed Change:**\n"
                    sorted_customers = sorted(
                        customer_changes.items(), 
                        key=lambda x: x[1]["change"], 
                        reverse=True
                    )[:10]  # Top 10 customers
            
                    for customer_name, data in sorted_customers:
                        days_ago = (today - data["last_date"].date()).days
                        report += f"\n• **{customer_name}**\n"
                        report += f"  🪙 Change Due: ${data['change']:.2f}\n"
                        report += f"  📊 Transactions: {data['count']}\n"
                        report += f"  💰 Total Sales: ${data['total_sales']:.2f}\n"
                        report += f"  📅 Last Transaction: {data['last_date'].strftime('%Y-%m-%d')} ({days_ago} days ago)\n"
                
                        # 🆕 ADDED: Collection history for this customer
                        if data["customer_id"]:
                            collection_records = tenant_db.query(PaymentRecordORM).filter(
                                PaymentRecordORM.customer_id == data["customer_id"],
                                PaymentRecordORM.payment_type == "change_collection"
                            ).order_by(PaymentRecordORM.recorded_at.desc()).limit(3).all()
                    
                            if collection_records:
                                report += f"  📋 **Recent Collections (Last 3):**\n"
                                for cr in collection_records:
                                    report += f"    • {cr.recorded_at.strftime('%Y-%m-%d')}: ${cr.amount:.2f} collected\n"
                                    if cr.notes:
                                        report += f"      Note: {cr.notes}\n"
        
                # Show recent change due sales (last 10)
                recent_changes = sorted(change_sales, key=lambda x: x.sale_date, reverse=True)[:10]
                if recent_changes:
                    report += f"\n📅 **Recent Change Due (Last 10):**\n"
                    for sale in recent_changes:
                        product = tenant_db.query(ProductORM).filter(
                            ProductORM.product_id == sale.product_id
                        ).first()
                        product_name = product.name if product else f"Product {sale.product_id}"
                
                        report += f"• {sale.sale_date.strftime('%Y-%m-%d %H:%M')}: {product_name}\n"
                        report += f"  Change Due: ${sale.change_left:.2f}, Paid: ${sale.amount_paid:.2f}\n"
                        if sale.customer_id:
                            customer = tenant_db.query(CustomerORM).filter(
                                CustomerORM.customer_id == sale.customer_id
                            ).first()
                            if customer:
                                report += f"  Customer: {customer.name}\n"
        
                # Add actionable advice
                report += f"\n💡 **Action Required:**\n"
                if total_change > 10:
                    report += f"⚠️ Significant change due (${total_change:.2f}). Consider following up with customers.\n"
                elif len(change_sales) > 5:
                    report += f"⚠️ Multiple small change amounts ({len(change_sales)} transactions). Keep more small change available.\n"
                else:
                    report += f"✅ Manageable change due. Follow up when convenient.\n"
    
            return report
                    
    except Exception as e:
        logger.error(f"❌ Error generating report {report_type}: {e}")
        import traceback
        traceback.print_exc()
        return f"❌ Error generating report: {str(e)}"
        
def debug_sales_data(tenant_db):
    """Debug function to check sales data structure"""
    sales = tenant_db.query(SaleORM).all()
    
    debug_info = f"📊 **Sales Data Debug - Total Sales: {len(sales)}**\n\n"
    
    for sale in sales[:10]:  # First 10 sales
        debug_info += f"**Sale ID: {sale.sale_id}**\n"
        debug_info += f"  Shop ID: {sale.shop_id}\n"
        debug_info += f"  Total Amount: ${sale.total_amount:.2f}\n"
        debug_info += f"  Amount Paid: ${sale.amount_paid:.2f}\n"
        debug_info += f"  Pending Amount: ${sale.pending_amount:.2f}\n"
        debug_info += f"  Change Left: ${sale.change_left:.2f}\n"
        debug_info += f"  Payment Method: {sale.payment_method}\n"
        debug_info += f"  Payment Type: {sale.payment_type}\n"
        debug_info += f"  Sale Type: {getattr(sale, 'sale_type', 'N/A')}\n"
        debug_info += "  ---\n"
    
    return debug_info
    
def report_menu_keyboard(role, is_shop_specific=False, shop_name=None):
    """
    Generate report menu keyboard based on user role and context
    
    Parameters:
    - role: user role (owner, admin, shopkeeper)
    - is_shop_specific: True if showing reports for a specific shop
    - shop_name: Name of the specific shop (for display)
    """
    
    # Base reports for all roles - INCLUDING CHANGE & CREDIT
    base_reports = [
        [{"text": "📅 Daily Sales", "callback_data": "report_daily"}],
        [{"text": "📊 Weekly Sales", "callback_data": "report_weekly"}],
        [{"text": "📈 Monthly Sales", "callback_data": "report_monthly"}],
        [{"text": "📦 Low Stock", "callback_data": "report_low_stock"}],
        [{"text": "🏆 Top Products", "callback_data": "report_top_products"}],
        [{"text": "🔄 Credit Sales", "callback_data": "report_credits"}],  # ✅ ADDED for all
        [{"text": "🪙 Change Due", "callback_data": "report_change"}],     # ✅ ADDED for all
    ]
    
    # Advanced reports for owners and admins
    advanced_reports = []
    if role in ["owner", "admin"]:
        advanced_reports = [
            [{"text": "💰 Average Order Value", "callback_data": "report_aov"}],
            [{"text": "🔄 Stock Turnover", "callback_data": "report_stock_turnover"}],
            [{"text": "💳 Payment Summary", "callback_data": "report_payment_summary"}],
        ]
    
    # Special owner-only options
    owner_special = []
    if role == "owner" and not is_shop_specific:
        owner_special = [
            [{"text": "🏪 Compare Shops", "callback_data": "report_compare_shops"}],
            [{"text": "🏪 Shop-Specific Reports", "callback_data": "report_select_shop"}],
        ]
    
    # Navigation buttons
    nav_buttons = []
    if is_shop_specific and shop_name:
        nav_buttons.append([{"text": f"⬅️ Back to All Shops", "callback_data": "report_all_shops"}])
    else:
        nav_buttons.append([{"text": "⬅️ Back to Menu", "callback_data": "back_to_menu"}])
    
    # Combine all sections
    keyboard = base_reports + advanced_reports + owner_special + nav_buttons
    
    return {"inline_keyboard": keyboard}


# -------------------- Webhook --------------------
@router.post("/telegram/webhook")
async def telegram_webhook(request: Request, db: Session = Depends(get_db)):
    import traceback
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
                    print(f"🔍 DEBUG [user_type:owner]: Starting owner creation for chat_id={chat_id}")
                    
                    # Create new owner with generated credentials
                    generated_username = create_username(f"Owner{chat_id}")
                    from app.user_management import generate_password, hash_password
                    generated_password = generate_password()
                    generated_email = f"{chat_id}_{int(time.time())}@example.com"
                    print(f"🔍 DEBUG: Generated username: {generated_username}")

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
                    print(f"🔍 DEBUG: User created with ID: {new_user.user_id}")

                    # Create tenant schema
                    try:
                        schema_name, _ = create_tenant_db(chat_id)
                        print(f"🔍 DEBUG: Creating tenant schema: {schema_name}")
                        new_user.tenant_schema = schema_name
                        db.commit()
                        logger.info(f"✅ New owner created: {generated_username} with schema '{schema_name}'")
                        print(f"🔍 DEBUG: Tenant schema created and linked")
                    except Exception as e:
                        logger.error(f"❌ Failed to create tenant schema: {e}")
                        send_message(chat_id, "❌ Could not initialize store database.")
                        return {"ok": True}

                    # DEBUG: Test send_message directly
                    print(f"🔍 DEBUG: Testing send_message...")
                    try:
                        send_message(chat_id, "🔍 DEBUG: Test message from bot")
                        print(f"🔍 DEBUG: Test message sent successfully")
                    except Exception as e:
                        print(f"❌ ERROR in send_message test: {e}")
                        import traceback
                        traceback.print_exc()

                    # Send credentials and start shop setup
                    print(f"🔍 DEBUG: Calling send_owner_credentials...")
                    send_owner_credentials(chat_id, generated_username, generated_password)
                    print(f"🔍 DEBUG: Credentials function called")
                    
                    print(f"🔍 DEBUG: Sending shop setup prompt...")
                    send_message(chat_id, "🏪 Let's set up your shop! Please enter the shop name:")
                    print(f"🔍 DEBUG: Shop setup prompt sent")
                    
                    user_states[chat_id] = {"action": "setup_shop", "step": 1, "data": {}}
                    print(f"🔍 DEBUG: user_state set: setup_shop")
                    
                else:  # shopkeeper
                    # Step-by-step shopkeeper login
                    send_message(chat_id, "👤 Please enter your username:")
                    user_states[chat_id] = {"action": "shop_user_login", "step": 1, "data": {}}
        
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
                        [{"text": "👥 Manage Shop Users", "callback_data": "manage_shop_users"}],  # NEW
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
                tenant_db.close()
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
                
            # -------------------- Manage Shop Users --------------------
            elif text == "manage_shop_users" and role == "owner":
                tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                if not tenant_db:
                    send_message(chat_id, "❌ Unable to access store database.")
                    return {"ok": True}

                shops = tenant_db.query(ShopORM).all()
                tenant_db.close()
    
                if not shops:
                    send_message(chat_id, "❌ No shops found. Please create a shop first.")
                    return {"ok": True}

                # Show shop selection for user management
                kb_rows = []
                for shop in shops:
                    kb_rows.append([{"text": f"🏪 {shop.name} - Manage Users", "callback_data": f"select_shop_for_user_mgmt:{shop.shop_id}"}])
    
                kb_rows.append([{"text": "➕ Create New Shop User", "callback_data": "create_shop_user"}])
                kb_rows.append([{"text": "⬅️ Back", "callback_data": "manage_shops"}])
    
                send_message(chat_id, "👥 *Shop User Management*\n\nSelect a shop to manage users:", {"inline_keyboard": kb_rows})
                return {"ok": True}


            elif text == "create_shop_user" and role == "owner":
                # Get shops for selection
                tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                if not tenant_db:
                    send_message(chat_id, "❌ Unable to access store database.")
                    return {"ok": True}

                shops = tenant_db.query(ShopORM).all()
                tenant_db.close()
    
                if not shops:
                    send_message(chat_id, "❌ No shops found. Please create a shop first.")
                    return {"ok": True}

                # Create shop selection keyboard
                kb_rows = []
                for shop in shops:
                    kb_rows.append([{"text": f"🏪 {shop.name}", "callback_data": f"create_user_for_shop:{shop.shop_id}"}])
                kb_rows.append([{"text": "⬅️ Back", "callback_data": "manage_shop_users"}])
    
                send_message(chat_id, "🏪 Select shop for new shop user:", {"inline_keyboard": kb_rows})
                return {"ok": True}


            elif text.startswith("create_user_for_shop:") and role == "owner":
                try:
                    shop_id = int(text.split(":")[1])
        
                    # Get tenant session
                    tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                    if not tenant_db:
                        send_message(chat_id, "❌ Unable to access store database.")
                        return {"ok": True}
        
                    # Create shop user
                    result = create_shop_user(db, tenant_db, user, shop_id)
                    tenant_db.close()
        
                    if result:
                        credentials_msg = (
                            f"✅ *Shop User Created*\n\n"
                            f"🏪 **Shop:** {result['shop_name']}\n"
                            f"👤 **Username:** `{result['username']}`\n"
                            f"🔑 **Password:** `{result['password']}`\n\n"
                            f"📝 **Instructions:**\n"
                            f"1. Share these credentials with shopkeeper\n"
                            f"2. They use /start in Telegram\n"
                            f"3. Select 'I'm a Shopkeeper'\n"
                            f"4. Enter username and password\n\n"
                            f"⚠️ **Save this information!**"
                        )
                        send_message(chat_id, credentials_msg)
                    else:
                        send_message(chat_id, "❌ Failed to create shop user. Please try again.")
        
                except Exception as e:
                    logger.error(f"❌ Error creating shop user: {e}")
                    send_message(chat_id, "❌ Error creating shop user.")
    
                return {"ok": True}


            elif text.startswith("select_shop_for_user_mgmt:") and role == "owner":
                try:
                    shop_id = int(text.split(":")[1])
        
                    # Get shop details
                    tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                    if not tenant_db:
                        send_message(chat_id, "❌ Unable to access store database.")
                        return {"ok": True}
        
                    shop = tenant_db.query(ShopORM).filter(ShopORM.shop_id == shop_id).first()
                    if not shop:
                        send_message(chat_id, "❌ Shop not found.")
                        tenant_db.close()
                        return {"ok": True}
        
                    # Get existing shop users for this shop
                    shop_users = get_shop_users(db, user.tenant_schema, shop_id)
        
                    tenant_db.close()
        
                    # Build message
                    message = f"🏪 *{shop.name} - Shop Users*\n\n"
        
                    if not shop_users:
                        message += "No shop users created yet.\n\n"
                    else:
                        message += f"**Existing Users ({len(shop_users)}):**\n"
                        for i, shop_user in enumerate(shop_users, 1):
                            message += f"{i}. `{shop_user['username']}`\n"
                        message += "\n"
        
                    # Create management buttons
                    kb_rows = [
                        [{"text": "➕ Create New User", "callback_data": f"create_user_for_shop:{shop_id}"}],
                        [{"text": "🔄 Reset Password", "callback_data": f"reset_user_password:{shop_id}"}],
                        [{"text": "🗑 Delete User", "callback_data": f"delete_shop_user:{shop_id}"}],
                        [{"text": "📋 View All Users", "callback_data": f"view_all_shop_users:{shop_id}"}],
                        [{"text": "⬅️ Back", "callback_data": "manage_shop_users"}]
                    ]
        
                    send_message(chat_id, message, {"inline_keyboard": kb_rows})
        
                except Exception as e:
                    logger.error(f"❌ Error managing shop users: {e}")
                    send_message(chat_id, "❌ Error loading shop users.")
    
                return {"ok": True}
    
    
            elif text.startswith("reset_user_password:") and role == "owner":
                try:
                    shop_id = int(text.split(":")[1])
        
                    # Get shop users for selection
                    shop_users = get_shop_users(db, user.tenant_schema, shop_id)
        
                    if not shop_users:
                        send_message(chat_id, "❌ No shop users found for this shop.")
                        return {"ok": True}
        
                    # Create user selection keyboard
                    kb_rows = []
                    for shop_user in shop_users:
                        kb_rows.append([{"text": f"🔄 Reset: {shop_user['username']}", "callback_data": f"reset_password_for:{shop_user['username']}"}])
        
                    kb_rows.append([{"text": "⬅️ Back", "callback_data": f"select_shop_for_user_mgmt:{shop_id}"}])
        
                    send_message(chat_id, "👤 Select user to reset password:", {"inline_keyboard": kb_rows})
        
                except Exception as e:
                    logger.error(f"❌ Error resetting password: {e}")
                    send_message(chat_id, "❌ Error resetting password.")
    
                return {"ok": True}


            elif text.startswith("reset_password_for:") and role == "owner":
                try:
                    username = text.split(":")[1]
        
                    # Reset password
                    new_password = reset_shop_user_password(db, username)
        
                    if new_password:
                        success_msg = (
                            f"✅ *Password Reset Successful*\n\n"
                            f"👤 **Username:** `{username}`\n"
                            f"🔑 **New Password:** `{new_password}`\n\n"
                            f"Share the new password with the shopkeeper."
                        )
                        send_message(chat_id, success_msg)
                    else:
                        send_message(chat_id, f"❌ Failed to reset password for {username}")
        
                except Exception as e:
                    logger.error(f"❌ Error resetting password: {e}")
                    send_message(chat_id, "❌ Error resetting password.")
    
                return {"ok": True}


            elif text.startswith("delete_shop_user:") and role == "owner":
                try:
                    shop_id = int(text.split(":")[1])
        
                    # Get shop users for selection
                    shop_users = get_shop_users(db, user.tenant_schema, shop_id)
        
                    if not shop_users:
                        send_message(chat_id, "❌ No shop users found for this shop.")
                        return {"ok": True}
        
                    # Create user selection keyboard
                    kb_rows = []
                    for shop_user in shop_users:
                        kb_rows.append([{"text": f"🗑 Delete: {shop_user['username']}", "callback_data": f"delete_user:{shop_user['username']}"}])
        
                    kb_rows.append([{"text": "⬅️ Back", "callback_data": f"select_shop_for_user_mgmt:{shop_id}"}])
        
                    send_message(chat_id, "⚠️ Select user to DELETE (cannot be undone):", {"inline_keyboard": kb_rows})
        
                except Exception as e:
                    logger.error(f"❌ Error deleting user: {e}")
                    send_message(chat_id, "❌ Error deleting user.")
    
                return {"ok": True}


            elif text.startswith("delete_user:") and role == "owner":
                try:
                    username = text.split(":")[1]
        
                    # Confirm deletion
                    user_states[chat_id] = {
                        "action": "confirm_delete_user",
                        "data": {"username": username}
                    }
        
                    send_message(chat_id, f"⚠️ **Confirm Deletion**\n\nDelete shop user `{username}`?\n\nType 'YES' to confirm or 'NO' to cancel:")
        
                except Exception as e:
                    logger.error(f"❌ Error deleting user: {e}")
                    send_message(chat_id, "❌ Error deleting user.")
    
                return {"ok": True}
    
            # -------------------- Create Shopkeeper --------------------
            elif text == "create_shopkeeper":
                if role != "owner":
                    send_message(chat_id, "❌ Only owners can create shopkeepers.")
                    return {"ok": True}

                user_states[chat_id] = {"action": "create_shopkeeper", "step": 1, "data": {}}
                send_message(chat_id, "👤 Enter a username for the new shopkeeper:")
                return {"ok": True}

            # -------------------- User Management (Owner only) --------------------
            elif text == "manage_users" and role == "owner":
                # Get all shops first
                tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                if not tenant_db:
                    send_message(chat_id, "❌ Unable to access store database.")
                    return {"ok": True}
    
                shops = tenant_db.query(ShopORM).all()
                tenant_db.close()
    
                if not shops:
                    kb_rows = [
                        [{"text": "➕ Create First Shop", "callback_data": "setup_first_shop"}],
                        [{"text": "🔙 Back to Menu", "callback_data": "back_to_menu"}]
                    ]
                    send_message(chat_id, "📋 *User Management*\n\nNo shops found. Create your first shop to add users:", 
                                {"inline_keyboard": kb_rows})
                    return {"ok": True}
    
                # Create shop selection keyboard
                kb_rows = []
                for shop in shops:
                    # Get user count for this shop
                    shop_users = db.query(User).filter(
                        User.shop_id == shop.shop_id,
                        User.role.in_(["admin", "shopkeeper"])
                    ).count()
        
                    kb_rows.append([{
                        "text": f"🏪 {shop.name} ({shop_users} users)",
                        "callback_data": f"manage_shop_users:{shop.shop_id}"
                    }])
    
                kb_rows.append([{"text": "➕ Create New User", "callback_data": "create_user"}])
                kb_rows.append([{"text": "🔙 Back to Menu", "callback_data": "back_to_menu"}])
    
                send_message(chat_id, "📋 *User Management*\n\nSelect a shop to manage users:", 
                            {"inline_keyboard": kb_rows})
                return {"ok": True}
    
            elif text.startswith("manage_shop_users:") and role == "owner":
                try:
                    shop_id = int(text.split(":")[1])
        
                    # Get shop info
                    tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                    if not tenant_db:
                        send_message(chat_id, "❌ Unable to access store database.")
                        return {"ok": True}
        
                    shop = tenant_db.query(ShopORM).filter(ShopORM.shop_id == shop_id).first()
                    if not shop:
                        send_message(chat_id, "❌ Shop not found.")
                        tenant_db.close()
                        return {"ok": True}
        
                    # Get users for this shop
                    shop_users = db.query(User).filter(
                        User.shop_id == shop_id,
                        User.role.in_(["admin", "shopkeeper"])
                    ).all()
        
                    tenant_db.close()
        
                    # Build message
                    message = f"🏪 *{shop.name} - User Management*\n\n"
        
                    if not shop_users:
                        message += "No users created for this shop yet.\n\n"
                    else:
                        message += f"**Users ({len(shop_users)}):**\n"
                        for i, shop_user in enumerate(shop_users, 1):
                            role_icon = "🛡️" if shop_user.role == 'admin' else "👨‍💼"
                            status = "✅" if shop_user.chat_id else "❌"
                            message += f"{i}. {role_icon} `{shop_user.username}` ({shop_user.role}) {status}\n"
                        message += "\n✅ = Telegram linked\n❌ = Not linked yet\n\n"
        
                    # Create management buttons
                    kb_rows = [
                        [{"text": "➕ Create New User", "callback_data": f"create_user_for_shop:{shop_id}"}],
                        [{"text": "🔄 Reset Password", "callback_data": f"reset_user_password:{shop_id}"}],
                        [{"text": "🗑 Delete User", "callback_data": f"delete_shop_user:{shop_id}"}],
                        [{"text": "🔙 Back to Shop List", "callback_data": "manage_users"}]
                    ]
        
                    send_message(chat_id, message, {"inline_keyboard": kb_rows})
        
                except Exception as e:
                    logger.error(f"❌ Error managing shop users: {e}")
                    send_message(chat_id, "❌ Error loading shop users.")
    
                return {"ok": True}
    
            elif text.startswith("create_user_for_shop:") and role == "owner":
                try:
                    shop_id = int(text.split(":")[1])
        
                    # Get shop info
                    tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                    if not tenant_db:
                        send_message(chat_id, "❌ Unable to access store database.")
                        return {"ok": True}
        
                    shop = tenant_db.query(ShopORM).filter(ShopORM.shop_id == shop_id).first()
                    if not shop:
                        send_message(chat_id, "❌ Shop not found.")
                        tenant_db.close()
                        return {"ok": True}
        
                    tenant_db.close()
        
                    # Start user creation flow
                    user_states[chat_id] = {
                        "action": "create_user_for_shop",
                        "step": 1,
                        "data": {
                            "shop_id": shop_id,
                            "shop_name": shop.name
                        }
                    }
        
                    send_message(chat_id, f"👤 *Create User for {shop.name}*\n\nEnter username for the new user:")
        
                except Exception as e:
                    logger.error(f"❌ Error creating user: {e}")
                    send_message(chat_id, "❌ Error starting user creation.")
    
                return {"ok": True}
    
            elif text.startswith("reset_user_password:") and role == "owner":
                try:
                    shop_id = int(text.split(":")[1])
        
                    # Get users for this shop
                    shop_users = db.query(User).filter(
                        User.shop_id == shop_id,
                        User.role.in_(["admin", "shopkeeper"])
                    ).all()
        
                    if not shop_users:
                        send_message(chat_id, "❌ No users found for this shop.")
                        return {"ok": True}
        
                    # Create user selection keyboard
                    kb_rows = []
                    for shop_user in shop_users:
                        role_icon = "🛡️" if shop_user.role == 'admin' else "👨‍💼"
                        kb_rows.append([{
                            "text": f"{role_icon} Reset: {shop_user.username} ({shop_user.role})",
                            "callback_data": f"reset_password_for:{shop_user.username}"
                        }])
        
                    kb_rows.append([{"text": "🔙 Back", "callback_data": f"manage_shop_users:{shop_id}"}])
        
                    send_message(chat_id, "👤 Select user to reset password:", {"inline_keyboard": kb_rows})
        
                except Exception as e:
                    logger.error(f"❌ Error resetting password: {e}")
                    send_message(chat_id, "❌ Error resetting password.")
    
                return {"ok": True}
    
            elif text.startswith("reset_password_for:") and role == "owner":
                try:
                    username = text.split(":")[1]
        
                    # Reset password
                    from app.user_management import reset_user_password
                    new_password = reset_user_password(username)
        
                    if new_password:
                        # Get user info
                        target_user = db.query(User).filter(User.username == username).first()
            
                        # Format credentials message
                        from app.user_management import format_user_credentials_message
                        credentials_msg = format_user_credentials_message(target_user, new_password, target_user.role)
            
                        send_message(chat_id, credentials_msg)
                    else:
                        send_message(chat_id, f"❌ Failed to reset password for {username}")
        
                except Exception as e:
                    logger.error(f"❌ Error resetting password: {e}")
                    send_message(chat_id, "❌ Error resetting password.")
    
                return {"ok": True}
    
            elif text.startswith("delete_shop_user:") and role == "owner":
                try:
                    shop_id = int(text.split(":")[1])
        
                    # Get users for this shop
                    shop_users = db.query(User).filter(
                        User.shop_id == shop_id,
                        User.role.in_(["admin", "shopkeeper"])
                    ).all()
        
                    if not shop_users:
                        send_message(chat_id, "❌ No users found for this shop.")
                        return {"ok": True}
        
                    # Create user selection keyboard
                    kb_rows = []
                    for shop_user in shop_users:
                        role_icon = "🛡️" if shop_user.role == 'admin' else "👨‍💼"
                        kb_rows.append([{
                            "text": f"{role_icon} Delete: {shop_user.username} ({shop_user.role})",
                            "callback_data": f"delete_user:{shop_user.username}"
                        }])
        
                    kb_rows.append([{"text": "🔙 Back", "callback_data": f"manage_shop_users:{shop_id}"}])
        
                    send_message(chat_id, "⚠️ Select user to DELETE (cannot be undone):", {"inline_keyboard": kb_rows})
        
                except Exception as e:
                    logger.error(f"❌ Error deleting user: {e}")
                    send_message(chat_id, "❌ Error deleting user.")
    
                return {"ok": True}
    
            elif text.startswith("delete_user:") and role == "owner":
                try:
                    username = text.split(":")[1]
        
                    # Confirm deletion
                    user_states[chat_id] = {
                        "action": "confirm_delete_user",
                        "data": {"username": username}
                    }
        
                    send_message(chat_id, f"⚠️ **Confirm Deletion**\n\nDelete user `{username}`?\n\nType 'YES' to confirm or 'NO' to cancel:")
        
                except Exception as e:
                    logger.error(f"❌ Error deleting user: {e}")
                    send_message(chat_id, "❌ Error deleting user.")
    
                return {"ok": True}
    
            # -------------------- Admin User Management Callbacks --------------------
            elif text == "manage_users_admin" and role == "admin":
                # Get tenant session
                tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                if not tenant_db:
                    send_message(chat_id, "❌ Unable to access store database.")
                    return {"ok": True}

                # Get shopkeepers for this admin's shop
                shopkeepers = db.query(User).filter(
                    User.tenant_schema == user.tenant_schema,
                    User.shop_id == user.shop_id,
                    User.role == "shopkeeper",
                    User.user_id != user.user_id  # Exclude self
                ).all()

                tenant_db.close()

                # Build message
                message = f"👥 **User Management (Admin)**\n\n"
                message += f"🏪 Shop: {user.shop_name or f'Shop {user.shop_id}'}\n"
                message += f"📊 Shopkeepers: {len(shopkeepers)}\n\n"

                if not shopkeepers:
                    message += "No shopkeepers assigned to your shop.\n\n"
                else:
                    message += "**Current Shopkeepers:**\n"
                    for i, shopkeeper in enumerate(shopkeepers, 1):
                        status = "✅ Linked" if shopkeeper.chat_id else "❌ Not Linked"
                        message += f"{i}. `{shopkeeper.username}` - {status}\n"
                    message += "\n"

                # Create admin management buttons
                kb_rows = [
                    [{"text": "➕ Create Shopkeeper", "callback_data": "create_shopkeeper_admin"}],
                    [{"text": "🔄 Reset Password", "callback_data": "reset_password_admin"}],
                    [{"text": "🗑 Delete Shopkeeper", "callback_data": "delete_shopkeeper_admin"}],
                    [{"text": "📋 View All Shopkeepers", "callback_data": "view_shopkeepers_admin"}],
                    [{"text": "🔙 Back to Menu", "callback_data": "main_menu"}]
                ]

                send_message(chat_id, message, {"inline_keyboard": kb_rows})
                return {"ok": True}

            elif text == "create_shopkeeper_admin" and role == "admin":
                # Start shopkeeper creation flow
                user_states[chat_id] = {
                    "action": "create_shopkeeper_admin",
                    "step": 1,
                    "data": {
                        "shop_id": user.shop_id,
                        "shop_name": user.shop_name
                    }
                }
                send_message(chat_id, "👤 Enter username for new shopkeeper:")
                return {"ok": True}

            elif text == "reset_password_admin" and role == "admin":
                # Get shopkeepers for selection
                shopkeepers = db.query(User).filter(
                    User.tenant_schema == user.tenant_schema,
                    User.shop_id == user.shop_id,
                    User.role == "shopkeeper"
                ).all()

                if not shopkeepers:
                    send_message(chat_id, "❌ No shopkeepers to reset password.")
                    return {"ok": True}

                # Create selection keyboard
                kb_rows = []
                for shopkeeper in shopkeepers:
                    kb_rows.append([{
                        "text": f"🔄 {shopkeeper.username}",
                        "callback_data": f"reset_password_admin_user:{shopkeeper.username}"
                    }])
    
                kb_rows.append([{"text": "🔙 Back", "callback_data": "manage_users_admin"}])
    
                send_message(chat_id, "👤 Select shopkeeper to reset password:", {"inline_keyboard": kb_rows})
                return {"ok": True}

            elif text.startswith("reset_password_admin_user:") and role == "admin":
                username = text.split(":")[1]
    
                # Reset password
                from app.user_management import reset_user_password
                new_password = reset_user_password(username)
    
                if new_password:
                    # Get user info
                    target_user = db.query(User).filter(
                        User.username == username,
                        User.tenant_schema == user.tenant_schema,
                        User.shop_id == user.shop_id
                    ).first()
        
                    if target_user:
                        success_msg = f"✅ **Password Reset Successful**\n\n"
                        success_msg += f"👤 **Username:** `{target_user.username}`\n"
                        success_msg += f"🔑 **New Password:** `{new_password}`\n\n"
                        success_msg += f"Share the new password with the shopkeeper."
            
                        send_message(chat_id, success_msg)
                    else:
                        send_message(chat_id, f"❌ Shopkeeper {username} not found in your shop.")
                else:
                    send_message(chat_id, f"❌ Failed to reset password for {username}")
    
                return {"ok": True}
    
            elif text == "delete_shopkeeper_admin" and role == "admin":
                # Get shopkeepers for deletion
                shopkeepers = db.query(User).filter(
                    User.tenant_schema == user.tenant_schema,
                    User.shop_id == user.shop_id,
                    User.role == "shopkeeper"
                ).all()

                if not shopkeepers:
                    send_message(chat_id, "❌ No shopkeepers to delete.")
                    return {"ok": True}

                kb_rows = []
                for shopkeeper in shopkeepers:
                    kb_rows.append([{
                        "text": f"🗑 {shopkeeper.username}",
                        "callback_data": f"delete_shopkeeper_admin_user:{shopkeeper.username}"
                    }])
    
                kb_rows.append([{"text": "🔙 Back", "callback_data": "manage_users_admin"}])
    
                send_message(chat_id, "⚠️ Select shopkeeper to DELETE:", {"inline_keyboard": kb_rows})
                return {"ok": True}

            elif text.startswith("delete_shopkeeper_admin_user:") and role == "admin":
                username = text.split(":")[1]
    
                # Confirm deletion
                user_states[chat_id] = {
                    "action": "confirm_delete_shopkeeper_admin",
                    "data": {"username": username}
                }
    
                send_message(chat_id, f"⚠️ **Confirm Deletion**\n\nDelete shopkeeper `{username}`?\n\nType 'YES' to confirm or 'NO' to cancel:")
                return {"ok": True}

                message = f"👥 **Shopkeepers - {user.shop_name}**\n\n"
    
                if not shopkeepers:
                    message += "No shopkeepers in your shop.\n"
                else:
                    for i, shopkeeper in enumerate(shopkeepers, 1):
                        created = shopkeeper.created_at.strftime("%Y-%m-%d") if shopkeeper.created_at else "Unknown"
                        status = "✅ Active" if shopkeeper.is_active else "❌ Inactive"
                        telegram = "📱 Linked" if shopkeeper.chat_id else "❌ Not Linked"
            
                        message += f"{i}. **{shopkeeper.username}**\n"
                        message += f"   👤 Name: {shopkeeper.name}\n"
                        message += f"   📅 Created: {created}\n"
                        message += f"   {status} | {telegram}\n\n"

                kb_rows = [[{"text": "🔙 Back", "callback_data": "manage_users_admin"}]]
                send_message(chat_id, message, {"inline_keyboard": kb_rows})
                return {"ok": True}
    
            # Add this in your callback handling section (around line where you handle other callbacks):
            elif text == "owner_dashboard" and user.role == "owner":
                # Create owner dashboard
                tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                if not tenant_db:
                    send_message(chat_id, "❌ Unable to access store database.")
                    return {"ok": True}
    
                # Get stats for dashboard
                shops_count = tenant_db.query(ShopORM).count()
                products_count = tenant_db.query(ProductORM).count()
                sales_count = tenant_db.query(SaleORM).count()
    
                dashboard_msg = f"👑 *Owner Dashboard*\n\n"
                dashboard_msg += f"🏪 **Shops:** {shops_count}\n"
                dashboard_msg += f"📦 **Products:** {products_count}\n"
                dashboard_msg += f"💰 **Total Sales:** {sales_count}\n"
    
                # Get recent sales
                recent_sales = tenant_db.query(SaleORM).order_by(SaleORM.sale_date.desc()).limit(5).all()
                if recent_sales:
                    dashboard_msg += f"\n📈 **Recent Sales:**\n"
                    for sale in recent_sales:
                        product = tenant_db.query(ProductORM).filter(ProductORM.product_id == sale.product_id).first()
                        product_name = product.name if product else f"Product {sale.product_id}"
                        dashboard_msg += f"• {product_name}: ${sale.total_amount:.2f}\n"
    
                kb_rows = [
                    [{"text": "🏪 Manage Shops", "callback_data": "manage_shops"}],
                    [{"text": "👥 Manage Users", "callback_data": "manage_users"}],
                    [{"text": "📊 View Reports", "callback_data": "report_menu"}],
                    [{"text": "⬅️ Back to Menu", "callback_data": "back_to_menu"}]
                ]
    
                send_message(chat_id, dashboard_msg, {"inline_keyboard": kb_rows})
                tenant_db.close()
                return {"ok": True}
    
            # -------------------- Add Product --------------------
            elif text == "add_product":
                # For owners with multiple shops, ask which shop
                tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                if not tenant_db:
                    send_message(chat_id, "⚠️ Tenant database not linked. Please restart with /start.")
                    return {"ok": True}

                shops = tenant_db.query(ShopORM).all()
                tenant_db.close()

                # ✅ FIXED: Admin should only see their assigned shop
                if user.role == "admin":
                    if not user.shop_id:
                        send_message(chat_id, "❌ You are not assigned to any shop. Please contact owner.")
                        return {"ok": True}
        
                    # Get admin's assigned shop
                    tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                    shop = tenant_db.query(ShopORM).filter(ShopORM.shop_id == user.shop_id).first()
                    tenant_db.close()
        
                    if not shop:
                        send_message(chat_id, "❌ Your assigned shop not found.")
                        return {"ok": True}
        
                    # Start product creation directly for admin's shop
                    user_states[chat_id] = {
                        "action": "awaiting_product", 
                        "step": 1, 
                        "data": {"shop_id": shop.shop_id, "shop_name": shop.name}
                    }
                    send_message(chat_id, f"🏪 Shop: {shop.name}\n➕ Add a new product! 🛒\n\nEnter product name:")
    
                elif len(shops) == 1:
                    # Only one shop - start product creation directly
                    user_states[chat_id] = {
                        "action": "awaiting_product", 
                        "step": 1, 
                        "data": {"shop_id": shops[0].shop_id, "shop_name": shops[0].name}
                    }
                    send_message(chat_id, "➕ Add a new product! 🛒\n\nEnter product name:")
                else:
                    # Multiple shops - ask user to select (owners only)
                    if user.role != "owner":
                        send_message(chat_id, "❌ You can only add products to your assigned shop.")
                        return {"ok": True}
            
                    kb_rows = []
                    for shop in shops:
                        kb_rows.append([{
                            "text": f"🏪 {shop.name} {'⭐' if shop.is_main else ''}",
                            "callback_data": f"select_shop_for_product:{shop.shop_id}"
                        }])
                    kb_rows.append([{"text": "⬅️ Cancel", "callback_data": "back_to_menu"}])

                    send_message(chat_id, "🏪 Select shop for the new product:", {"inline_keyboard": kb_rows})

                return {"ok": True}
        
            elif text.startswith("select_shop_for_product:"):
                try:
                    shop_id = int(text.split(":")[1])
        
                    tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                    if not tenant_db:
                        send_message(chat_id, "❌ Unable to access store database.")
                        return {"ok": True}
        
                    shop = tenant_db.query(ShopORM).filter(ShopORM.shop_id == shop_id).first()
                    tenant_db.close()
        
                    if shop:
                        user_states[chat_id] = {
                            "action": "awaiting_product", 
                            "step": 1, 
                            "data": {"shop_id": shop_id, "shop_name": shop.name}
                        }
                        send_message(chat_id, f"🏪 Shop: {shop.name}\n➕ Add a new product! 🛒\n\nEnter product name:")
                    else:
                        send_message(chat_id, "❌ Shop not found.")
    
                except (ValueError, IndexError):
                    send_message(chat_id, "❌ Invalid shop selection.")
    
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
            
            # ==================== INLINE CONFIRMATION HANDLERS ====================
            
            # ✅ Handle delete confirmation from inline buttons
            elif text.startswith("confirm_delete_"):
                confirmation = text.split("_")[-1]  # "yes" or "no"
                
                if confirmation == "yes":
                    # Get current state to find username
                    current_state = user_states.get(chat_id, {})
                    username = current_state.get("data", {}).get("username")
                    
                    if not username:
                        send_message(chat_id, "❌ Error: No user selected for deletion.")
                        user_states.pop(chat_id, None)
                        return {"ok": True}
                    
                    # Get admin user to verify shop assignment
                    admin_user = db.query(User).filter(User.chat_id == chat_id).first()
                    if not admin_user or admin_user.role != 'admin':
                        send_message(chat_id, "❌ Unauthorized: Admin access required.")
                        user_states.pop(chat_id, None)
                        return {"ok": True}
                    
                    try:
                        # Find and delete the shopkeeper
                        shopkeeper = db.query(User).filter(
                            User.username == username,
                            User.tenant_schema == admin_user.tenant_schema,
                            User.shop_id == admin_user.shop_id,
                            User.role == 'shopkeeper'
                        ).first()
                        
                        if shopkeeper:
                            # Delete the shopkeeper
                            db.delete(shopkeeper)
                            db.commit()
                            
                            send_message(chat_id, f"✅ Shopkeeper `{username}` has been successfully deleted!")
                        else:
                            send_message(chat_id, f"❌ Shopkeeper `{username}` not found or doesn't belong to your shop.")
                    
                    except Exception as e:
                        db.rollback()
                        logging.error(f"Error deleting shopkeeper: {e}")
                        send_message(chat_id, f"❌ An error occurred while deleting the shopkeeper: {e}")
                
                else:  # "no"
                    send_message(chat_id, "✅ Deletion cancelled. The shopkeeper was not deleted.")
                
                # Clear state and show admin menu
                user_states.pop(chat_id, None)
                
                # Show admin user management menu
                kb_rows = [
                    [{"text": "➕ Create Shopkeeper", "callback_data": "create_shopkeeper_admin"}],
                    [{"text": "🔄 Reset Password", "callback_data": "reset_password_admin"}],
                    [{"text": "🗑 Delete Shopkeeper", "callback_data": "delete_shopkeeper_admin"}],
                    [{"text": "📋 View All Shopkeepers", "callback_data": "view_shopkeepers_admin"}],
                    [{"text": "🔙 Back to Menu", "callback_data": "main_menu"}]
                ]
                
                send_message(chat_id, "👥 User Management:", {"inline_keyboard": kb_rows})
                return {"ok": True}
            
            # ==================== END INLINE CONFIRMATION HANDLERS ====================    
            
            # -------------------- Quick Stock Update --------------------
            elif text == "quick_stock_update":
                # ✅ FIXED: Admin can only update stock in their assigned shop
                if user.role == "admin":
                    if not user.shop_id:
                        send_message(chat_id, "❌ You are not assigned to any shop. Please contact owner.")
                        return {"ok": True}
        
                    # Admin can update stock in their shop without approval
                    # (or with approval depending on your business rules)
                    # For now, let them update directly like owners for their shop
        
                user_states[chat_id] = {"action": "quick_stock_update", "step": 1, "data": {}}
                send_message(chat_id, "🔍 Enter product name to search:")
                return {"ok": True}
    
            # -------------------- Quick Stock Update Callbacks --------------------
            elif text.startswith("select_stock_product:"):
                try:
                    product_id = int(text.split(":")[1])
        
                    current_state = user_states.get(chat_id, {})
                    current_data = current_state.get("data", {})
        
                    # Get tenant session to check stock
                    tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                    if not tenant_db:
                        send_message(chat_id, "❌ Unable to access store database.")
                        return {"ok": True}
        
                    # Find the selected product from product_matches
                    selected_product = None
                    for product in current_data.get("product_matches", []):
                        if product["product_id"] == product_id:
                            selected_product = product
                            break
        
                    if selected_product:
                        # Get shop-specific stock
                        shop_id = current_data.get("selected_shop_id")
                        if not shop_id:
                            send_message(chat_id, "❌ No shop selected. Please start over.")
                            user_states.pop(chat_id, None)
                            return {"ok": True}
            
                        stock_item = tenant_db.query(ProductShopStockORM).filter(
                            ProductShopStockORM.product_id == product_id,
                            ProductShopStockORM.shop_id == shop_id
                        ).first()
            
                        current_stock = stock_item.stock if stock_item else 0
            
                        # Update selected product with current stock
                        selected_product["current_stock"] = current_stock
                        current_data["selected_product"] = selected_product
            
                        user_states[chat_id] = {"action": "quick_stock_update", "step": 2, "data": current_data}
            
                        # Get shop name for message
                        shop = tenant_db.query(ShopORM).filter(ShopORM.shop_id == shop_id).first()
                        shop_name = shop.name if shop else f"Shop {shop_id}"
            
                        send_message(chat_id, f"📦 Selected: {selected_product['name']}\n🏪 Shop: {shop_name}\n📊 Current stock: {current_stock}\n\nEnter quantity to ADD to stock:")
                    else:
                        send_message(chat_id, "❌ Product selection failed. Please try again.")
                        user_states.pop(chat_id, None)

                except (ValueError, IndexError):
                    send_message(chat_id, "❌ Invalid product selection.")
                    user_states.pop(chat_id, None)

                return {"ok": True}

            # New callback for owner shop selection
            elif text.startswith("select_shop_for_quick_stock:"):
                try:
                    shop_id = int(text.split(":")[1])
        
                    current_state = user_states.get(chat_id, {})
                    current_data = current_state.get("data", {})
        
                    # Get tenant session
                    tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                    if not tenant_db:
                        send_message(chat_id, "❌ Unable to access store database.")
                        return {"ok": True}
        
                    # Get shop info
                    shop = tenant_db.query(ShopORM).filter(ShopORM.shop_id == shop_id).first()
                    if not shop:
                        send_message(chat_id, "❌ Shop not found.")
                        user_states.pop(chat_id, None)
                        return {"ok": True}
        
                    current_data["selected_shop_id"] = shop_id
                    current_data["selected_shop_name"] = shop.name
        
                    # Get product matches from previous step
                    product_matches = current_data.get("product_matches", [])
        
                    if len(product_matches) == 1:
                        # Single match - proceed directly
                        product = product_matches[0]
                        current_data["selected_product"] = product
                        user_states[chat_id] = {"action": "quick_stock_update", "step": 2, "data": current_data}
            
                        # Get current stock for this shop
                        stock_item = tenant_db.query(ProductShopStockORM).filter(
                            ProductShopStockORM.product_id == product["product_id"],
                            ProductShopStockORM.shop_id == shop_id
                        ).first()
                        current_stock = stock_item.stock if stock_item else 0
            
                        send_message(chat_id, f"📦 Selected: {product['name']}\n🏪 Shop: {shop.name}\n📊 Current stock: {current_stock}\n\nEnter quantity to ADD to stock:")
                    else:
                        # Multiple matches - show product selection
                        user_states[chat_id] = {"action": "quick_stock_update", "step": 1.5, "data": current_data}
            
                        kb_rows = []
                        for product in product_matches:
                            # Get current stock for this shop
                            stock_item = tenant_db.query(ProductShopStockORM).filter(
                                ProductShopStockORM.product_id == product["product_id"],
                                ProductShopStockORM.shop_id == shop_id
                            ).first()
                            current_stock = stock_item.stock if stock_item else 0
                
                            kb_rows.append([{
                                "text": f"{product['name']} (Stock: {current_stock})",
                                "callback_data": f"select_stock_product:{product['product_id']}"
                            }])
                        kb_rows.append([{"text": "❌ Cancel", "callback_data": "cancel_quick_stock"}])
            
                        send_message(chat_id, f"🏪 Shop: {shop.name}\n🔍 Multiple products found. Select one:", {"inline_keyboard": kb_rows})
        
                except (ValueError, IndexError):
                    send_message(chat_id, "❌ Invalid shop selection.")
                    user_states.pop(chat_id, None)
    
                return {"ok": True}
    
            elif text == "cancel_quick_stock":
                user_states.pop(chat_id, None)
                send_message(chat_id, "❌ Quick stock update cancelled.")
                from app.user_management import get_role_based_menu
                kb = get_role_based_menu(user.role)
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
    
                # ✅ FIXED: Admin can only update products in their shop
                if user.role == "admin" and user.shop_id:
                    # Admin can only see products from their shop
                    # You might want to add shop_id to ProductORM or filter by stock
                    # For now, let admins search all products but only update ones in their shop
                    pass  # We'll handle this in the search
    
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

                # Get total stock across all shops (for informational purposes)
                total_stock = 0
                shop_stocks = tenant_db.query(ProductShopStockORM).filter(
                    ProductShopStockORM.product_id == product_id
                ).all()
                
                if shop_stocks:
                    total_stock = sum(stock.stock for stock in shop_stocks)
                
                # Start update flow
                user_states[chat_id] = {
                    "action": "awaiting_update",
                    "step": 2,
                    "data": {"product_id": product_id}
                }

                # FIXED: Removed product.stock - show total stock from all shops instead
                text_msg = (
                    f"✏️ Updating: {product.name}\n\n"
                    f"💰 Price: ${product.price:.2f}\n"
                    f"📦 Unit: {product.unit_type}\n"
                    f"🏪 Total Stock (across all shops): {total_stock}\n\n"
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

                # Get shops based on user role
                if user.role == "owner":
                    # Owner can see all shops
                    shops = tenant_db.query(ShopORM).all()
                elif user.role in ["admin", "shopkeeper"]:
                    # Admin/shopkeeper can only see their assigned shop
                    shops = tenant_db.query(ShopORM).filter(ShopORM.shop_id == user.shop_id).all()
                else:
                    send_message(chat_id, "❌ Invalid role.")
                    return {"ok": True}

                if not shops:
                    send_message(chat_id, "❌ No shops found. Please set up shops first in 'Manage Shops'.")
                    return {"ok": True}

                # ✅ IMPROVED: Always show shop selection for owners with multiple shops
                # This ensures owners can choose which shop to record sale from

                if user.role == "owner" and len(shops) > 1:
                    # Owner with multiple shops - always show selection
                    kb_rows = []
                    for shop in shops:
                        # Get shop stats (optional)
                        # FIXED: Use ShopORM in query instead of undefined variable
                        sales_count = tenant_db.query(SaleORM).filter(SaleORM.shop_id == shop.shop_id).count()
                        kb_rows.append([{
                            "text": f"🏪 {shop.name} ({sales_count} sales)",
                            "callback_data": f"select_shop_for_sale:{shop.shop_id}"
                        }])
    
                    # Add option to view all shops first
                    kb_rows.append([{"text": "📋 View All Shops Info", "callback_data": "view_shops_before_sale"}])
                    kb_rows.append([{"text": "⬅️ Cancel", "callback_data": "back_to_menu"}])

                    send_message(chat_id, "🏪 *Select Shop for Sale*\n\nChoose which shop you're recording the sale from:", {"inline_keyboard": kb_rows})
    
                elif len(shops) == 1:
                    # Only one shop (for any role) - use it automatically
                    user_states[chat_id] = {
                        "action": "awaiting_sale", 
                        "step": 1, 
                        "data": {
                            "selected_shop_id": shops[0].shop_id,
                            "selected_shop_name": shops[0].name,
                            "cart": []  # Initialize empty cart
                        }
                    }
                    send_message(chat_id, f"🏪 Shop: {shops[0].name}\n💰 Record a new sale!\nEnter product name:")
                else:
                    # Multiple shops for non-owner (shouldn't happen, but handle it)
                    kb_rows = []
                    for shop in shops:
                        kb_rows.append([{
                            "text": f"🏪 {shop.name}",
                            "callback_data": f"select_shop_for_sale:{shop.shop_id}"
                        }])

                    send_message(chat_id, "🏪 Select shop for sale:", {"inline_keyboard": kb_rows})

                tenant_db.close()
                return {"ok": True}
                            
            elif text == "view_shops_before_sale":
                tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                if not tenant_db:
                    send_message(chat_id, "❌ Unable to access store database.")
                    return {"ok": True}
    
                shops = tenant_db.query(ShopORM).all()
    
                shop_info = "🏪 *Your Shops - Sale Recording*\n\n"
                for shop in shops:
                    # Get sales stats for this shop
                    sales_today = tenant_db.query(SaleORM).filter(
                        SaleORM.shop_id == shop.shop_id,
                        func.date(SaleORM.sale_date) == func.current_date()
                    ).count()
        
                    total_sales = tenant_db.query(SaleORM).filter(
                        SaleORM.shop_id == shop.shop_id
                    ).count()
        
                    shop_info += f"*{shop.name}* {'⭐' if shop.is_main else ''}\n"
                    shop_info += f"📍 {shop.location or 'No location'}\n"
                    shop_info += f"📞 {shop.contact or 'No contact'}\n"
                    shop_info += f"📊 Today's sales: {sales_today}\n"
                    shop_info += f"📈 Total sales: {total_sales}\n\n"
    
                tenant_db.close()
    
                # Create selection buttons
                kb_rows = []
                for shop in shops:
                    kb_rows.append([{
                        "text": f"💰 Record Sale at {shop.name}",
                        "callback_data": f"select_shop_for_sale:{shop.shop_id}"
                    }])
    
                kb_rows.append([{"text": "⬅️ Back to Menu", "callback_data": "back_to_menu"}])
    
                send_message(chat_id, shop_info, {"inline_keyboard": kb_rows})
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

                    # ✅ Initialize sale state with empty cart
                    user_states[chat_id] = {
                        "action": "awaiting_sale", 
                        "step": 1, 
                        "data": {
                            "selected_shop_id": shop_id,
                            "selected_shop_name": shop.name,
                            "cart": []  # Initialize empty cart
                        }
                    }
        
                    # REMOVED: Incorrect product count check
                    # ProductORM doesn't have shop_id field, so this query was wrong
                    # Instead, check if shop has any stock assigned
        
                    # Check if shop has any stock items
                    stock_count = tenant_db.query(ProductShopStockORM).filter(
                        ProductShopStockORM.shop_id == shop_id
                    ).count()
        
                    if stock_count == 0:
                        send_message(chat_id, f"⚠️ *{shop.name} has no stock assigned yet.*\n\nAdd stock to this shop first or contact admin.")
                        kb_rows = [
                            [{"text": "📦 View Stock", "callback_data": "view_stock"}],
                            [{"text": "⬅️ Choose Another Shop", "callback_data": "record_sale"}],
                            [{"text": "🏠 Main Menu", "callback_data": "back_to_menu"}]
                        ]
                        send_message(chat_id, "What would you like to do?", {"inline_keyboard": kb_rows})
                    else:
                        send_message(chat_id, f"🏪 Shop: {shop.name}\n💰 Record a new sale!\nEnter product name:")
        
                    tenant_db.close()
        
                except (ValueError, IndexError):
                    send_message(chat_id, "❌ Invalid shop selection.")
    
                return {"ok": True}
                        
            elif text == "continue_sale":
                # Get current state
                current_state = user_states.get(chat_id, {})
                current_data = current_state.get("data", {})
    
                if not current_data.get("selected_shop_id"):
                    send_message(chat_id, "❌ No shop selected. Please start over.")
                    user_states.pop(chat_id, None)
                    return {"ok": True}
    
                # Continue with sale even if shop has no stock
                user_states[chat_id] = {
                    "action": "awaiting_sale", 
                    "step": 1, 
                    "data": current_data
                }
                
                # Check if shop actually has stock (inform user)
                tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                if tenant_db:
                    stock_count = tenant_db.query(ProductShopStockORM).filter(
                        ProductShopStockORM.shop_id == current_data["selected_shop_id"]
                    ).count()
                    tenant_db.close()
                    
                    if stock_count == 0:
                        send_message(chat_id, f"⚠️ Warning: This shop has no stock assigned.\nYou can still add products but stock won't be tracked.\n\nEnter product name:")
                        return {"ok": True}
                
                send_message(chat_id, f"💰 Recording sale...\nEnter product name:")
                return {"ok": True}
                    
            # -------------------- Product Selection for Sale --------------------
            elif text.startswith("select_sale:"):
                try:
                    # New format: select_sale:product_id:stock_id (from awaiting_sale handler)
                    # Old format: select_sale:product_id (from other handlers)
                    parts = text.split(":")
                    
                    # ✅ CRITICAL: Get current state to preserve cart
                    current_state = user_states.get(chat_id, {})
                    current_data = current_state.get("data", {})
        
                    logger.info(f"🔍 CART DEBUG [select_sale] - Chat: {chat_id}, Items: {len(current_data.get('cart', []))}")
        
                    # Ensure tenant session is available
                    tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                    if tenant_db is None:
                        send_message(chat_id, "❌ Unable to access tenant database.")
                        return {"ok": True}
                    
                    shop_id = current_data.get("selected_shop_id")
                    if not shop_id:
                        send_message(chat_id, "❌ No shop selected. Please start over.")
                        user_states.pop(chat_id, None)
                        return {"ok": True}
                    
                    if len(parts) >= 3:
                        # New format with stock_id
                        product_id = int(parts[1])
                        stock_id = int(parts[2])
                        
                        # Get product and stock info
                        product = tenant_db.query(ProductORM).filter(ProductORM.product_id == product_id).first()
                        stock_item = tenant_db.query(ProductShopStockORM).filter(
                            ProductShopStockORM.id == stock_id,
                            ProductShopStockORM.shop_id == shop_id
                        ).first()
                        
                    else:
                        # Old format - find stock_id
                        product_id = int(parts[1])
                        product = tenant_db.query(ProductORM).filter(ProductORM.product_id == product_id).first()
                        stock_item = tenant_db.query(ProductShopStockORM).filter(
                            ProductShopStockORM.product_id == product_id,
                            ProductShopStockORM.shop_id == shop_id
                        ).first()
        
                    if not product:
                        send_message(chat_id, "❌ Product not found. Please try again.")
                        return {"ok": True}
                    
                    if not stock_item:
                        send_message(chat_id, f"❌ {product.name} not available in this shop's stock.")
                        return {"ok": True}
        
                    # Store selected product and preserve existing cart
                    current_data["current_product"] = {
                        "product_id": product.product_id,
                        "name": product.name,
                        "price": float(product.price),
                        "unit_type": product.unit_type,
                        "available_stock": stock_item.stock,  # FIXED: Use stock_item.stock
                        "stock_id": stock_item.id  # Store for stock update
                    }
        
                    # Update state with preserved cart and new product
                    user_states[chat_id] = {
                        "action": "awaiting_sale", 
                        "step": 2, 
                        "data": current_data  # This preserves the cart!
                    }
        
                    send_message(chat_id, f"📦 Selected {product.name} ({product.unit_type}). Enter quantity to add:")
        
                except (ValueError, IndexError) as e:
                    logger.error(f"❌ Error in select_sale handler: {e}")
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
                from app.user_management import get_role_based_menu
                kb = get_role_based_menu(user.role)
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
                    
            # -------------------- Record Payment/Credit/Change Collection --------------------
            elif text == "record_payment":
                # Start payment recording flow
                user_states[chat_id] = {"action": "record_payment", "step": 1, "data": {}}
    
                # Ask what type of payment to record
                kb_rows = [
                    [{"text": "💳 Credit Payment", "callback_data": "payment_type:credit"}],
                    [{"text": "🪙 Change Collection", "callback_data": "payment_type:change"}],
                    [{"text": "⬅️ Cancel", "callback_data": "back_to_menu"}]
                ]
    
                send_message(chat_id, "💰 *Record Payment*\n\nSelect payment type:", {"inline_keyboard": kb_rows})
                return {"ok": True}

            elif text.startswith("payment_type:"):
                payment_type = text.split(":")[1]
    
                current_state = user_states.get(chat_id, {})
                current_data = current_state.get("data", {})
                current_data["payment_type"] = payment_type
    
                user_states[chat_id] = {"action": "record_payment", "step": 2, "data": current_data}
    
                if payment_type == "credit":
                    send_message(chat_id, "💳 *Record Credit Payment*\n\nEnter customer name to search:")
                else:  # change
                    send_message(chat_id, "🪙 *Record Change Collection*\n\nEnter customer name to search:")
    
                return {"ok": True}
    
            elif text.startswith("select_customer_payment:"):
                try:
                    parts = text.split(":")
                    customer_id = int(parts[1])
                    payment_type = parts[2]
        
                    current_state = user_states.get(chat_id, {})
                    current_data = current_state.get("data", {})
        
                    # Find the selected customer from results
                    selected_customer = None
                    for customer in current_data.get("customer_results", []):
                        if customer["customer_id"] == customer_id:
                            selected_customer = customer
                            break
        
                    if selected_customer:
                        current_data["selected_customer"] = selected_customer
                        user_states[chat_id] = {"action": "record_payment", "step": 3, "data": current_data}
            
                        # Get tenant session for shop info
                        tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                        if not tenant_db:
                            send_message(chat_id, "❌ Unable to access store database.")
                            return {"ok": True}
            
                        if payment_type == "credit":
                            send_message(chat_id, f"👤 Selected: {selected_customer['name']}\n"
                                                 f"📞 Contact: {selected_customer['contact']}\n"
                                                 f"💳 Total Credit Due: ${selected_customer['total_pending']:.2f}\n"
                                                 f"📊 From {selected_customer['sales_count']} transaction(s)\n\n"
                                                 f"Enter amount received from customer:")
                        else:
                            send_message(chat_id, f"👤 Selected: {selected_customer['name']}\n"
                                                 f"📞 Contact: {selected_customer['contact']}\n"
                                                 f"🪙 Total Change Due: ${selected_customer['total_change']:.2f}\n"
                                                 f"📊 From {selected_customer['sales_count']} transaction(s)\n\n"
                                                 f"Enter amount of change collected from customer:")
        
                    else:
                        send_message(chat_id, "❌ Customer selection failed. Please try again.")
                        user_states.pop(chat_id, None)
    
                except (ValueError, IndexError):
                    send_message(chat_id, "❌ Invalid customer selection.")
                    user_states.pop(chat_id, None)
    
                return {"ok": True}
    
            # -------------------- View Stock --------------------
            elif text == "view_stock":
                tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                if not tenant_db:
                    send_message(chat_id, "⚠️ Cannot view stock: tenant DB unavailable.")
                    return {"ok": True}

                # ✅ FIXED: Get shops based on role
                if user.role == "owner":
                    shops = tenant_db.query(ShopORM).all()
                elif user.role == "admin":
                    # Admin can only see their assigned shop
                    if not user.shop_id:
                        send_message(chat_id, "❌ You are not assigned to any shop.")
                        tenant_db.close()
                        return {"ok": True}
        
                    shop = tenant_db.query(ShopORM).filter(ShopORM.shop_id == user.shop_id).first()
                    if not shop:
                        send_message(chat_id, "❌ Your assigned shop not found.")
                        tenant_db.close()
                        return {"ok": True}
        
                    shops = [shop]
                else:  # shopkeeper
                    # Shopkeeper can only see their assigned shop
                    if not user.shop_id:
                        send_message(chat_id, "❌ You are not assigned to any shop.")
                        tenant_db.close()
                        return {"ok": True}
        
                    shop = tenant_db.query(ShopORM).filter(ShopORM.shop_id == user.shop_id).first()
                    if not shop:
                        send_message(chat_id, "❌ Your assigned shop not found.")
                        tenant_db.close()
                        return {"ok": True}
        
                    shops = [shop]
    
                tenant_db.close()

                if not shops:
                    send_message(chat_id, "🏪 No shops found. Please create a shop first.")
                    return {"ok": True}

                if len(shops) == 1:
                    # Only one shop - show stock directly
                    tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                    stock_list = get_stock_list(tenant_db, shops[0].shop_id)
                    tenant_db.close()

                    # Show appropriate menu based on role
                    if user.role == "owner":
                        kb_rows = [
                            [{"text": "💰 Record Sale", "callback_data": "record_sale"}],
                            [{"text": "📊 Reports", "callback_data": "report_menu"}],
                            [{"text": "⬅️ Back to Menu", "callback_data": "back_to_menu"}]
                        ]
                    else:
                        # Admin/shopkeeper menu
                        kb_rows = [
                            [{"text": "💰 Record Sale", "callback_data": "record_sale"}],
                            [{"text": "📊 Reports", "callback_data": "report_menu"}],
                            [{"text": "⬅️ Back to Menu", "callback_data": "back_to_menu"}]
                        ]
        
                    send_message(chat_id, stock_list, {"inline_keyboard": kb_rows})
                else:
                    # Multiple shops - ask owner to select
                    if user.role != "owner":
                        send_message(chat_id, "❌ You can only view stock for your assigned shop.")
                        return {"ok": True}
        
                    kb_rows = []
                    for shop in shops:
                        kb_rows.append([{
                            "text": f"🏪 {shop.name} {'⭐' if shop.is_main else ''}",
                            "callback_data": f"view_stock_for_shop:{shop.shop_id}"
                        }])
                    kb_rows.append([{"text": "⬅️ Back to Menu", "callback_data": "back_to_menu"}])

                    send_message(chat_id, "🏪 Select shop to view stock:", {"inline_keyboard": kb_rows})

                return {"ok": True}
                    
            # Add this handler right after the view_stock handler:
            elif text.startswith("view_stock_for_shop:"):
                try:
                    shop_id = int(text.split(":")[1])
        
                    tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                    if not tenant_db:
                        send_message(chat_id, "❌ Unable to access store database.")
                        return {"ok": True}
        
                    # Get shop name
                    shop = tenant_db.query(ShopORM).filter(ShopORM.shop_id == shop_id).first()
                    if not shop:
                        send_message(chat_id, "❌ Shop not found.")
                        tenant_db.close()
                        return {"ok": True}
        
                    # Get stock for this shop
                    stock_list = get_stock_list(tenant_db, shop_id)
                    tenant_db.close()
        
                    # Create management buttons for owners
                    if user.role == "owner":
                        kb_rows = [
                            [{"text": "🏪 View Another Shop", "callback_data": "view_stock"}],
                            [{"text": "📊 Manage Shop Stock", "callback_data": f"manage_shop_stock:{shop_id}"}],
                            [{"text": "⬅️ Back to Menu", "callback_data": "back_to_menu"}]
                        ]
                    else:
                        # Non-owners get limited options
                        kb_rows = [
                            [{"text": "⬅️ Back to Menu", "callback_data": "back_to_menu"}]
                        ]
        
                    send_message(chat_id, stock_list, {"inline_keyboard": kb_rows})
        
                except (ValueError, IndexError):
                    send_message(chat_id, "❌ Invalid shop selection.")
    
                return {"ok": True}
        
            # -------------------- Reports Menu --------------------
            elif text == "report_menu":
                # Check if this is a shop-specific report menu
                current_state = user_states.get(chat_id, {})
                is_shop_specific = False
                shop_name = None
    
                if current_state and current_state.get("action") == "awaiting_shop_report":
                    is_shop_specific = True
                    current_data = current_state.get("data", {})
                    shop_name = current_data.get("selected_shop_name")
    
                # Generate appropriate menu
                kb_dict = report_menu_keyboard(user.role, is_shop_specific, shop_name)
    
                # Custom message based on context
                if is_shop_specific and shop_name:
                    message = f"📊 *Reports for {shop_name}*\n\nSelect report type:"
                elif user.role == "owner":
                    # Check if owner has multiple shops
                    tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                    if tenant_db:
                        shops = tenant_db.query(ShopORM).all()
                        tenant_db.close()
            
                        if shops and len(shops) > 1:
                            message = "📊 *Owner Reports*\n\nYou have multiple shops. Select report type:"
                        else:
                            message = "📊 Select a report:"
                    else:
                        message = "📊 Select a report:"
                else:
                    message = "📊 Select a report:"
    
                send_message(chat_id, message, kb_dict)
                return {"ok": True}
    
            # -------------------- Owner Report Selection Callbacks --------------------
            elif text == "report_all_shops" and user.role == "owner":
                # Show standard report menu for all shops
                kb_dict = report_menu_keyboard(user.role)
                send_message(chat_id, "📊 *All Shops Report*\n\nSelect report type:", kb_dict)
                return {"ok": True}

            elif text == "report_select_shop" and user.role == "owner":
                # Show shop selection for specific shop reports
                tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                if not tenant_db:
                    send_message(chat_id, "❌ Unable to access store database.")
                    return {"ok": True}
    
                shops = tenant_db.query(ShopORM).all()
                tenant_db.close()
    
                if not shops:
                    send_message(chat_id, "❌ No shops found.")
                    return {"ok": True}
    
                # Store shops for report selection
                user_states[chat_id] = {
                    "action": "select_shop_for_report",
                    "data": {
                        "shops": [{"shop_id": s.shop_id, "name": s.name} for s in shops]
                    }
                }
    
                kb_rows = []
                for shop in shops:
                    kb_rows.append([{
                        "text": f"🏪 {shop.name} {'⭐' if shop.is_main else ''}",
                        "callback_data": f"select_report_shop:{shop.shop_id}"
                    }])
                kb_rows.append([{"text": "⬅️ Back", "callback_data": "report_menu"}])
    
                send_message(chat_id, "🏪 Select shop for report:", {"inline_keyboard": kb_rows})
                return {"ok": True}

            elif text == "report_compare_shops" and user.role == "owner":
                # Show comparison report options
                tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                if not tenant_db:
                    send_message(chat_id, "❌ Unable to access store database.")
                    return {"ok": True}
    
                shops = tenant_db.query(ShopORM).all()
                tenant_db.close()
    
                if not shops or len(shops) < 2:
                    send_message(chat_id, "❌ Need at least 2 shops for comparison.")
                    return {"ok": True}
    
                # Store shops for comparison
                user_states[chat_id] = {
                    "action": "compare_shops_for_report",
                    "data": {
                        "shops": [{"shop_id": s.shop_id, "name": s.name} for s in shops]
                    }
                }
    
                # Create comparison report menu
                message = "📈 *Shop Comparison Reports*\n\nCompare performance across your shops:\n\n"
                for i, shop in enumerate(shops, 1):
                    message += f"{i}. 🏪 {shop.name} {'⭐' if shop.is_main else ''}\n"
    
                kb_rows = [
                    [{"text": "📅 Compare Daily Sales", "callback_data": "compare_report:daily"}],
                    [{"text": "📊 Compare Weekly Sales", "callback_data": "compare_report:weekly"}],
                    [{"text": "📦 Compare Low Stock", "callback_data": "compare_report:low_stock"}],
                    [{"text": "🏆 Compare Top Products", "callback_data": "compare_report:top_products"}],
                    [{"text": "⬅️ Back to Reports", "callback_data": "report_menu"}]
                ]
    
                send_message(chat_id, message, {"inline_keyboard": kb_rows})
                return {"ok": True}
    
            elif text.startswith("select_report_shop:") and user.role == "owner":
                try:
                    shop_id = int(text.split(":")[1])
        
                    # Store selected shop for report
                    current_state = user_states.get(chat_id, {})
                    current_data = current_state.get("data", {})
                    current_data["selected_shop_id"] = shop_id
        
                    # Get shop name
                    tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                    if tenant_db:
                        shop = tenant_db.query(ShopORM).filter(ShopORM.shop_id == shop_id).first()
                        current_data["selected_shop_name"] = shop.name if shop else f"Shop {shop_id}"
                        tenant_db.close()
        
                    user_states[chat_id] = {"action": "awaiting_shop_report", "data": current_data}
        
                    # Show enhanced report menu for specific shop
                    kb_dict = report_menu_keyboard(user.role, is_shop_specific=True, shop_name=current_data["selected_shop_name"])
                    send_message(chat_id, f"📊 *Reports for {current_data['selected_shop_name']}*\n\nSelect report type:", kb_dict)
        
                except (ValueError, IndexError):
                    send_message(chat_id, "❌ Invalid shop selection.")
    
                return {"ok": True}
    
            elif text.startswith("compare_report:") and user.role == "owner":
                try:
                    report_type = text.split(":")[1]
        
                    current_state = user_states.get(chat_id, {})
                    current_data = current_state.get("data", {})
                    shops_data = current_data.get("shops", [])
        
                    if not shops_data:
                        send_message(chat_id, "❌ No shops data available. Please start over.")
                        user_states.pop(chat_id, None)
                        return {"ok": True}
        
                    tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                    if not tenant_db:
                        send_message(chat_id, "❌ Unable to access store database.")
                        return {"ok": True}
        
                    # Generate comparison report
                    shop_ids = [shop["shop_id"] for shop in shops_data]
                    shop_names = [shop["name"] for shop in shops_data]
        
                    # Use the existing generate_report function with comparison logic
                    # or create a new comparison function
                    report = generate_comparison_report(tenant_db, f"report_{report_type}", shop_ids, shop_names)
        
                    # Add header
                    report_titles = {
                        "daily": "Daily Sales Comparison",
                        "weekly": "Weekly Sales Comparison", 
                        "low_stock": "Low Stock Comparison",
                        "top_products": "Top Products Comparison"
                    }
        
                    title = report_titles.get(report_type, "Shop Comparison")
                    final_report = f"📈 *{title}*\n\n{report}"
        
                    send_message(chat_id, final_report)
        
                    # Add option to view another comparison
                    kb_rows = [
                        [{"text": "📈 Another Comparison", "callback_data": "report_compare_shops"}],
                        [{"text": "⬅️ Back to Reports", "callback_data": "report_menu"}],
                        [{"text": "🏠 Main Menu", "callback_data": "back_to_menu"}]
                    ]
        
                    send_message(chat_id, "What would you like to do next?", {"inline_keyboard": kb_rows})
        
                except Exception as e:
                    logger.error(f"❌ Comparison report error: {e}")
                    send_message(chat_id, f"❌ Error generating comparison report: {str(e)}")
    
                return {"ok": True}
    
            # Add this to the report callbacks section temporarily
            elif text == "debug_sales_data" and user.role == "owner":
                tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                if tenant_db:
                    debug_report = debug_sales_data(tenant_db)
                    send_message(chat_id, debug_report)
                return {"ok": True}
    
            # -------------------- Report Callbacks (UPDATED FOR MULTI-SHOP) --------------------
            elif text in ["report_daily", "report_weekly", "report_monthly", "report_low_stock", 
                          "report_top_products", "report_aov", "report_stock_turnover", 
                          "report_credits", "report_change", "report_payment_summary"]:

                logger.info(f"🎯 Processing callback: {text} from chat_id={chat_id}, role={user.role}")

                tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                if tenant_db is None:
                    send_message(chat_id, "❌ Unable to access tenant database.")
                    return {"ok": True}

                try:
                    # ✅ ENHANCED: Check if owner selected a specific shop
                    shop_id = None
                    shop_name = None
                    report_title = text.replace('_', ' ').title()
        
                    # Check if owner is viewing a specific shop report
                    current_state = user_states.get(chat_id, {})
                    if current_state and current_state.get("action") == "awaiting_shop_report":
                        current_data = current_state.get("data", {})
                        shop_id = current_data.get("selected_shop_id")
                        shop_name = current_data.get("selected_shop_name")
            
                        if shop_id:
                            # Clear the state after use
                            user_states.pop(chat_id, None)
                            logger.info(f"📊 Owner generating {text} for specific shop: {shop_name} (ID: {shop_id})")
                    elif user.role in ["admin", "shopkeeper"]:
                        # Admin/Shopkeeper can only see reports for their assigned shop
                        shop_id = user.shop_id
                        shop_name = user.shop_name or f"Shop {shop_id}"
            
                        if not shop_id:
                            send_message(chat_id, "❌ You are not assigned to any shop. Contact the owner.")
                            return {"ok": True}
            
                        logger.info(f"📊 {user.role.title()} '{user.username}' generating {text} for shop {shop_name} (ID: {shop_id})")
                    else:
                        # Owner viewing all shops (default)
                        logger.info(f"📊 Owner '{user.username}' generating {text} for all shops")
                        report_title = f"All Shops - {report_title}"

                    # ✅ Generate report with shop filtering (if specified)
                    report = generate_report(tenant_db, text, shop_id=shop_id, shop_name=shop_name)
        
                    # Add header based on scope
                    if shop_name:
                        final_report = f"📊 *{shop_name} - {report_title}*\n\n{report}"
                    else:
                        final_report = f"📊 *{report_title}*\n\n{report}"

                    # Send the report
                    send_message(chat_id, final_report)

                    # Log successful generation
                    logger.info(f"✅ Report '{text}' generated successfully for chat_id={chat_id}")

                except Exception as e:
                    logger.error(f"❌ {text} failed for chat_id={chat_id}: {e}")
                    import traceback
                    traceback.print_exc()

                    error_msg = f"❌ Failed to generate {text.replace('_', ' ')}."
                    if "division by zero" in str(e):
                        error_msg += "\n\nℹ️ No data available for this report period."
                    elif "relation" in str(e) and "does not exist" in str(e):
                        error_msg += "\n\nℹ️ Database tables not initialized. Please contact support."

                    send_message(chat_id, error_msg)

                return {"ok": True}
        
            # Handle back to menu
            elif text == "back_to_menu":
                logger.info(f"🎯 Processing callback: back_to_menu from chat_id={chat_id}")
    
                from app.user_management import get_role_based_menu
                kb = get_role_based_menu(user.role)
                send_message(chat_id, "🏠 Main Menu:", keyboard=kb)
                return {"ok": True}
    
            # -------------------- Logout Callback --------------------
            elif text == "logout":
                logger.info(f"🚪 User {user.username} (chat_id={chat_id}) logging out")
    
                # Clear user session
                try:
                    # Clear chat_id from user record (soft logout)
                    user.chat_id = None
                    db.commit()
        
                    # Clear any user states
                    user_states.pop(chat_id, None)
        
                    # Send logout confirmation
                    send_message(chat_id, "✅ You have been logged out successfully.\n\nUse /start to login again.")
        
                    logger.info(f"✅ User {user.username} logged out successfully")
        
                except Exception as e:
                    logger.error(f"❌ Error during logout: {e}")
                    send_message(chat_id, "❌ Error during logout. Please try again.")
    
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
                    # ✅ CASE: User already exists and chat_id is linked
                    role_display = {
                        "owner": "👑 Owner",
                        "admin": "🛡️ Admin", 
                        "shopkeeper": "👨‍💼 Shopkeeper"
                    }

                    welcome_msg = f"👋 Welcome back, {user.name}!\n"
                    welcome_msg += f"👤 Role: {role_display.get(user.role, user.role)}"

                    if user.role in ["admin", "shopkeeper"] and user.shop_name:
                        welcome_msg += f"\n🏪 Shop: {user.shop_name}"

                    # Show role-based menu immediately
                    from app.user_management import get_role_based_menu
                    kb = get_role_based_menu(user.role)
                    send_message(chat_id, welcome_msg, keyboard=kb)

                else:
                    # ✅ CASE: New user OR staff without linked chat_id
                    # Fresh start - ask for role
                    kb_rows = [
                        [{"text": "👑 I'm a Shop Owner", "callback_data": "user_type:owner"}],
                        [{"text": "👤 I'm a Shop User", "callback_data": "user_type:shop_user"}]
                    ]
                    send_message(chat_id, 
                                "👋 Welcome! Please select your role:\n\n"
                                "• 👑 **Shop Owner** - Create your own store\n"
                                "• 👤 **Shop User** - Already have credentials (Admin/Shopkeeper)", 
                                {"inline_keyboard": kb_rows})

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

                    # ✅ UPDATED: Create tenant schema WITHOUT default users
                    try:
                        # ✅ Updated: create_tenant_db returns only owner credentials now
                        schema_name, credentials_dict = create_tenant_db(chat_id)
                        new_user.tenant_schema = schema_name
                        # ❌ REMOVE: new_user.shop_id = 1  # No default shop ID yet
                        db.commit()
                        logger.info(f"✅ New owner created: {generated_username} with schema '{schema_name}'")

                        # ✅ Send ONLY owner credentials
                        send_owner_credentials(chat_id, generated_username, generated_password)

                        # ✅ REMOVED: Don't send default users credentials (they don't exist yet)
                        # Default users will be created when owner creates their first shop

                    except Exception as e:
                        logger.error(f"❌ Failed to create tenant schema: {e}")
                        send_message(chat_id, "❌ Could not initialize store database.")
                        return {"ok": True}

                    # ✅ Start FIRST shop setup (not just any shop)
                    send_message(chat_id, "🏪 Let's set up your FIRST shop! Please enter the shop name:")
                    user_states[chat_id] = {"action": "setup_first_shop", "step": 1, "data": {}}

                else:  # shop_user (admin or shopkeeper)
                    # Step-by-step shop user login
                    send_message(chat_id, "👤 Please enter your username:")
                    user_states[chat_id] = {"action": "shop_user_login", "step": 1, "data": {}}

                return {"ok": True}
    
    
            # -------------------- Login Flow --------------------
            if chat_id in user_states:
                state = user_states[chat_id]
                action = state.get("action")
                step = state.get("step", 1)
                data = state.get("data", {})

                # ✅ SHOP USER LOGIN (for admin/shopkeeper users - first time linking chat_id)
                if action == "shop_user_login":
                    if step == 1:  # Enter Username
                        username = text.strip()
                        if not username:
                            send_message(chat_id, "❌ Username cannot be empty. Please enter your username:")
                            return {"ok": True}

                        # Check if username exists and is NOT an owner (admin or shopkeeper only)
                        candidate = db.query(User).filter(
                            User.username == username,
                            User.role.in_(["admin", "shopkeeper"])  # Only allow admin/shopkeeper
                        ).first()

                        if not candidate:
                            send_message(chat_id, "❌ Username not found or invalid user type. Please try again:")
                            return {"ok": True}

                        # Store username and move to password step
                        data["username"] = username
                        data["candidate_user_id"] = candidate.user_id
                        user_states[chat_id] = {"action": "shop_user_login", "step": 2, "data": data}
                        send_message(chat_id, "🔐 Please enter your password:")

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

                        # ✅ IMPORTANT: Make sure verify_password is imported
                        # Add this at the top of your file if not already there:
                        # from app.user_management import verify_password
        
                        if not verify_password(password, candidate.password_hash):
                            send_message(chat_id, "❌ Incorrect password. Please try again:")
                            return {"ok": True}

                        # ✅ CRITICAL: Check if user is already logged in elsewhere
                        if candidate.chat_id and candidate.chat_id != chat_id:
                            # User is logged in from another device - ask if they want to switch
                            send_message(chat_id, "⚠️ This account is already logged in from another device. Do you want to switch to this device? (yes/no)")
                            data["existing_chat_id"] = candidate.chat_id
                            user_states[chat_id] = {"action": "shop_user_login", "step": 3, "data": data}
                            return {"ok": True}
        
                        # ✅ Login successful - link Telegram chat_id
                        candidate.chat_id = chat_id
                        db.commit()

                        # Welcome message
                        role_display = {
                            "admin": "🛡️ Admin (Full Access)",
                            "shopkeeper": "👨‍💼 Shopkeeper (Limited Access)"
                        }
                        welcome_msg = f"✅ Login successful! Welcome, {candidate.name}.\n"
                        welcome_msg += f"👤 Role: {role_display.get(candidate.role, candidate.role)}"

                        # Add shop info if available
                        if candidate.shop_name:
                            welcome_msg += f"\n🏪 Shop: {candidate.shop_name}"

                        send_message(chat_id, welcome_msg)
                        user_states.pop(chat_id, None)

                        # Show role-based menu
                        from app.user_management import get_role_based_menu
                        kb = get_role_based_menu(candidate.role)
                        send_message(chat_id, "🏠 Main Menu:", keyboard=kb)
        
                    elif step == 3:  # Handle switching devices
                        confirmation = text.strip().lower()
                        if confirmation == "yes":
                            # Get candidate again
                            candidate = db.query(User).filter(User.user_id == data["candidate_user_id"]).first()
                            if candidate:
                                # Switch chat_id to current device
                                candidate.chat_id = chat_id
                                db.commit()
                
                                send_message(chat_id, "✅ Device switched successfully!")
                
                                # Show role-based menu
                                from app.user_management import get_role_based_menu
                                kb = get_role_based_menu(candidate.role)
                                send_message(chat_id, "🏠 Main Menu:", keyboard=kb)
                            else:
                                send_message(chat_id, "❌ User not found. Please start over.")
                        else:
                            send_message(chat_id, "❌ Login cancelled. Account remains on previous device.")
        
                        user_states.pop(chat_id, None)
                        return {"ok": True}

                    return {"ok": True}
                            
                # -------------------- Unified Shop Setup/Update (Owner only) --------------------
                elif action == "setup_shop" and user.role == "owner":  # CHANGED: "owner" only, not "owner, admin"
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

                            # ✅ UPDATED: Use tenant_db helpers for shop creation
                            if is_main:
                                new_shop = create_initial_shop(tenant_db, data["name"], data.get("location", ""), data.get("contact", ""))
                            else:
                                new_shop = create_additional_shop(tenant_db, data["name"], data.get("location", ""), data.get("contact", ""))

                            if not new_shop:
                                send_message(chat_id, "❌ Failed to create shop. Please try again.")
                                user_states.pop(chat_id, None)
                                return {"ok": True}

                            success_msg = f"✅ Shop {'created' if is_main else 'added'} successfully!\n\n"
                            success_msg += f"🏪 *{new_shop.name}*\n"
                            if data.get("location"):
                                success_msg += f"📍 {data['location']}\n"
                            if data.get("contact"):
                                success_msg += f"📞 {data['contact']}\n"
                            if new_shop.is_main:
                                success_msg += f"⭐ *Set as Main Store*\n"

                            # ✅ Create shop-specific users (ONLY owner can create default users)
                            credentials = create_shop_users(chat_id, new_shop.shop_id, new_shop.name)
            
                            if credentials:
                                # Send credentials via notifications
                                from app.telegram_notifications import send_new_user_credentials
                
                                # Send admin credentials
                                admin_data = credentials.get("admin", {})
                                if admin_data:
                                    send_new_user_credentials(
                                        chat_id, 
                                        "admin", 
                                        admin_data["username"], 
                                        admin_data["password"], 
                                        admin_data["email"],
                                        new_shop.name
                                    )
                
                                # Send shopkeeper credentials  
                                shopkeeper_data = credentials.get("shopkeeper", {})
                                if shopkeeper_data:
                                    send_new_user_credentials(
                                        chat_id,
                                        "shopkeeper",
                                        shopkeeper_data["username"],
                                        shopkeeper_data["password"],
                                        shopkeeper_data["email"],
                                        new_shop.name
                                    )
                
                                success_msg += f"\n👥 *Default users created for this shop!*\n"
                                success_msg += f"Check messages above for credentials to share with staff."
                            else:
                                success_msg += f"\n⚠️ Could not create default users. You can create them later via 'Manage Users'."

                            send_message(chat_id, success_msg)

                        except Exception as e:
                            logger.error(f"❌ Error saving shop: {e}")
                            send_message(chat_id, "❌ Failed to save shop. Please try again.")

                        # Clear state and return to menu
                        user_states.pop(chat_id, None)
                        from app.user_management import get_role_based_menu
                        kb = get_role_based_menu(user.role)
                        send_message(chat_id, "🏠 Main Menu:", keyboard=kb)

                    return {"ok": True}
    

                # -------------------- Update Existing Shop (Owner only) --------------------
                elif action == "update_existing_shop" and user.role == "owner":  # CHANGED: owner only
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
                        from app.user_management import get_role_based_menu
                        kb = get_role_based_menu(user.role)
                        send_message(chat_id, "🏠 Main Menu:", keyboard=kb)

                    return {"ok": True}

                # -------------------- User Creation Flow (Owner/Admin only) --------------------
                elif action == "create_user" and user.role in ["owner", "admin"]:
                    if step == 1:  # Select Role
                        role_selection = text.strip().lower()
        
                        # ✅ RESTRICTION: Admin can only create shopkeepers
                        if user.role == "admin" and role_selection != "shopkeeper":
                            send_message(chat_id, "❌ Admins can only create shopkeepers, not other admins.")
                            user_states.pop(chat_id, None)
                            return {"ok": True}
            
                        if role_selection not in ["admin", "shopkeeper"]:
                            send_message(chat_id, "❌ Please select a valid role: 'admin' or 'shopkeeper'")
                            return {"ok": True}
                    
                        data["role"] = role_selection
                        user_states[chat_id] = {"action": action, "step": 2, "data": data}
        
                        # Get shops for selection
                        tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                        if not tenant_db:
                            send_message(chat_id, "❌ Unable to access store database.")
                            user_states.pop(chat_id, None)
                            return {"ok": True}

                        # ✅ DIFFERENT LOGIC FOR ADMIN vs OWNER
                        if user.role == "admin":
                            # Admin can only create users for their own shop
                            shop = tenant_db.query(ShopORM).filter(ShopORM.shop_id == user.shop_id).first()
                            if not shop:
                                send_message(chat_id, "❌ You are not assigned to any shop.")
                                user_states.pop(chat_id, None)
                                return {"ok": True}
            
                            # Auto-select admin's shop and skip to step 3 (username)
                            data["selected_shop_id"] = user.shop_id
                            data["shop_name"] = user.shop_name
                            user_states[chat_id] = {"action": action, "step": 3, "data": data}
                            send_message(chat_id, f"👤 Creating {role_selection} for {user.shop_name}\nEnter username:")

                        elif user.role == "owner":
                            # Owner sees all shops
                            shops = tenant_db.query(ShopORM).all()
                            tenant_db.close()

                            if not shops:
                                send_message(chat_id, "❌ No shops found. Please create a shop first.")
                                user_states.pop(chat_id, None)
                                return {"ok": True}

                            # Create shop selection keyboard for owner
                            kb_rows = []
                            for shop in shops:
                                kb_rows.append([{"text": f"🏪 {shop.name}", "callback_data": f"select_shop_for_user:{shop.shop_id}"}])
                            kb_rows.append([{"text": "⬅️ Cancel", "callback_data": "back_to_menu"}])

                            send_message(chat_id, f"🏪 Select shop for new {role_selection}:", {"inline_keyboard": kb_rows})
                            
                    elif step == 2:  # Shop selected via callback (handled separately)
                        # This will be handled by the callback
                        pass
    
                    elif step == 3:  # Custom name (optional)
                        custom_name = text.strip()
                        if custom_name.lower() == "skip":
                            custom_name = None
        
                        shop_id = data.get("selected_shop_id")
                        role = data.get("role")
        
                        if not shop_id or not role:
                            send_message(chat_id, "❌ Missing information. Please start over.")
                            user_states.pop(chat_id, None)
                            return {"ok": True}
        
                        # Create the user
                        from app.user_management import create_custom_user
        
                        result = create_custom_user(db, user.tenant_schema, shop_id, role, custom_name)
        
                        if result:
                            from app.user_management import format_user_credentials_message
            
                            credentials_msg = format_user_credentials_message({role: result})
                            send_message(chat_id, credentials_msg)
                        else:
                            send_message(chat_id, f"❌ Failed to create {role} user. Please try again.")
        
                        # Clear state and return to menu
                        user_states.pop(chat_id, None)
                        from app.user_management import get_role_based_menu
                        kb = get_role_based_menu(user.role)
                        send_message(chat_id, "🏠 Main Menu:", keyboard=kb)
    
                    return {"ok": True}

                # -------------------- Add Shop Flow (Owner only) --------------------
                elif action == "add_shop" and user.role == "owner":  # CHANGED: owner only
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
                            # Create additional shop (not main)
                            new_shop = create_additional_shop(tenant_db, data["name"], data["location"], data["contact"])

                            if not new_shop:
                                send_message(chat_id, "❌ Failed to add shop. Please try again.")
                                user_states.pop(chat_id, None)
                                return {"ok": True}

                            success_msg = f"✅ Shop added successfully!\n\n"
                            success_msg += f"🏪 *{new_shop.name}*\n"
                            success_msg += f"📍 {new_shop.location}\n"
                            success_msg += f"📞 {new_shop.contact}\n"

                            # ✅ Create shop-specific users
                            credentials = create_shop_users(chat_id, new_shop.shop_id, new_shop.name)
            
                            if credentials:
                                # Send credentials via notifications
                                from app.telegram_notifications import send_new_user_credentials
                
                                # Send admin credentials
                                admin_data = credentials.get("admin", {})
                                if admin_data:
                                    send_new_user_credentials(
                                        chat_id, 
                                        "admin", 
                                        admin_data["username"], 
                                        admin_data["password"], 
                                        admin_data["email"],
                                        new_shop.name
                                    )
                
                                # Send shopkeeper credentials  
                                shopkeeper_data = credentials.get("shopkeeper", {})
                                if shopkeeper_data:
                                    send_new_user_credentials(
                                        chat_id,
                                        "shopkeeper",
                                        shopkeeper_data["username"],
                                        shopkeeper_data["password"],
                                        shopkeeper_data["email"],
                                        new_shop.name
                                    )
                
                                success_msg += f"\n👥 *Default users created for this shop!*\n"
                                success_msg += f"Check messages above for credentials to share with staff."
                            else:
                                success_msg += f"\n⚠️ Could not create default users. You can create them later via 'Manage Users'."

                            send_message(chat_id, success_msg)

                        except Exception as e:
                            logger.error(f"❌ Error adding shop: {e}")
                            send_message(chat_id, "❌ Failed to add shop. Please try again.")

                        # Clear state and return to menu
                        user_states.pop(chat_id, None)
                        from app.user_management import get_role_based_menu
                        kb = get_role_based_menu(user.role)
                        send_message(chat_id, "🏠 Main Menu:", keyboard=kb)

                    return {"ok": True}
    
                elif action == "confirm_delete_user":
                    confirmation = text.strip().upper()
    
                    if confirmation == "YES":
                        username = data.get("username")
        
                        # Delete the user
                        from app.user_management import delete_user
                        if delete_user(username):
                            send_message(chat_id, f"✅ User `{username}` deleted successfully.")
                        else:
                            send_message(chat_id, f"❌ Failed to delete user `{username}`.")
                    else:
                        send_message(chat_id, "❌ Deletion cancelled.")
    
                    user_states.pop(chat_id, None)
                    from app.user_management import get_role_based_menu
                    kb = get_role_based_menu(user.role)
                    send_message(chat_id, "🏠 Main Menu:", keyboard=kb)
                    return {"ok": True}
    
                # ==================== STEP 2: ADMIN SHOPKEEPER DELETION CONFIRMATION ====================
                elif text == "confirm_delete_shopkeeper_admin":
                    confirmation = text.strip().upper()
    
                    if confirmation == "YES":
                        username = data.get("username")
                        
                        if not username:
                            send_message(chat_id, "❌ Error: Username not found in state.")
                            user_states.pop(chat_id, None)
                            return {"ok": True}
                        
                        # Get admin user to verify shop assignment
                        admin_user = db.query(User).filter(User.chat_id == chat_id).first()
                        if not admin_user or admin_user.role != 'admin':
                            send_message(chat_id, "❌ Unauthorized: Admin access required.")
                            user_states.pop(chat_id, None)
                            return {"ok": True}
                        
                        try:
                            # Find and delete the shopkeeper (must be in admin's shop)
                            shopkeeper = db.query(User).filter(
                                User.username == username,
                                User.tenant_schema == admin_user.tenant_schema,
                                User.shop_id == admin_user.shop_id,
                                User.role == 'shopkeeper'
                            ).first()
                            
                            if shopkeeper:
                                # Delete the shopkeeper
                                db.delete(shopkeeper)
                                db.commit()
                                
                                send_message(chat_id, f"✅ Shopkeeper `{username}` has been successfully deleted!")
                            else:
                                send_message(chat_id, f"❌ Shopkeeper `{username}` not found or doesn't belong to your shop.")
                        
                        except Exception as e:
                            db.rollback()
                            logging.error(f"Error deleting shopkeeper: {e}")
                            send_message(chat_id, f"❌ An error occurred while deleting the shopkeeper: {e}")
                    
                    else:  # NO
                        send_message(chat_id, "✅ Deletion cancelled. The shopkeeper was not deleted.")
                    
                    # Clear state and show admin menu
                    user_states.pop(chat_id, None)
                    
                    # Show admin menu
                    from app.user_management import get_role_based_menu
                    kb = get_role_based_menu('admin')
                    send_message(chat_id, "🛡️ Admin Menu:", keyboard=kb)
                    return {"ok": True}
                # ==================== END STEP 2 ====================
                
                # -------------------- Admin Create Shopkeeper Flow --------------------
                elif action == "create_shopkeeper_admin" and user.role == "admin":
                    if step == 1:  # Enter username
                        username = text.strip()
                        if not username:
                            send_message(chat_id, "❌ Username cannot be empty. Enter username:")
                            return {"ok": True}
        
                        # Check if username already exists in this tenant
                        existing_user = db.query(User).filter(
                            User.username == username,
                            User.tenant_schema == user.tenant_schema
                        ).first()
        
                        if existing_user:
                            send_message(chat_id, f"❌ Username '{username}' already exists. Try another:")
                            return {"ok": True}
        
                        data["username"] = username
                        user_states[chat_id] = {"action": action, "step": 2, "data": data}
                        send_message(chat_id, "👤 Enter name for shopkeeper (press Enter to skip):")
    
                    elif step == 2:  # Enter name (optional)
                        name = text.strip()
                        if name:
                            data["name"] = name
        
                        # Generate credentials
                        from app.user_management import generate_password, hash_password
        
                        password = generate_password()
                        email = f"{data['username']}_{int(time.time())}@example.com"
        
                        # Create shopkeeper user
                        new_shopkeeper = User(
                            username=data["username"],
                            name=data.get("name", data["username"]),
                            email=email,
                            password_hash=hash_password(password),
                            role="shopkeeper",
                            tenant_schema=user.tenant_schema,
                            shop_id=user.shop_id,
                            shop_name=user.shop_name,
                            created_by=user.username
                        )
        
                        db.add(new_shopkeeper)
                        db.commit()
        
                        # Send credentials
                        success_msg = f"✅ **Shopkeeper Created Successfully!**\n\n"
                        success_msg += f"🏪 Shop: {user.shop_name}\n"
                        success_msg += f"👤 Username: `{new_shopkeeper.username}`\n"
                        success_msg += f"🔑 Password: `{password}`\n\n"
                        success_msg += "Share these credentials with the shopkeeper."
        
                        send_message(chat_id, success_msg)
        
                        # Clear state
                        user_states.pop(chat_id, None)
        
                        # Return to admin user management
                        kb_rows = [
                            [{"text": "➕ Create Another", "callback_data": "create_shopkeeper_admin"}],
                            [{"text": "🔙 Back to User Management", "callback_data": "manage_users_admin"}]
                        ]
                        send_message(chat_id, "What would you like to do next?", {"inline_keyboard": kb_rows})
    
                    return {"ok": True}
    
                # -------------------- Add Product --------------------
                elif action == "awaiting_product":
                    # Add comprehensive debug
                    print(f"🔍 DEBUG [awaiting_product]: Action triggered")
                    print(f"  Step: {step}")
                    print(f"  Text received: '{text}'")
                    print(f"  Data keys: {list(data.keys())}")
                    print(f"  Shop ID in data: {data.get('shop_id')}")
                    print(f"  Shop Name in data: {data.get('shop_name')}")
    
                    tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                    if tenant_db is None:
                        print(f"❌ DEBUG: Failed to get tenant session")
                        send_message(chat_id, "❌ Unable to access tenant database.")
                        return {"ok": True}
    
                    print(f"✅ DEBUG: Tenant session obtained")

                    # -------------------- Step Handling --------------------
                    if step == 1:  # Product Name
                        product_name = text.strip()
                        if not product_name:
                            send_message(chat_id, "❌ Product name cannot be empty. Please enter a valid product name:")
                            return {"ok": True}
                        data["name"] = product_name
                        user_states[chat_id] = {"action": action, "step": 2, "data": data}
                        send_message(chat_id, "📦 Enter quantity:")
                        print(f"🔍 DEBUG: Product name saved, moving to step 2")
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
                            print(f"🔍 DEBUG: Quantity saved, moving to step 3")
                        except ValueError:
                            send_message(chat_id, "❌ Invalid quantity. Please enter a positive number:")
                        return {"ok": True}

                    elif step == 3:  # Unit Type - SIMPLIFIED
                        unit_type = text.strip().lower()
                        if not unit_type:
                            send_message(chat_id, "❌ Unit type cannot be empty. Please enter a unit type:")
                            return {"ok": True}
        
                        # Simple validation - just check it's not just a number
                        if unit_type.isdigit():
                            send_message(chat_id, "❌ Unit type cannot be just numbers. Examples: piece, box, kg:")
                            return {"ok": True}
        
                        data["unit_type"] = unit_type
                        user_states[chat_id] = {"action": action, "step": 4, "data": data}
                        send_message(chat_id, "💲 Enter product price:")
                        print(f"🔍 DEBUG: Unit type '{unit_type}' saved, moving to step 4")
                        return {"ok": True}

                    elif step == 4:  # Price
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
                            print(f"🔍 DEBUG: Price saved, moving to step 5")
                        except ValueError:
                            send_message(chat_id, "❌ Invalid price. Please enter a positive number:")
                        return {"ok": True}

                    elif step == 5:  # Min Stock Level
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
                            send_message(chat_id, "⚠️ Enter low stock threshold (e.g., 5):")
                            print(f"🔍 DEBUG: Min stock saved, moving to step 6")
                        except ValueError:
                            send_message(chat_id, "❌ Invalid number. Please enter a valid minimum stock level:")
                        return {"ok": True}

                    elif step == 6:  # Low Stock Threshold
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

                            print(f"🔍 DEBUG: Calling add_product function with data: {data}")
                            # ✅ CORRECT: Call add_product with the right parameters (no 'user' parameter)
                            from app.routes.telegram import add_product
                            result = add_product(tenant_db, chat_id, data)
            
                            if result is None:  # add_product returns None on success (sends message itself)
                                success_msg = f"✅ Product *{data['name']}* added successfully!"
                                if data.get('shop_name'):
                                    success_msg += f"\n🏪 Shop: {data['shop_name']}"
                                send_message(chat_id, success_msg)
                            else:
                                # If add_product returned something (error), it already sent message
                                pass
                
                            user_states.pop(chat_id, None)
                            print(f"🔍 DEBUG: Product saved, clearing state")
            
                            # Return to main menu
                            from app.user_management import get_role_based_menu
                            kb = get_role_based_menu(user.role)
                            send_message(chat_id, "🏠 Main Menu:", keyboard=kb)
            
                        except ValueError as e:
                            send_message(chat_id, f"❌ Invalid number: {e}")
                        except Exception as e:
                            print(f"❌ DEBUG: Exception in step 6: {e}")
                            import traceback
                            traceback.print_exc()
                            send_message(chat_id, f"❌ Error saving product: {e}")
                        return {"ok": True}

                    else:
                        print(f"❌ DEBUG: Unknown step {step} in awaiting_product")
                        send_message(chat_id, "❌ Invalid step in product creation. Please start over.")
                        user_states.pop(chat_id, None)
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

                        # Store product matches (without stock yet - we need shop first)
                        data["product_matches"] = [
                            {
                                "product_id": p.product_id,
                                "name": p.name
                            } for p in matches
                        ]
        
                        # If owner with multiple shops, ask which shop
                        if user.role == "owner":
                            # Get all shops
                            shops = tenant_db.query(ShopORM).all()
                            if not shops:
                                send_message(chat_id, "❌ No shops found in the system.")
                                user_states.pop(chat_id, None)
                                return {"ok": True}
            
                            if len(shops) == 1:
                                # Only one shop - use it automatically
                                data["selected_shop_id"] = shops[0].shop_id
                                data["selected_shop_name"] = shops[0].name
                
                                # Show product selection
                                if len(data["product_matches"]) == 1:
                                    # Single match - proceed directly to quantity
                                    product = data["product_matches"][0]
                                    data["selected_product"] = product
                                    user_states[chat_id] = {"action": "quick_stock_update", "step": 2, "data": data}
                    
                                    # Get current stock for this shop
                                    stock_item = tenant_db.query(ProductShopStockORM).filter(
                                        ProductShopStockORM.product_id == product["product_id"],
                                        ProductShopStockORM.shop_id == shops[0].shop_id
                                    ).first()
                                    current_stock = stock_item.stock if stock_item else 0
                    
                                    send_message(chat_id, f"📦 Selected: {product['name']}\n🏪 Shop: {shops[0].name}\n📊 Current stock: {current_stock}\n\nEnter quantity to ADD to stock:")
                                else:
                                    # Multiple matches - show selection
                                    user_states[chat_id] = {"action": "quick_stock_update", "step": 1.5, "data": data}
                    
                                    kb_rows = []
                                    for product in data["product_matches"]:
                                        # Get current stock for this shop
                                        stock_item = tenant_db.query(ProductShopStockORM).filter(
                                            ProductShopStockORM.product_id == product["product_id"],
                                            ProductShopStockORM.shop_id == shops[0].shop_id
                                        ).first()
                                        current_stock = stock_item.stock if stock_item else 0
                        
                                        kb_rows.append([{
                                            "text": f"{product['name']} (Stock: {current_stock})",
                                            "callback_data": f"select_stock_product:{product['product_id']}"
                                        }])
                                    kb_rows.append([{"text": "❌ Cancel", "callback_data": "cancel_quick_stock"}])
                    
                                    send_message(chat_id, f"🏪 Shop: {shops[0].name}\n🔍 Multiple products found. Select one:", {"inline_keyboard": kb_rows})
                            else:
                                # Multiple shops - ask owner to select
                                user_states[chat_id] = {"action": "quick_stock_update", "step": 1.5, "data": data}
                
                                kb_rows = []
                                for shop in shops:
                                    kb_rows.append([{
                                        "text": f"🏪 {shop.name} {'⭐' if shop.is_main else ''}",
                                        "callback_data": f"select_shop_for_quick_stock:{shop.shop_id}"
                                    }])
                                kb_rows.append([{"text": "❌ Cancel", "callback_data": "cancel_quick_stock"}])
                
                                send_message(chat_id, "🏪 Select which shop to update stock for:", {"inline_keyboard": kb_rows})
        
                        else:  # shopkeeper or admin
                            # Non-owners use their assigned shop
                            if not hasattr(user, 'shop_id') or user.shop_id is None:
                                send_message(chat_id, "❌ You are not assigned to any shop. Please contact owner.")
                                user_states.pop(chat_id, None)
                                return {"ok": True}
            
                            data["selected_shop_id"] = user.shop_id
                            # Get shop name for display
                            shop = tenant_db.query(ShopORM).filter(ShopORM.shop_id == user.shop_id).first()
                            data["selected_shop_name"] = shop.name if shop else "Your Shop"
            
                            # Show product selection
                            if len(data["product_matches"]) == 1:
                                # Single match - proceed directly
                                product = data["product_matches"][0]
                                data["selected_product"] = product
                                user_states[chat_id] = {"action": "quick_stock_update", "step": 2, "data": data}
                
                                # Get current stock for this shop
                                stock_item = tenant_db.query(ProductShopStockORM).filter(
                                    ProductShopStockORM.product_id == product["product_id"],
                                    ProductShopStockORM.shop_id == user.shop_id
                                ).first()
                                current_stock = stock_item.stock if stock_item else 0
                
                                send_message(chat_id, f"📦 Selected: {product['name']}\n🏪 Shop: {data['selected_shop_name']}\n📊 Current stock: {current_stock}\n\nEnter quantity to ADD to stock:")
                            else:
                                # Multiple matches - show selection
                                user_states[chat_id] = {"action": "quick_stock_update", "step": 1.5, "data": data}
                
                                kb_rows = []
                                for product in data["product_matches"]:
                                    # Get current stock for this shop
                                    stock_item = tenant_db.query(ProductShopStockORM).filter(
                                        ProductShopStockORM.product_id == product["product_id"],
                                        ProductShopStockORM.shop_id == user.shop_id
                                    ).first()
                                    current_stock = stock_item.stock if stock_item else 0
                    
                                    kb_rows.append([{
                                        "text": f"{product['name']} (Stock: {current_stock})",
                                        "callback_data": f"select_stock_product:{product['product_id']}"
                                    }])
                                kb_rows.append([{"text": "❌ Cancel", "callback_data": "cancel_quick_stock"}])
                
                                send_message(chat_id, f"🏪 Shop: {data['selected_shop_name']}\n🔍 Multiple products found. Select one:", {"inline_keyboard": kb_rows})
                        return {"ok": True}

                    # STEP 2: Enter quantity to add
                    elif step == 2:
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
                            shop_id = data.get("selected_shop_id")
            
                            if not shop_id:
                                send_message(chat_id, "❌ No shop selected. Please start over.")
                                user_states.pop(chat_id, None)
                                return {"ok": True}

                            # Get shop name for display
                            shop = tenant_db.query(ShopORM).filter(ShopORM.shop_id == shop_id).first()
                            shop_name = shop.name if shop else f"Shop ID: {shop_id}"
            
                            if user.role == "owner":
                                # Owner can update directly
                                # Get or create shop-specific stock record
                                stock_item = tenant_db.query(ProductShopStockORM).filter(
                                    ProductShopStockORM.product_id == product["product_id"],
                                    ProductShopStockORM.shop_id == shop_id
                                ).first()
                
                                if not stock_item:
                                    # Create stock record if it doesn't exist
                                    stock_item = ProductShopStockORM(
                                        product_id=product["product_id"],
                                        shop_id=shop_id,
                                        stock=0
                                    )
                                    tenant_db.add(stock_item)
                                    tenant_db.flush()
                
                                old_stock = stock_item.stock
                                new_stock = old_stock + quantity_to_add
                                stock_item.stock = new_stock
                                tenant_db.commit()

                                # Success message for owner
                                success_msg = f"✅ Stock updated successfully!\n\n"
                                success_msg += f"📦 Product: {product['name']}\n"
                                success_msg += f"🏪 Shop: {shop_name}\n"
                                success_msg += f"📊 Old Stock: {old_stock}\n"
                                success_msg += f"📈 Added: +{quantity_to_add}\n"
                                success_msg += f"🆕 New Stock: {new_stock}\n"

                                send_message(chat_id, success_msg)

                                # Return to main menu
                                user_states.pop(chat_id, None)
                                from app.user_management import get_role_based_menu
                                kb = get_role_based_menu(user.role)
                                send_message(chat_id, "🏠 Main Menu:", keyboard=kb)
                
                            else:
                                # Shopkeeper: request approval for stock update
                                # Get shop-specific stock for the product
                                stock_item = tenant_db.query(ProductShopStockORM).filter(
                                    ProductShopStockORM.product_id == product["product_id"],
                                    ProductShopStockORM.shop_id == shop_id
                                ).first()
                
                                if not stock_item:
                                    # Create stock record if it doesn't exist
                                    stock_item = ProductShopStockORM(
                                        product_id=product["product_id"],
                                        shop_id=shop_id,
                                        stock=0
                                    )
                                    tenant_db.add(stock_item)
                                    tenant_db.flush()
                
                                old_stock = stock_item.stock
                                new_stock = old_stock + quantity_to_add

                                # Save stock update for approval
                                stock_data = {
                                    "product_id": product["product_id"],
                                    "product_name": product["name"],
                                    "shop_id": shop_id,
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
                                        shop_id=shop_id,
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
                                            pending_stock.approval_id,
                                            shop_id
                                        )

                                    central_db.close()

                                    send_message(chat_id, f"✅ Stock update request submitted for approval. Owner will review adding +{quantity_to_add} to {product['name']} at {shop_name}.")
                                else:
                                    send_message(chat_id, "❌ Failed to submit stock update request.")
                                    central_db.close()

                                user_states.pop(chat_id, None)

                        except ValueError:
                            send_message(chat_id, "❌ Invalid quantity. Enter a valid number:")
                        except Exception as e:
                            send_message(chat_id, f"❌ Error updating stock: {str(e)}")
                            import logging
                            logging.error(f"Stock update error: {str(e)}")
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
                        from app.user_management import get_role_based_menu
                        kb = get_role_based_menu(user.role)
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
    
                        # ✅ FIXED: For admins, filter products by their shop's stock
                        if user.role == "admin" and user.shop_id:
                            # Get products that have stock in admin's shop
                            matches = tenant_db.query(ProductORM).join(
                                ProductShopStockORM, ProductShopStockORM.product_id == ProductORM.product_id
                            ).filter(
                                ProductORM.name.ilike(f"%{query_text}%"),
                                ProductShopStockORM.shop_id == user.shop_id
                            ).all()
                        else:
                            # Owner can see all products
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
                        # FIXED: Don't show stock here (product doesn't have stock attribute)
                        kb_rows = [
                            [{"text": f"{p.name} ({p.unit_type}) — Price: ${p.price:.2f}",
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

                        # --- Proceed step-by-step: name → price → unit → category/shop (REMOVED: quantity, min_stock, low_threshold) ---
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
                            send_message(chat_id, "📦 Enter new unit type (or send `-` to keep current):")
                            return {"ok": True}

                        if step == 4:  # unit
                            val = text.strip()
                            if val == "":
                                send_message(chat_id, "⚠️ Please enter a valid unit type or '-' to keep current:")
                                return {"ok": True}
                            if val != "-":
                                data["new_unit"] = val
                            
                            # ✅ ASK: Update stock in shops? (since stock is now shop-specific)
                            user_states[chat_id] = {"action": "awaiting_update", "step": 5, "data": data}
                            send_message(chat_id, "🏪 Do you want to update stock levels in shops?\nSend 'yes' to update shop stocks or '-' to skip:")
                            return {"ok": True}

                        if step == 5:  # update shop stocks?
                            val = text.strip().lower()
                            if val == "":
                                send_message(chat_id, "⚠️ Please enter 'yes' to update shop stocks or '-' to skip:")
                                return {"ok": True}
                            
                            if val == "yes":
                                # Get all shops that have this product in stock
                                shop_stocks = tenant_db.query(ProductShopStockORM).filter(
                                    ProductShopStockORM.product_id == product_id
                                ).all()
                                
                                if not shop_stocks:
                                    send_message(chat_id, "ℹ️ This product has no stock assigned to any shop.")
                                    # Continue to confirmation
                                    user_states[chat_id] = {"action": "awaiting_update", "step": 7, "data": data}
                                    send_message(chat_id, "✅ Update product details only? (yes/no):")
                                    return {"ok": True}
                                
                                data["shop_stocks"] = [
                                    {"stock_id": stock.id, "shop_id": stock.shop_id, "current_stock": stock.stock}
                                    for stock in shop_stocks
                                ]
                                data["current_shop_index"] = 0  # Start with first shop
                                
                                # Get shop name for first shop
                                first_stock = shop_stocks[0]
                                shop = tenant_db.query(ShopORM).filter(ShopORM.shop_id == first_stock.shop_id).first()
                                shop_name = shop.name if shop else f"Shop {first_stock.shop_id}"
                                
                                user_states[chat_id] = {"action": "awaiting_update", "step": 6, "data": data}
                                send_message(chat_id, f"🏪 Shop: {shop_name}\nCurrent stock: {first_stock.stock}\nEnter new stock quantity (or '-' to keep current):")
                                return {"ok": True}
                            else:
                                # Skip shop stock updates
                                user_states[chat_id] = {"action": "awaiting_update", "step": 7, "data": data}
                                send_message(chat_id, "✅ Update product details only? (yes/no):")
                                return {"ok": True}

                        if step == 6:  # update stock for each shop
                            val = text.strip()
                            current_index = data.get("current_shop_index", 0)
                            shop_stocks = data.get("shop_stocks", [])
                            
                            if not shop_stocks:
                                send_message(chat_id, "❌ Error: No shop stocks data.")
                                user_states.pop(chat_id, None)
                                return {"ok": True}
                            
                            if val != "-":
                                try:
                                    new_stock = int(val)
                                    if new_stock < 0:
                                        send_message(chat_id, "❌ Stock cannot be negative. Enter a valid number:")
                                        return {"ok": True}
                                    # Update stock in the list
                                    shop_stocks[current_index]["new_stock"] = new_stock
                                except ValueError:
                                    send_message(chat_id, "❌ Invalid number. Enter an integer or `-` to skip:")
                                    return {"ok": True}
                            
                            # Move to next shop
                            next_index = current_index + 1
                            data["current_shop_index"] = next_index
                            
                            if next_index < len(shop_stocks):
                                # Get next shop info
                                next_stock = shop_stocks[next_index]
                                shop = tenant_db.query(ShopORM).filter(ShopORM.shop_id == next_stock["shop_id"]).first()
                                shop_name = shop.name if shop else f"Shop {next_stock['shop_id']}"
                                
                                user_states[chat_id] = {"action": "awaiting_update", "step": 6, "data": data}
                                send_message(chat_id, f"🏪 Shop: {shop_name}\nCurrent stock: {next_stock['current_stock']}\nEnter new stock quantity (or '-' to keep current):")
                            else:
                                # Finished all shops
                                user_states[chat_id] = {"action": "awaiting_update", "step": 7, "data": data}
                                send_message(chat_id, "✅ All shop stocks updated. Update product details? (yes/no):")
                            return {"ok": True}

                        if step == 7:  # confirmation
                            val = text.strip().lower()
                            if val == "":
                                send_message(chat_id, "⚠️ Please enter 'yes' or 'no':")
                                return {"ok": True}
                            
                            if val != "yes":
                                send_message(chat_id, "❌ Update cancelled.")
                                user_states.pop(chat_id, None)
                                from app.user_management import get_role_based_menu
                                kb = get_role_based_menu(user.role)
                                send_message(chat_id, "🏠 Main Menu:", keyboard=kb)
                                return {"ok": True}
                            
                            # ✅ Update product in DB
                            # Update ProductORM fields
                            if "new_name" in data and data["new_name"]:
                                product.name = data["new_name"]
                            if "new_price" in data:
                                product.price = data["new_price"]
                            if "new_unit" in data and data["new_unit"]:
                                product.unit_type = data["new_unit"]
                            
                            tenant_db.commit()
                            
                            # ✅ Update shop stocks if requested
                            shop_stocks = data.get("shop_stocks", [])
                            if shop_stocks:
                                for stock_info in shop_stocks:
                                    if "new_stock" in stock_info:
                                        stock_item = tenant_db.query(ProductShopStockORM).filter(
                                            ProductShopStockORM.id == stock_info["stock_id"]
                                        ).first()
                                        if stock_item:
                                            stock_item.stock = stock_info["new_stock"]
                                            tenant_db.commit()
                            
                            send_message(chat_id, f"✅ Product *{product.name}* updated successfully.")
                            
                            # ✅ Return to main menu
                            user_states.pop(chat_id, None)  # Clear state
                            from app.user_management import get_role_based_menu
                            kb = get_role_based_menu(user.role)
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

                        # Include shop name in the message
                        shop_name = data.get("selected_shop_name", "Shop")
                        send_message(chat_id, f"🏪 {shop_name}\n🔍 Searching for products...")

                        # Get shop_id from data (should be set when shopkeeper selected shop)
                        shop_id = data.get("selected_shop_id")
                        if not shop_id:
                            send_message(chat_id, "❌ No shop selected. Please start over from main menu.")
                            user_states.pop(chat_id, None)
                            return {"ok": True}

                        # CORRECT QUERY: Get shop-specific stock with product info
                        matches = tenant_db.query(ProductShopStockORM, ProductORM).join(
                            ProductORM, ProductORM.product_id == ProductShopStockORM.product_id
                        ).filter(
                            ProductShopStockORM.shop_id == shop_id,
                            ProductORM.name.ilike(f"%{text}%")
                        ).all()
                        
                        if not matches:
                            send_message(chat_id, "⚠️ No products found with that name in this shop. Try again:")
                            return {"ok": True}

                        if len(matches) == 1:
                            stock_item, product = matches[0]
                            data["current_product"] = {
                                "product_id": product.product_id,
                                "name": product.name,
                                "price": float(product.price),
                                "unit_type": product.unit_type,
                                "available_stock": stock_item.stock,  # CORRECT: Shop-specific stock
                                "stock_id": stock_item.id  # For updating stock later
                            }
                            user_states[chat_id] = {"action": "awaiting_sale", "step": 2, "data": data}
                            send_message(chat_id, f"📦 Selected {product.name} ({product.unit_type}). Enter quantity to add:")
                            return {"ok": True}

                        # multiple matches -> show inline keyboard for user to pick
                        kb_rows = []
                        for stock_item, product in matches:
                            status = "🟢" if stock_item.stock > stock_item.low_stock_threshold else "🔴" if stock_item.stock == 0 else "🟡"
                            kb_rows.append([{
                                "text": f"{status} {product.name} — Stock: {stock_item.stock} ({product.unit_type})", 
                                "callback_data": f"select_sale:{product.product_id}:{stock_item.id}"
                            }])
                        
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
                                "subtotal": current_product["price"] * qty,
                                "stock_id": current_product.get("stock_id")  # Include stock_id for stock update
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
    
                        # FIXED: Always go to step 6 for contact collection
                        # Whether it's credit sale or change due, we should collect contact
                        user_states[chat_id] = {"action": "awaiting_sale", "step": 6, "data": data}
    
                        # Ask for contact (optional)
                        if data.get("sale_type") == "credit":
                            send_message(chat_id, "📞 Enter customer contact number (optional for credit follow-up) or type 'skip':")
                        else:
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
        
                    # STEP 6: customer contact OR confirmation ONLY collect contact info - NEVER record sales here
                    elif step == 6:
                        logger.info(f"🔍 STEP 6 ENTERED - Collecting contact - Chat: {chat_id}, Text: '{text}'")
    
                        # This should ONLY be for collecting customer contact
                        customer_contact = text.strip()
    
                        # Handle "skip" for optional contact
                        if customer_contact.lower() == "skip":
                            customer_contact = ""
    
                        data["customer_contact"] = customer_contact
                        user_states[chat_id] = {"action": "awaiting_sale", "step": 7, "data": data}
    
                        # Now ask for confirmation
                        send_message(chat_id, f"✅ Customer info recorded. Confirm sale? (yes/no)")
                        return {"ok": True}

                    # STEP 7: ONLY record sales here - this is the final confirmation step
                    elif step == 7:
                        logger.info(f"🔍 STEP 7 ENTERED - Final confirmation - Chat: {chat_id}, Text: '{text}'")
    
                        confirmation = text.strip().lower()
                        if not confirmation:
                            send_message(chat_id, "⚠️ Please confirm with 'yes' or 'no':")
                            return {"ok": True}
    
                        if confirmation != "yes":
                            send_message(chat_id, "❌ Sale cancelled.")
                            user_states.pop(chat_id, None)
                            from app.user_management import get_role_based_menu
                            kb = get_role_based_menu(user.role)
                            send_message(chat_id, "🏠 Main Menu:", keyboard=kb)
                            return {"ok": True}
    
                        logger.info(f"🎯 STEP 7 → Recording sale - Chat: {chat_id}")
    
                        # Record the sale (ONLY HERE!)
                        record_sale_result = record_cart_sale(tenant_db, chat_id, data)
    
                        if record_sale_result:
                            logger.info(f"✅ STEP 7 → Sale recorded successfully - Chat: {chat_id}")
        
                            # Clear state and return to menu
                            user_states.pop(chat_id, None)
                            from app.user_management import get_role_based_menu
                            kb = get_role_based_menu(user.role)
                            send_message(chat_id, "🏠 Main Menu:", keyboard=kb)
                        else:
                            logger.error(f"❌ STEP 7 → Sale recording failed - Chat: {chat_id}")
                            send_message(chat_id, "❌ Failed to record sale. Please try again.")
                            user_states.pop(chat_id, None)
    
                        return {"ok": True}
                                                                    
                # -------------------- Record Payment Flow --------------------
                elif action == "record_payment":
                    tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                    if tenant_db is None:
                        send_message(chat_id, "❌ Unable to access tenant database.")
                        return {"ok": True}
    
                    data = state.get("data", {})
                    payment_type = data.get("payment_type")
    
                    # STEP 1: Already handled by callback (payment_type selection)
    
                    # STEP 2: Search for customer
                    if step == 2:
                        customer_name = text.strip()
                        if not customer_name:
                            send_message(chat_id, "❌ Customer name cannot be empty. Please enter customer name:")
                            return {"ok": True}
        
                        # Search for customers with outstanding balance
                        if payment_type == "credit":
                            # Find customers with pending credit
                            sales_with_credit = tenant_db.query(SaleORM).filter(
                                SaleORM.pending_amount > 0.01
                            ).all()
            
                            # Get unique customers with credit
                            customer_ids = set()
                            customers_with_credit = []
            
                            for sale in sales_with_credit:
                                if sale.customer_id and sale.customer_id not in customer_ids:
                                    customer_ids.add(sale.customer_id)
                                    customer = tenant_db.query(CustomerORM).filter(
                                        CustomerORM.customer_id == sale.customer_id
                                    ).first()
                                    if customer and customer_name.lower() in customer.name.lower():
                                        # Calculate total pending for this customer
                                        customer_sales = tenant_db.query(SaleORM).filter(
                                            SaleORM.customer_id == sale.customer_id,
                                            SaleORM.pending_amount > 0.01
                                        ).all()
                        
                                        total_pending = sum(s.pending_amount for s in customer_sales)
                                        customers_with_credit.append({
                                            "customer_id": customer.customer_id,
                                            "name": customer.name,
                                            "contact": customer.contact or "No contact",
                                            "total_pending": total_pending,
                                            "sales_count": len(customer_sales)
                                        })
            
                        else:  # change collection
                            # Find customers with change due
                            sales_with_change = tenant_db.query(SaleORM).filter(
                                SaleORM.change_left > 0.01
                            ).all()
            
                            # Get unique customers with change due
                            customer_ids = set()
                            customers_with_change = []
            
                            for sale in sales_with_change:
                                if sale.customer_id and sale.customer_id not in customer_ids:
                                    customer_ids.add(sale.customer_id)
                                    customer = tenant_db.query(CustomerORM).filter(
                                        CustomerORM.customer_id == sale.customer_id
                                    ).first()
                                    if customer and customer_name.lower() in customer.name.lower():
                                        # Calculate total change due for this customer
                                        customer_sales = tenant_db.query(SaleORM).filter(
                                            SaleORM.customer_id == sale.customer_id,
                                            SaleORM.change_left > 0.01
                                        ).all()
                        
                                        total_change = sum(s.change_left for s in customer_sales)
                                        customers_with_change.append({
                                            "customer_id": customer.customer_id,
                                            "name": customer.name,
                                            "contact": customer.contact or "No contact",
                                            "total_change": total_change,
                                            "sales_count": len(customer_sales)
                                        })
        
                        # Check results
                        if payment_type == "credit" and not customers_with_credit:
                            send_message(chat_id, f"❌ No customers found with credit balance matching '{customer_name}'.\n\nTry again or enter a different name:")
                            return {"ok": True}
                        elif payment_type == "change" and not customers_with_change:
                            send_message(chat_id, f"❌ No customers found with change due matching '{customer_name}'.\n\nTry again or enter a different name:")
                            return {"ok": True}
        
                        # Store results and move to selection
                        if payment_type == "credit":
                            data["customer_results"] = customers_with_credit
                            if len(customers_with_credit) == 1:
                                # Single match - proceed directly
                                customer = customers_with_credit[0]
                                data["selected_customer"] = customer
                                user_states[chat_id] = {"action": "record_payment", "step": 3, "data": data}
                
                                send_message(chat_id, f"👤 Selected: {customer['name']}\n"
                                                     f"📞 Contact: {customer['contact']}\n"
                                                     f"💳 Total Credit Due: ${customer['total_pending']:.2f}\n"
                                                     f"📊 From {customer['sales_count']} transaction(s)\n\n"
                                                     f"Enter amount received from customer:")
                            else:
                                # Multiple matches - show selection
                                user_states[chat_id] = {"action": "record_payment", "step": 2.5, "data": data}
                
                                kb_rows = []
                                for customer in customers_with_credit:
                                    kb_rows.append([{
                                        "text": f"👤 {customer['name']} - ${customer['total_pending']:.2f} due",
                                        "callback_data": f"select_customer_payment:{customer['customer_id']}:credit"
                                    }])
                                kb_rows.append([{"text": "⬅️ Cancel", "callback_data": "back_to_menu"}])
                
                                send_message(chat_id, "🔍 Multiple customers found. Select one:", {"inline_keyboard": kb_rows})
        
                        else:  # change
                            data["customer_results"] = customers_with_change
                            if len(customers_with_change) == 1:
                                # Single match - proceed directly
                                customer = customers_with_change[0]
                                data["selected_customer"] = customer
                                user_states[chat_id] = {"action": "record_payment", "step": 3, "data": data}
                
                                send_message(chat_id, f"👤 Selected: {customer['name']}\n"
                                                     f"📞 Contact: {customer['contact']}\n"
                                                     f"🪙 Total Change Due: ${customer['total_change']:.2f}\n"
                                                     f"📊 From {customer['sales_count']} transaction(s)\n\n"
                                                     f"Enter amount of change collected from customer:")
                            else:
                                # Multiple matches - show selection
                                user_states[chat_id] = {"action": "record_payment", "step": 2.5, "data": data}
                
                                kb_rows = []
                                for customer in customers_with_change:
                                    kb_rows.append([{
                                        "text": f"👤 {customer['name']} - ${customer['total_change']:.2f} change due",
                                        "callback_data": f"select_customer_payment:{customer['customer_id']}:change"
                                    }])
                                kb_rows.append([{"text": "⬅️ Cancel", "callback_data": "back_to_menu"}])
                
                                send_message(chat_id, "🔍 Multiple customers found. Select one:", {"inline_keyboard": kb_rows})
        
                        return {"ok": True}
    
                    # STEP 3: Enter payment amount
                    elif step == 3:
                        amount_text = text.strip()
                        if not amount_text:
                            send_message(chat_id, "❌ Amount cannot be empty. Please enter amount:")
                            return {"ok": True}
        
                        try:
                            amount = float(amount_text)
                            if amount <= 0:
                                send_message(chat_id, "❌ Amount must be greater than 0. Please enter a positive amount:")
                                return {"ok": True}
            
                            customer = data.get("selected_customer")
                            if not customer:
                                send_message(chat_id, "❌ Customer not selected. Please start over.")
                                user_states.pop(chat_id, None)
                                return {"ok": True}
            
                            # Validate amount
                            if payment_type == "credit":
                                max_amount = customer["total_pending"]
                                if amount > max_amount:
                                    send_message(chat_id, f"❌ Amount (${amount:.2f}) exceeds total credit due (${max_amount:.2f}).\n\nEnter amount received (max ${max_amount:.2f}):")
                                    return {"ok": True}
                            else:  # change
                                max_amount = customer["total_change"]
                                if amount > max_amount:
                                    send_message(chat_id, f"❌ Amount (${amount:.2f}) exceeds total change due (${max_amount:.2f}).\n\nEnter amount collected (max ${max_amount:.2f}):")
                                    return {"ok": True}
            
                            data["payment_amount"] = amount
                            user_states[chat_id] = {"action": "record_payment", "step": 4, "data": data}
            
                            # Ask for payment method if it's a credit payment
                            if payment_type == "credit":
                                kb_rows = [
                                    [{"text": "💵 Cash", "callback_data": "payment_method:cash"}],
                                    [{"text": "📱 Ecocash", "callback_data": "payment_method:ecocash"}],
                                    [{"text": "💳 Swipe", "callback_data": "payment_method:swipe"}],
                                    [{"text": "⬅️ Cancel", "callback_data": "back_to_menu"}]
                                ]
                
                                send_message(chat_id, f"💰 Amount: ${amount:.2f}\n💳 Select payment method:", {"inline_keyboard": kb_rows})
                            else:
                                # For change collection, just confirm
                                send_message(chat_id, f"🪙 Confirm Change Collection:\n\n"
                                                     f"👤 Customer: {customer['name']}\n"
                                                     f"💰 Amount Collected: ${amount:.2f}\n\n"
                                                     f"Type 'YES' to confirm or 'NO' to cancel:")
            
                        except ValueError:
                            send_message(chat_id, "❌ Invalid amount. Please enter a valid number:")
        
                        return {"ok": True}
    
                    # STEP 4: Process payment (for credit) or confirm (for change)
                    elif step == 4:
                        if payment_type == "credit":
                            # This step is handled by callback (payment_method)
                            pass
                        else:
                            # Change collection confirmation
                            confirmation = text.strip().upper()
                            if confirmation != "YES":
                                send_message(chat_id, "❌ Change collection cancelled.")
                                user_states.pop(chat_id, None)
                                return {"ok": True}
            
                            # Process change collection
                            customer = data.get("selected_customer")
                            amount = data.get("payment_amount", 0)
            
                            # Get all sales with change due for this customer
                            sales_with_change = tenant_db.query(SaleORM).filter(
                                SaleORM.customer_id == customer["customer_id"],
                                SaleORM.change_left > 0.01
                            ).order_by(SaleORM.sale_date).all()
            
                            remaining_amount = amount
                            processed_sales = []
            
                            # Apply payment to oldest sales first (FIFO)
                            for sale in sales_with_change:
                                if remaining_amount <= 0:
                                    break
                
                                if sale.change_left <= remaining_amount:
                                    # Full change collected for this sale
                                    processed_amount = sale.change_left
                                    sale.change_left = 0
                                    remaining_amount -= processed_amount
                                else:
                                    # Partial change collected
                                    sale.change_left -= remaining_amount
                                    processed_amount = remaining_amount
                                    remaining_amount = 0
                
                                processed_sales.append({
                                    "sale_id": sale.sale_id,
                                    "amount": processed_amount,
                                    "remaining_change": sale.change_left
                                })
            
                            # Commit changes
                            tenant_db.commit()
            
                            # Create payment record
                            from datetime import datetime
                            payment_record = PaymentRecordORM(
                                customer_id=customer["customer_id"],
                                customer_name=customer["name"],
                                payment_type="change_collection",
                                amount=amount,
                                remaining_amount=remaining_amount if remaining_amount > 0 else 0,
                                notes=f"Change collected by {user.name}",
                                recorded_by=user.user_id,
                                recorded_by_name=user.name,
                                shop_id=user.shop_id if hasattr(user, 'shop_id') else None,
                                recorded_at=datetime.utcnow()
                            )
                            tenant_db.add(payment_record)
                            tenant_db.commit()
            
                            # Success message
                            success_msg = f"✅ Change collection recorded successfully!\n\n"
                            success_msg += f"👤 Customer: {customer['name']}\n"
                            success_msg += f"💰 Amount Collected: ${amount:.2f}\n\n"
            
                            if processed_sales:
                                success_msg += f"📋 **Applied to {len(processed_sales)} sale(s):**\n"
                                for ps in processed_sales:
                                    success_msg += f"• Sale #{ps['sale_id']}: ${ps['amount']:.2f} collected\n"
                                    if ps['remaining_change'] > 0:
                                        success_msg += f"  Remaining change: ${ps['remaining_change']:.2f}\n"
            
                            if remaining_amount > 0:
                                success_msg += f"\n⚠️ Note: ${remaining_amount:.2f} was not applied (exceeds total change due)\n"
            
                            # Check if customer still has change due
                            remaining_change = tenant_db.query(SaleORM).filter(
                                SaleORM.customer_id == customer["customer_id"],
                                SaleORM.change_left > 0.01
                            ).count()
            
                            if remaining_change > 0:
                                total_remaining = sum(s.change_left for s in tenant_db.query(SaleORM).filter(
                                    SaleORM.customer_id == customer["customer_id"],
                                    SaleORM.change_left > 0.01
                                ).all())
                                success_msg += f"\nℹ️ Customer still has ${total_remaining:.2f} change due from {remaining_change} transaction(s)"
            
                            send_message(chat_id, success_msg)
            
                            # Clear state and return to menu
                            user_states.pop(chat_id, None)
                            from app.user_management import get_role_based_menu
                            kb = get_role_based_menu(user.role)
                            send_message(chat_id, "🏠 Main Menu:", keyboard=kb)
        
                        return {"ok": True}
                    
                    # STEP 5: Confirm and process credit payment
                    elif step == 5:
                        if payment_type != "credit":
                            send_message(chat_id, "❌ Invalid step for payment type.")
                            user_states.pop(chat_id, None)
                            return {"ok": True}
        
                        confirmation = text.strip().upper()
                        if confirmation != "YES":
                            send_message(chat_id, "❌ Credit payment cancelled.")
                            user_states.pop(chat_id, None)
                            return {"ok": True}
        
                        # Process credit payment
                        customer = data.get("selected_customer")
                        amount = data.get("payment_amount", 0)
                        payment_method = data.get("payment_method", "cash")
        
                        # Get all sales with pending amount for this customer
                        sales_with_credit = tenant_db.query(SaleORM).filter(
                            SaleORM.customer_id == customer["customer_id"],
                            SaleORM.pending_amount > 0.01
                        ).order_by(SaleORM.sale_date).all()
        
                        remaining_amount = amount
                        processed_sales = []
        
                        # Apply payment to oldest sales first (FIFO)
                        for sale in sales_with_credit:
                            if remaining_amount <= 0:
                                break
            
                            if sale.pending_amount <= remaining_amount:
                                # Full payment for this sale
                                processed_amount = sale.pending_amount
                                sale.pending_amount = 0
                                sale.amount_paid = sale.total_amount  # Mark as fully paid
                                remaining_amount -= processed_amount
                            else:
                                # Partial payment
                                sale.pending_amount -= remaining_amount
                                sale.amount_paid += remaining_amount
                                processed_amount = remaining_amount
                                remaining_amount = 0
            
                            processed_sales.append({
                                "sale_id": sale.sale_id,
                                "amount": processed_amount,
                                "remaining_pending": sale.pending_amount
                            })
        
                        # Commit changes
                        tenant_db.commit()
        
                        # Create payment record
                        from datetime import datetime
                        payment_record = PaymentRecordORM(
                            customer_id=customer["customer_id"],
                            customer_name=customer["name"],
                            payment_type="credit_payment",
                            payment_method=payment_method,
                            amount=amount,
                            remaining_amount=remaining_amount if remaining_amount > 0 else 0,
                            notes=f"Credit payment via {payment_method} collected by {user.name}",
                            recorded_by=user.user_id,
                            recorded_by_name=user.name,
                            shop_id=user.shop_id if hasattr(user, 'shop_id') else None,
                            recorded_at=datetime.utcnow()
                        )
                        tenant_db.add(payment_record)
                        tenant_db.commit()
        
                        # Success message
                        success_msg = f"✅ Credit payment recorded successfully!\n\n"
                        success_msg += f"👤 Customer: {customer['name']}\n"
                        success_msg += f"💰 Amount Paid: ${amount:.2f}\n"
                        success_msg += f"💵 Method: {payment_method.title()}\n\n"
        
                        if processed_sales:
                            success_msg += f"📋 **Applied to {len(processed_sales)} sale(s):**\n"
                            for ps in processed_sales:
                                success_msg += f"• Sale #{ps['sale_id']}: ${ps['amount']:.2f} paid\n"
                                if ps['remaining_pending'] > 0:
                                    success_msg += f"  Remaining credit: ${ps['remaining_pending']:.2f}\n"
        
                        if remaining_amount > 0:
                            success_msg += f"\n⚠️ Note: ${remaining_amount:.2f} was not applied (exceeds total credit due)\n"
        
                        # Check if customer still has credit balance
                        remaining_credit = tenant_db.query(SaleORM).filter(
                            SaleORM.customer_id == customer["customer_id"],
                            SaleORM.pending_amount > 0.01
                        ).count()
        
                        if remaining_credit > 0:
                            total_remaining = sum(s.pending_amount for s in tenant_db.query(SaleORM).filter(
                                SaleORM.customer_id == customer["customer_id"],
                                SaleORM.pending_amount > 0.01
                            ).all())
                            success_msg += f"\nℹ️ Customer still has ${total_remaining:.2f} credit balance from {remaining_credit} transaction(s)"
        
                        send_message(chat_id, success_msg)
        
                        # Clear state and return to menu
                        user_states.pop(chat_id, None)
                        from app.user_management import get_role_based_menu
                        kb = get_role_based_menu(user.role)
                        send_message(chat_id, "🏠 Main Menu:", keyboard=kb)
        
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
