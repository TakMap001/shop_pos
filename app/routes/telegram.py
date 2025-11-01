# app/routes/telegram.py

from fastapi import APIRouter, Request, Depends
import requests, os
from sqlalchemy.orm import Session
from decimal import Decimal
from datetime import datetime
from sqlalchemy import func, text, extract
from app.models.central_models import Tenant  # Central DB
from app.models.models import Base as TenantBase
from app.models.models import User, ProductORM, SaleORM  # Tenant DB
from app.database import get_db  # central DB session
from app.telegram_notifications import notify_low_stock, notify_top_product, notify_high_value_sale, send_message
from app.tenants import create_tenant_db, get_engine_for_tenant, get_session_for_tenant
from config import DATABASE_URL
from telebot import types
from app.telegram_notifications import notify_owner_of_new_shopkeeper
from config import TELEGRAM_BOT_TOKEN, TELEGRAM_API_URL
from app.tenant_db import get_tenant_session, create_tenant_db, ensure_tenant_tables, ensure_tenant_session
import random
import string
import bcrypt
import time
from app.core import SessionLocal, get_db
from sqlalchemy.exc import SQLAlchemyError
import uuid
import logging
from telegram.helpers import escape_markdown
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
import re
import html
from app.models.models import User

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
    from app.models.models import User
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
                [{"text": "📦 View Stock", "callback_data": "view_stock"}],
                [{"text": "💰 Record Sale", "callback_data": "record_sale"}],  # NEW BUTTON
                [{"text": "📊 Reports", "callback_data": "report_menu"}],
                [{"text": "🏪 Update Shop Info", "callback_data": "setup_shop"}],
                [{"text": "👤 Create Shopkeeper", "callback_data": "create_shopkeeper"}],
                [{"text": "❓ Help", "callback_data": "help"}]
            ]
        }
    elif role == "shopkeeper":
        kb_dict = {
            "inline_keyboard": [
                [{"text": "➕ Add Product", "callback_data": "add_product"}],
                [{"text": "✏️ Update Product", "callback_data": "update_product"}],
                [{"text": "📦 View Stock", "callback_data": "view_stock"}],
                [{"text": "💰 Record Sale", "callback_data": "record_sale"}],  # NEW BUTTON
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
    Register a new user in a tenant-aware way.
    
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

    # -------------------- Check for Existing Tenant --------------------
    tenant = central_db.query(Tenant).filter(Tenant.telegram_owner_id == chat_id).first()
    if role == "owner" and tenant:
        send_message(chat_id, f"❌ You already have a tenant registered.")
        return

    # -------------------- Handle Owner Registration --------------------
    if role == "owner":
        # Construct tenant DB URL
        tenant_db_url = DATABASE_URL.rsplit("/", 1)[0] + f"/tenant_{new_chat_id}"

        # Create tenant DB
        create_tenant_db(tenant_db_url)
        engine = get_engine_for_tenant(tenant_db_url)
        TenantBase.metadata.create_all(bind=engine)

        # Add to central Tenant table
        new_tenant = Tenant(
            tenant_id=str(new_chat_id),
            store_name=f"{name}'s Store",
            telegram_owner_id=new_chat_id,
            database_url=tenant_db_url
        )
        try:
            central_db.add(new_tenant)
            central_db.commit()
            central_db.refresh(new_tenant)
        except Exception as e:
            central_db.rollback()
            send_message(chat_id, f"❌ Database error (central DB): {str(e)}")
            return

        send_message(chat_id, f"✅ Owner '{name}' registered and tenant DB created.")

    # -------------------- Handle Shopkeeper / Tenant Users --------------------
    else:
        if not tenant:
            send_message(chat_id, "❌ No tenant found. Please register as an owner first.")
            return

        # Connect to tenant DB
        tenant_db = get_session_for_tenant(tenant.database_url)

        # Check if user exists in tenant DB
        existing_user = tenant_db.query(User).filter(User.user_id == new_chat_id).first()
        if existing_user:
            send_message(chat_id, f"❌ User with ID {new_chat_id} already exists in tenant DB.")
            return

        # Add user to tenant DB
        new_user = User(
            user_id=new_chat_id,
            name=name,
            email=f"{new_chat_id}@example.com",
            password_hash="",
            role=role
        )
        try:
            tenant_db.add(new_user)
            tenant_db.commit()
            tenant_db.refresh(new_user)
        except Exception as e:
            tenant_db.rollback()
            send_message(chat_id, f"❌ Database error (tenant DB): {str(e)}")
            return

        send_message(chat_id, f"✅ {role.title()} '{name}' added successfully to tenant DB.")

    # -------------------- Welcome Message --------------------
    send_message(new_chat_id, f"👋 Hello {name}! Use /start to begin.")

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
def generate_report(db: Session, report_type: str, tenant_id: int = None):
    """
    Generate tenant-aware reports.
    - db: SQLAlchemy session (tenant DB or central DB)
    - report_type: report_daily, report_weekly, report_monthly, etc.
    - tenant_id: optional, used for multi-tenant filtering in central DB
    """

    def apply_tenant_filter(query, model):
        return query.filter(model.tenant_id == tenant_id) if tenant_id else query

    # -------------------- Daily Sales --------------------
    if report_type == "report_daily":
        results = (
            apply_tenant_filter(
                db.query(
                    func.date(SaleORM.sale_date).label("day"),
                    func.sum(SaleORM.quantity).label("total_qty"),
                    func.sum(SaleORM.total_amount).label("total_revenue")
                ),
                SaleORM
            )
            .group_by(func.date(SaleORM.sale_date))
            .order_by(func.date(SaleORM.sale_date))
            .all()
        )
        if not results:
            return "No sales data."
        lines = ["📅 *Daily Sales*"]
        for r in results:
            lines.append(f"{r.day}: {r.total_qty} items, ${float(r.total_revenue)}")
        return "\n".join(lines)

    # -------------------- Weekly Sales --------------------
    elif report_type == "report_weekly":
        results = (
            apply_tenant_filter(
                db.query(
                    extract("week", SaleORM.sale_date).label("week"),
                    func.sum(SaleORM.quantity).label("total_qty"),
                    func.sum(SaleORM.total_amount).label("total_revenue")
                ),
                SaleORM
            )
            .group_by("week")
            .order_by("week")
            .all()
        )
        if not results:
            return "No sales data."
        lines = ["📆 *Weekly Sales*"]
        for r in results:
            lines.append(f"Week {int(r.week)}: {r.total_qty} items, ${float(r.total_revenue)}")
        return "\n".join(lines)

    # -------------------- Monthly Sales per Product --------------------
    elif report_type == "report_monthly":
        now = datetime.now()
        results = (
            apply_tenant_filter(
                db.query(
                    ProductORM.name.label("product"),
                    func.sum(SaleORM.quantity).label("total_qty"),
                    func.sum(SaleORM.total_amount).label("total_revenue")
                ),
                SaleORM
            )
            .join(ProductORM, SaleORM.product_id == ProductORM.product_id)
            .filter(extract("year", SaleORM.sale_date) == now.year)
            .filter(extract("month", SaleORM.sale_date) == now.month)
            .group_by(ProductORM.name)
            .all()
        )
        if not results:
            return "No sales data."
        lines = ["📊 *Monthly Sales per Product*"]
        for r in results:
            lines.append(f"{r.product}: {r.total_qty} items, ${float(r.total_revenue)}")
        return "\n".join(lines)

    # -------------------- Low Stock Products --------------------
    elif report_type == "report_low_stock":
        products = apply_tenant_filter(db.query(ProductORM), ProductORM).filter(ProductORM.stock <= 10).all()
        if not products:
            return "All products have sufficient stock."
        lines = ["⚠️ *Low Stock Products:*"]
        for p in products:
            lines.append(f"{p.name}: {p.stock} units left")
        return "\n".join(lines)

    # -------------------- Top Products --------------------
    elif report_type == "report_top_products":
        results = (
            apply_tenant_filter(
                db.query(
                    ProductORM.name.label("product"),
                    func.sum(SaleORM.quantity).label("total_qty"),
                    func.sum(SaleORM.total_amount).label("total_revenue")
                ),
                ProductORM
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
            lines.append(f"{r.product}: {r.total_qty} sold, ${float(r.total_revenue)} revenue")
        return "\n".join(lines)

    # -------------------- Top Customers --------------------
    elif report_type == "report_top_customers":
        results = (
            apply_tenant_filter(
                db.query(
                    User.name.label("user"),
                    func.sum(SaleORM.quantity).label("total_qty"),
                    func.sum(SaleORM.total_amount).label("total_spent")
                ),
                User
            )
            .join(SaleORM, User.user_id == SaleORM.user_id)
            .group_by(User.name)
            .order_by(func.sum(SaleORM.total_amount).desc())
            .limit(5)
            .all()
        )
        if not results:
            return "No sales data."
        lines = ["👥 *Top Customers*"]
        for r in results:
            lines.append(f"{r.user}: {r.total_qty} items, ${float(r.total_spent)} spent")
        return "\n".join(lines)

    # -------------------- Top Repeat Customers --------------------
    elif report_type == "report_top_repeat_customers":
        customers = (
            apply_tenant_filter(
                db.query(
                    SaleORM.user_id,
                    func.count(SaleORM.sale_id).label("num_purchases"),
                    func.sum(SaleORM.total_amount).label("total_spent")
                ),
                SaleORM
            )
            .group_by(SaleORM.user_id)
            .order_by(func.count(SaleORM.sale_id).desc())
            .limit(5)
            .all()
        )
        if not customers:
            return "No sales data."
        lines = ["🔁 *Top Repeat Customers*"]
        for c in customers:
            user = apply_tenant_filter(db.query(User), User).filter(User.user_id == c.user_id).first()
            name = user.name if user else f"User {c.user_id}"
            lines.append(f"{name}: {c.num_purchases} purchases, ${float(c.total_spent)} spent")
        return "\n".join(lines)

    # -------------------- Average Order Value --------------------
    elif report_type == "report_aov":
        total_orders = apply_tenant_filter(db.query(func.count(SaleORM.sale_id)), SaleORM).scalar() or 0
        total_revenue = apply_tenant_filter(db.query(func.sum(SaleORM.total_amount)), SaleORM).scalar() or 0
        aov = round(total_revenue / total_orders, 2) if total_orders > 0 else 0
        return f"💰 *Average Order Value*\nTotal Orders: {total_orders}\nTotal Revenue: ${total_revenue}\nAOV: ${aov}"

    # -------------------- Stock Turnover --------------------
    elif report_type == "report_stock_turnover":
        products = apply_tenant_filter(db.query(ProductORM), ProductORM).all()
        if not products:
            return "No products found."
        lines = ["📦 *Stock Turnover per Product*"]
        for p in products:
            total_sold = apply_tenant_filter(db.query(func.sum(SaleORM.quantity)), SaleORM).filter(SaleORM.product_id == p.product_id).scalar() or 0
            turnover_rate = total_sold / (p.stock + total_sold) if (p.stock + total_sold) > 0 else 0
            lines.append(f"{p.name}: Sold {total_sold}, Stock {p.stock}, Turnover Rate {turnover_rate:.2f}")
        return "\n".join(lines)

    # -------------------- Credit List --------------------
    elif report_type == "report_credits":
        sales_with_credit = (
            apply_tenant_filter(db.query(SaleORM), SaleORM)
            .filter(SaleORM.pending_amount > 0)
            .order_by(SaleORM.sale_date.desc())
            .all()
        )
        if not sales_with_credit:
            return "No outstanding credits."
        lines = ["💳 *Credit List*"]
        for s in sales_with_credit:
            customer = db.query(CustomerORM).filter(CustomerORM.customer_id == s.customer_id).first()
            if customer:
                lines.append(
                    f"{customer.name} ({customer.contact}): ${float(s.pending_amount)} pending for {s.quantity} × {s.unit_type} of {s.product.name}"
                )
        return "\n".join(lines)

    # -------------------- Change List --------------------
    elif report_type == "report_change":
        sales_with_change = (
            apply_tenant_filter(db.query(SaleORM), SaleORM)
            .filter(SaleORM.change_left > 0)
            .order_by(SaleORM.sale_date.desc())
            .all()
        )
        if not sales_with_change:
            return "No sales with change."
        lines = ["💵 *Change List*"]
        for s in sales_with_change:
            customer = db.query(CustomerORM).filter(CustomerORM.customer_id == s.customer_id).first()
            if customer:
                lines.append(
                    f"{customer.name} ({customer.contact}): ${float(s.change_left)} change for {s.quantity} × {s.unit_type} of {s.product.name}"
                )
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
                [{"text": "👥 Top Customers", "callback_data": "report_top_customers"}],
                [{"text": "🔁 Top Repeat Customers", "callback_data": "report_top_repeat_customers"}],
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
        if "message" in data:
            chat_id = data["message"]["chat"]["id"]
            text = data["message"].get("text", "").strip()
        elif "callback_query" in data:
            chat_id = data["callback_query"]["message"]["chat"]["id"]
            text = data["callback_query"]["data"]

        if not chat_id:
            return {"ok": True}

        # 1. Get user from central DB
        user = db.query(User).filter(User.chat_id == chat_id).first()

        # -------------------- /start --------------------
        if text == "/start":
            user = db.query(User).filter(User.chat_id == chat_id).first()

            if user:
                generated_password = None

                # 🧩 Ensure username and password exist
                if not user.username or not user.password_hash:
                    if not user.username:
                        user.username = create_username(f"{user.role.capitalize()}{chat_id}")
                    if not user.password_hash:
                        generated_password = generate_password()
                        user.password_hash = hash_password(generated_password)
                    db.commit()

                    if generated_password:
                        send_owner_credentials(chat_id, user.username, generated_password)

                    send_message(chat_id, "🏪 Let's set up your shop! Please enter the shop name:")
                    user_states[chat_id] = {"action": "setup_shop", "step": 1, "data": {}}

                else:
                    send_message(chat_id, "👋 Welcome back! Please enter your password to continue:")
                    user_states[chat_id] = {"action": "login", "step": 1, "data": {}}

                # -------------------- Ensure tenant schema for owners --------------------
                if user.role == "owner":
                    try:
                        schema_name = f"tenant_{chat_id}"

                        # ✅ Create tenant schema if not exists
                        tenant_db_url = create_tenant_db(chat_id)

                        # ✅ Always store only schema name in user.tenant_schema
                        if not user.tenant_schema or user.tenant_schema != schema_name:
                            user.tenant_schema = schema_name
                            db.commit()
                            logger.info(f"✅ Linked user {user.username} to tenant schema '{schema_name}'")

                        # ✅ Ensure tenant record exists or update
                        existing_tenant = db.query(Tenant).filter(Tenant.telegram_owner_id == chat_id).first()
                        if not existing_tenant:
                            new_tenant = Tenant(
                                tenant_id=str(chat_id),
                                store_name=f"Owner{chat_id}",
                                telegram_owner_id=chat_id,
                                database_url=tenant_db_url,
                            )
                            db.add(new_tenant)
                            db.commit()
                            logger.info(f"✅ Tenant record added for owner {user.username}")
                        else:
                            existing_tenant.database_url = tenant_db_url
                            db.commit()
                            logger.info(f"ℹ️ Tenant record updated for owner {user.username}")

                        # ✅ Initialize tenant tables
                        base_url, schema = (
                            tenant_db_url.split("#", 1)
                            if "#" in tenant_db_url
                            else (tenant_db_url, "public")
                        )
                        ensure_tenant_tables(base_url, schema)
                        logger.info(f"✅ Tenant tables ensured for {user.username} in schema '{schema}'")

                    except Exception as e:
                        logger.error(f"❌ Failed to create tenant schema for owner {user.username}: {e}")
                        send_message(chat_id, "❌ Could not initialize tenant database.")
                        return {"ok": True}

            else:
                # -------------------- New user: create owner by default --------------------
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

                # ✅ Create tenant schema immediately
                try:
                    tenant_db_url = create_tenant_db(chat_id)
                    new_user.tenant_schema = tenant_db_url
                    db.commit()

                    # ✅ Add tenant record to central table
                    new_tenant = Tenant(
                        tenant_id=str(chat_id),
                        store_name=f"Owner{chat_id}",
                        telegram_owner_id=chat_id,
                        database_url=tenant_db_url
                    )
                    db.add(new_tenant)
                    db.commit()
                    logger.info(f"✅ Tenant record created for {generated_username}")

                    # Initialize tenant tables
                    base_url, schema_name = (
                        tenant_db_url.split("#", 1)
                        if "#" in tenant_db_url
                        else (tenant_db_url, "public")
                    )
                    ensure_tenant_tables(base_url, schema_name)
                    logger.info(f"✅ Tenant tables ensured for new owner {generated_username} in schema '{schema_name}'")

                except Exception as e:
                    logger.error(f"❌ Failed to create tenant schema for new owner {generated_username}: {e}")
                    send_message(chat_id, "❌ Could not initialize tenant database.")
                    return {"ok": True}

                send_owner_credentials(chat_id, generated_username, generated_password)
                send_message(chat_id, "🏪 Let's set up your shop! Please enter the shop name:")
                user_states[chat_id] = {"action": "setup_shop", "step": 1, "data": {}}

            return {"ok": True}


        # -------------------- Login flow --------------------
        if chat_id in user_states:
            state = user_states[chat_id]
            action = state.get("action")
            step = state.get("step", 1)
            data = state.get("data", {})

            if action == "login" and step == 1:
                entered_text = text.strip()
                user = db.query(User).filter(User.chat_id == chat_id).first()

                # -------------------- Shopkeeper first-time login ("username password") --------------------
                if not user and " " in entered_text:
                    username, password = entered_text.split(" ", 1)
                    candidate = db.query(User).filter(User.username == username).first()
                    if candidate and verify_password(password, candidate.password_hash):
                        candidate.chat_id = chat_id
                        db.commit()
                        user = candidate

                # -------------------- Invalid credentials --------------------
                if not user:
                    send_message(chat_id, "❌ Invalid credentials. Please try again or /start.")
                    user_states.pop(chat_id, None)
                    return {"ok": True}

                # -------------------- Verify password --------------------
                if not verify_password(entered_text, user.password_hash):
                    send_message(chat_id, "❌ Incorrect password. Please try again:")
                    return {"ok": True}

                # ✅ Login successful
                send_message(chat_id, f"✅ Login successful! Welcome, {user.name}.")
                user_states.pop(chat_id, None)

                # -------------------- Link tenant schema from Tenant table --------------------
                try:
                    tenant = db.query(Tenant).filter(Tenant.telegram_owner_id == chat_id).first()
                    if tenant:
                        # Prefer explicit tenant_schema field, fallback to database_url for backward compatibility
                        user.tenant_schema = tenant.database_url
                        db.commit()
                        logger.info(f"✅ Tenant schema linked for {user.username}: {user.tenant_schema}")
                    else:
                        logger.warning(f"⚠️ No tenant record found for {user.username} ({chat_id})")
                except Exception as e:
                    logger.error(f"⚠️ Tenant schema lookup failed for {user.username}: {e}")

                # -------------------- Ensure tenant schema --------------------
                tenant_db_url = None

                try:
                    if user.role == "owner":
                        logger.debug(f"LOGIN DEBUG: Owner {user.username} tenant_schema = {user.tenant_schema}")
                        if not user.tenant_schema:
                            tenant_db_url = create_tenant_db(user.chat_id)
                            user.tenant_schema = tenant_db_url
                            db.commit()
                            logger.info(f"✅ Tenant schema created for owner {user.username}: {tenant_db_url}")
                        else:
                            tenant_db_url = user.tenant_schema

                    elif user.role == "shopkeeper":
                        owner = db.query(User).filter(User.user_id == user.owner_id).first()
                        if not owner or not owner.tenant_schema:
                            send_message(chat_id, "❌ Unable to access tenant database. Contact support.")
                            return {"ok": True}
                        tenant_db_url = owner.tenant_schema

                    else:  # fallback
                        tenant_db_url = user.tenant_schema

                    # -------------------- Auto-create tenant record if missing --------------------
                    if not tenant_db_url or tenant_db_url.endswith("#public"):
                        tenant = db.query(Tenant).filter(Tenant.owner_id == user.user_id).first()
                        if not tenant:
                            logger.warning(f"⚠️ No tenant record found for {user.username} ({chat_id}). Creating new one...")
                            tenant_db_url = create_tenant_db(user.chat_id)
                            user.tenant_schema = tenant_db_url
                            db.commit()
                            logger.info(f"✅ Tenant schema created for {user.username}: {tenant_db_url}")
                        else:
                            tenant_db_url = tenant.database_url
                            user.tenant_schema = tenant_db_url
                            db.commit()
                            logger.info(f"✅ Tenant schema restored for {user.username}: {tenant_db_url}")

                    # -------------------- Tenant DB Initialization --------------------
                    if not tenant_db_url:
                        send_message(chat_id, "⚠️ Tenant database missing. Please contact support.")
                        return {"ok": True}

                    tenant_db = get_tenant_session(tenant_db_url, chat_id)
                    if tenant_db is None:
                        logger.warning(f"⚠️ Tenant DB connection failed for {user.username}: {tenant_db_url}")
                        send_message(chat_id, "⚠️ Unable to access tenant database. Please contact support.")
                        return {"ok": True}

                    # Extract base_url and schema_name for table creation
                    if "#" in tenant_db_url:
                        base_url, schema_name = tenant_db_url.split("#", 1)
                    else:
                        base_url = tenant_db_url
                        schema_name = "public"

                    ensure_tenant_tables(base_url, schema_name)
                    logger.info(f"✅ Tenant tables ensured for {user.username} in schema '{schema_name}'")

                except Exception as e:
                    logger.error(f"❌ Tenant DB session init failed for {user.username}: {e}")
                    send_message(chat_id, "❌ Unable to access tenant database. Contact support.")
                    return {"ok": True}

                # -------------------- Show main menu --------------------
                kb = main_menu(user.role)
                send_message(chat_id, "🏠 Main Menu:", keyboard=kb)
                return {"ok": True}


            # -------------------- Shop Setup (Owner only) --------------------
            elif action == "setup_shop" and user.role == "owner":
                if step == 1:  # Shop Name
                    shop_name = text.strip()
                    if shop_name:
                        data["name"] = shop_name
                        user_states[chat_id] = {"action": action, "step": 2, "data": data}
                        send_message(chat_id, "📍 Now enter the shop location:")
                    else:
                        send_message(chat_id, "❌ Shop name cannot be empty. Please enter your shop name:")

                elif step == 2:  # Shop Location
                    location = text.strip()
                    if location:
                        data["location"] = location
                        user_states[chat_id] = {"action": action, "step": 3, "data": data}
                        send_message(chat_id, "📞 Finally, enter the shop contact number:")
                    else:
                        send_message(chat_id, "❌ Location cannot be empty. Please enter your shop location:")

                elif step == 3:  # Shop Contact
                    contact = text.strip()
                    if not contact:
                        send_message(chat_id, "❌ Contact cannot be empty. Enter shop contact number:")
                        return {"ok": False}

                    data["contact"] = contact

                    # -------------------- Check if tenant already exists --------------------
                    existing_tenant = db.query(Tenant).filter(Tenant.telegram_owner_id == chat_id).first()

                    schema_url = None
                    tenant_schema = None

                    if existing_tenant:
                        # Update existing tenant info
                        existing_tenant.store_name = data["name"]
                        existing_tenant.location = data["location"]
                        existing_tenant.contact = contact
                        tenant_schema = existing_tenant.database_url  # keep compatibility

                        send_message(
                            chat_id,
                            f"✅ Your existing shop info has been updated!\n\n"
                            f"🏪 {data['name']}\n📍 {data['location']}\n📞 {contact}"
                        )
                    else:
                        # No existing tenant — create a new one
                        try:
                            schema_url = create_tenant_db(chat_id)
                            tenant_schema = schema_url
                        except Exception as e:
                            logger.error(f"❌ Failed to create tenant schema for owner {user.username}: {e}")
                            send_message(chat_id, "❌ Could not initialize tenant database.")
                            return {"ok": True}

                        # Save tenant info in main DB
                        new_tenant = Tenant(
                            tenant_id=str(uuid.uuid4()),
                            telegram_owner_id=chat_id,
                            store_name=data["name"],
                            database_url=schema_url,  # full URL + schema tag
                            location=data["location"],
                            contact=contact,
                        )
                        db.add(new_tenant)
                        db.commit()

                        send_message(
                            chat_id,
                            f"✅ Shop info saved!\n\n"
                            f"🏪 {data['name']}\n📍 {data['location']}\n📞 {contact}"
                        )

                    # -------------------- Link owner to tenant schema --------------------
                    user.tenant_schema = tenant_schema
                    db.commit()

                    # -------------------- Ensure tenant tables exist --------------------
                    try:
                        base_url, schema_name = (
                            tenant_schema.split("#", 1) if "#" in tenant_schema else (tenant_schema, "public")
                        )
                        ensure_tenant_tables(base_url, schema_name)
                        logger.info(f"✅ Tenant tables ensured for owner {user.username} in schema '{schema_name}'")
                    except Exception as e:
                        logger.error(f"❌ Failed to initialize tenant tables for owner {user.username}: {e}")
                        send_message(chat_id, "⚠️ Shop created but tenant tables could not be initialized.")
                        return {"ok": True}

                    # -------------------- Show Owner Main Menu --------------------
                    kb_dict = main_menu(user.role)
                    send_message(chat_id, "🏠 Main Menu:", kb_dict)

                    # Clear user state
                    user_states.pop(chat_id, None)

            # -------------------- Shopkeeper Creation / Management --------------------
            elif action == "manage_shopkeepers" and user.role == "owner":
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

                    # -------------------- Generate Credentials --------------------
                    username = create_username(f"SK{int(time.time())}")
                    password = generate_password()
                    password_hash = hash_password(password)

                    # -------------------- Validate tenant schema --------------------
                    if not user.tenant_schema:
                        send_message(chat_id, "⚠️ Owner tenant database missing. Please contact support.")
                        return {"ok": True}

                    # -------------------- Save Shopkeeper --------------------
                    new_sk = User(
                        name=data["name"],
                        username=username,
                        password_hash=password_hash,
                        email=contact if "@" in contact else None,
                        chat_id=None,  # will link on Telegram login
                        role="shopkeeper",
                        owner_id=user.user_id,
                        tenant_schema=user.tenant_schema  # ✅ unified tenant linkage
                    )
                    db.add(new_sk)
                    db.commit()
                    db.refresh(new_sk)

                    # -------------------- Notify Owner --------------------
                    send_message(
                        chat_id,
                        f"✅ Shopkeeper created successfully!\n\n"
                        f"👤 Name: {data['name']}\n"
                        f"🔑 Username: {username}\n"
                        f"🔑 Password: {password}\n"
                        f"📞 Contact: {contact}"
                    )

                    # -------------------- Reset & Show Menu --------------------
                    user_states.pop(chat_id, None)
                    kb_dict = main_menu(user.role)
                    send_message(chat_id, "🏠 Main Menu:", kb_dict)
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
                    if product_name:
                        data["name"] = product_name
                        user_states[chat_id] = {"action": action, "step": 2, "data": data}
                        send_message(chat_id, "📦 Enter quantity:")
                    else:
                        send_message(chat_id, "❌ Product name cannot be empty. Please enter again:")

                elif step == 2:  # Quantity
                    try:
                        qty = int(text.strip())
                        if qty < 0:
                            raise ValueError
                        data["quantity"] = qty
                        user_states[chat_id] = {"action": action, "step": 3, "data": data}
                        send_message(chat_id, "📏 Enter unit type (e.g., piece, pack, box, carton):")
                    except ValueError:
                        send_message(chat_id, "❌ Invalid quantity. Please enter a positive number:")

                elif step == 3:  # Unit Type
                    unit_type = text.strip()
                    if unit_type:
                        data["unit_type"] = unit_type
                        if user.role == "owner":
                            # Owner continues to set price and thresholds
                            user_states[chat_id] = {"action": action, "step": 4, "data": data}
                            send_message(chat_id, "💲 Enter product price:")
                        else:
                            # Shopkeeper: send notification to owner for approval
                            add_product_pending_approval(tenant_db, chat_id, data)
                            tenant_db.commit()
                            send_message(chat_id, f"✅ Product *{data['name']}* added for approval. Owner will review.")
                            notify_owner_of_new_product(chat_id, data)
                            user_states.pop(chat_id, None)
                    else:
                        send_message(chat_id, "❌ Unit type cannot be empty. Please enter:")

                elif step == 4:  # Price (Owner only)
                    try:
                        price = float(text.strip())
                        if price <= 0:
                            raise ValueError
                        data["price"] = price
                        user_states[chat_id] = {"action": action, "step": 5, "data": data}
                        send_message(chat_id, "📊 Enter minimum stock level (e.g., 10):")
                    except ValueError:
                        send_message(chat_id, "❌ Invalid price. Please enter a positive number:")

                elif step == 5:  # Min Stock Level (Owner)
                    try:
                        min_stock = int(text.strip())
                        if min_stock < 0:
                            raise ValueError
                        data["min_stock_level"] = min_stock
                        user_states[chat_id] = {"action": action, "step": 6, "data": data}
                        send_message(chat_id, "⚠️ Enter low stock threshold (alert level):")
                    except ValueError:
                        send_message(chat_id, "❌ Invalid number. Please enter a valid minimum stock level:")

                elif step == 6:  # Low Stock Threshold (Owner)
                    try:
                        threshold = int(text.strip())
                        if threshold < 0:
                            raise ValueError
                        data["low_stock_threshold"] = threshold

                        # Save product
                        add_product(tenant_db, chat_id, data)
                        tenant_db.commit()
                        send_message(chat_id, f"✅ Product *{data['name']}* added successfully.")
                        user_states.pop(chat_id, None)
                    except ValueError:
                        send_message(chat_id, "❌ Invalid number. Please enter a valid low stock threshold:")

                    # -------------------- Return to role-based main menu --------------------
                    user = db.query(User).filter(User.chat_id == chat_id).first()
                    if user:
                        kb = main_menu(user.role)
                        send_message(chat_id, "🏠 Main Menu:", keyboard=kb)

            # -------------------- Update Product (owner only, step-by-step) --------------------
            elif action == "awaiting_update" and user.role == "owner":
                # ✅ Always use safe helper (ensures tenant is linked and session is valid)
                tenant_db = ensure_tenant_session(chat_id, db)
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
                    matches = tenant_db.query(ProductORM).filter(ProductORM.name.ilike(f"%{query_text}%")).all()

                    if not matches:
                        send_message(chat_id, f"⚠️ No products found matching '{query_text}'.\n"
                                              "Click ➕ Add Product to add it, or go back to the main menu.")
                        user_states[chat_id] = {}  # reset state
                        return {"ok": True}

                    if len(matches) == 1:
                        selected = matches[0]
                        data["product_id"] = selected.product_id
                        user_states[chat_id] = {"action": "awaiting_update", "step": 2, "data": data}
                        send_message(chat_id, f"✏️ Updating *{selected.name}*.\nEnter NEW name (or send `-` to keep current):")
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
                        if val and val != "-":
                            data["new_name"] = val
                        user_states[chat_id] = {"action": "awaiting_update", "step": 3, "data": data}
                        send_message(chat_id, "💲 Enter new price (or send `-` to keep current):")
                        return {"ok": True}

                    if step == 3:  # price
                        val = text.strip()
                        if val and val != "-":
                            try:
                                data["new_price"] = float(val)
                            except ValueError:
                                send_message(chat_id, "❌ Invalid price. Enter a number or `-` to skip:")
                                return {"ok": True}
                        user_states[chat_id] = {"action": "awaiting_update", "step": 4, "data": data}
                        send_message(chat_id, "🔢 Enter new quantity (or send `-` to keep current):")
                        return {"ok": True}

                    if step == 4:  # quantity
                        val = text.strip()
                        if val and val != "-":
                            try:
                                data["new_quantity"] = int(val)
                            except ValueError:
                                send_message(chat_id, "❌ Invalid quantity. Enter a number or `-` to skip:")
                                return {"ok": True}
                        user_states[chat_id] = {"action": "awaiting_update", "step": 5, "data": data}
                        send_message(chat_id, "📦 Enter new unit type (or send `-` to keep current):")
                        return {"ok": True}

                    if step == 5:  # unit
                        val = text.strip()
                        if val and val != "-":
                            data["new_unit"] = val
                        user_states[chat_id] = {"action": "awaiting_update", "step": 6, "data": data}
                        send_message(chat_id, "📊 Enter new minimum stock level (or send `-` to keep current):")
                        return {"ok": True}

                    if step == 6:  # min stock
                        val = text.strip()
                        if val and val != "-":
                            try:
                                data["new_min_stock"] = int(val)
                            except ValueError:
                                send_message(chat_id, "❌ Invalid number. Enter an integer or `-` to skip:")
                                return {"ok": True}
                        user_states[chat_id] = {"action": "awaiting_update", "step": 7, "data": data}
                        send_message(chat_id, "⚠️ Enter new low stock threshold (or send `-` to keep current):")
                        return {"ok": True}

                    if step == 7:  # low threshold
                        val = text.strip()
                        if val and val != "-":
                            try:
                                data["new_low_threshold"] = int(val)
                            except ValueError:
                                send_message(chat_id, "❌ Invalid number. Enter an integer or `-` to skip:")
                                return {"ok": True}

                        # ✅ Update product in DB
                        update_product(tenant_db, chat_id, product, data)
                        tenant_db.commit()
                        send_message(chat_id, f"✅ Product *{product.name}* updated successfully.")
                        user_states.pop(chat_id, None)
                        return {"ok": True}


            # -------------------- Record Sale (step-by-step, search by name) --------------------
            elif action == "awaiting_sale":
                # Ensure tenant session is available
                tenant_db = get_tenant_session(user.tenant_schema, chat_id)
                if tenant_db is None:
                    send_message(chat_id, "❌ Unable to access tenant database.")
                    return {"ok": True}

                data = state.get("data", {})

                # STEP 1: search by product name
                if step == 1:
                    if not text:
                        send_message(chat_id, "🛒 Enter product name to sell:")
                        return {"ok": True}

                    matches = tenant_db.query(ProductORM).filter(ProductORM.name.ilike(f"%{text}%")).all()
                    if not matches:
                        send_message(chat_id, "⚠️ No products found with that name. Try again:")
                        return {"ok": True}

                    if len(matches) == 1:
                        selected = matches[0]
                        data["product_id"] = selected.product_id
                        data["unit_type"] = selected.unit_type
                        user_states[chat_id] = {"action": "awaiting_sale", "step": 2, "data": data}
                        send_message(chat_id, f"📦 Selected {selected.name} ({selected.unit_type}). Enter quantity sold:")
                        return {"ok": True}

                    # multiple matches -> show inline keyboard for user to pick
                    kb_rows = [
                        [{"text": f"{p.name} — Stock: {p.stock} ({p.unit_type})", "callback_data": f"select_sale:{p.product_id}"}]
                        for p in matches
                    ]
                    kb_rows.append([{"text": "⬅️ Cancel", "callback_data": "back_to_menu"}])
                    send_message(chat_id, "🔹 Multiple products found. Please select:", {"inline_keyboard": kb_rows})
                    return {"ok": True}

                # STEP 2: quantity
                elif step == 2:
                    try:
                        qty = int(text.strip())
                        if qty <= 0:
                            raise ValueError("quantity must be > 0")
                        data["quantity"] = qty
                        user_states[chat_id] = {"action": "awaiting_sale", "step": 3, "data": data}
                        send_message(chat_id, "💰 Enter payment type (full, partial, credit):")
                    except ValueError:
                        send_message(chat_id, "❌ Invalid quantity. Enter a positive integer:")
                    return {"ok": True}

                # STEP 3: payment type
                elif step == 3:
                    payment_type = text.strip().lower()
                    if payment_type not in ["full", "partial", "credit"]:
                        send_message(chat_id, "❌ Invalid type. Choose: full, partial, credit:")
                        return {"ok": True}

                    data["payment_type"] = payment_type
                    if payment_type == "full":
                        data["amount_paid"] = None
                        data["pending_amount"] = 0
                        data["change_left"] = 0
                        user_states[chat_id] = {"action": "awaiting_sale", "step": 6, "data": data}
                        send_message(chat_id, "✅ Full payment selected. Confirm sale? (yes/no)")
                    else:
                        user_states[chat_id] = {"action": "awaiting_sale", "step": 4, "data": data}
                        send_message(chat_id, "💵 Enter amount paid by customer:")
                    return {"ok": True}

                # STEP 4: amount paid (partial / credit)
                elif step == 4:
                    try:
                        amount_paid = float(text.strip())
                        data["amount_paid"] = amount_paid
                        product = tenant_db.query(ProductORM).filter(ProductORM.product_id == data["product_id"]).first()
                        total_price = float(product.price) * data["quantity"]
                        data["pending_amount"] = max(total_price - amount_paid, 0)
                        data["change_left"] = max(amount_paid - total_price, 0)

                        user_states[chat_id] = {"action": "awaiting_sale", "step": 5, "data": data}
                        send_message(chat_id, "👤 Enter customer name:")
                    except ValueError:
                        send_message(chat_id, "❌ Invalid number. Enter a valid amount:")
                    return {"ok": True}

                # STEP 5: customer name
                elif step == 5:
                    customer_name = text.strip()
                    if not customer_name:
                        send_message(chat_id, "❌ Name cannot be empty. Enter customer name:")
                        return {"ok": True}
                    data["customer_name"] = customer_name
                    user_states[chat_id] = {"action": "awaiting_sale", "step": 7, "data": data}
                    send_message(chat_id, "📞 Enter customer contact number:")
                    return {"ok": True}

                # STEP 6: confirm sale for full payment
                elif step == 6:
                    if text.strip().lower() != "yes":
                        send_message(chat_id, "❌ Sale cancelled.")
                        user_states.pop(chat_id, None)
                        return {"ok": True}
                    try:
                        record_sale(tenant_db, chat_id, data)
                        send_message(chat_id, f"✅ Sale recorded successfully: {data['quantity']} {data['unit_type']} sold.")
                    except Exception as e:
                        send_message(chat_id, f"⚠️ Failed to record sale: {str(e)}")
                    user_states.pop(chat_id, None)
                    return {"ok": True}

                # STEP 7: customer contact
                elif step == 7:
                    customer_contact = text.strip()
                    if not customer_contact:
                        send_message(chat_id, "❌ Contact cannot be empty. Enter customer contact number:")
                        return {"ok": True}
                    data["customer_contact"] = customer_contact
                    user_states[chat_id] = {"action": "awaiting_sale", "step": 8, "data": data}
                    send_message(chat_id, f"✅ Customer info recorded. Confirm sale? (yes/no)")
                    return {"ok": True}

                # STEP 8: final confirmation
                elif step == 8:
                    if text.strip().lower() != "yes":
                        send_message(chat_id, "❌ Sale cancelled.")
                        user_states.pop(chat_id, None)
                        return {"ok": True}
                    try:
                        record_sale(tenant_db, chat_id, data)
                        send_message(chat_id, f"✅ Sale recorded successfully: {data['quantity']} {data['unit_type']} sold.")
                    except Exception as e:
                        send_message(chat_id, f"⚠️ Failed to record sale: {str(e)}")
                    user_states.pop(chat_id, None)
                    return {"ok": True}


        # -------------------- Handle callbacks --------------------
        if "callback_query" in data:
            chat_id = data["callback_query"]["message"]["chat"]["id"]
            action = data["callback_query"]["data"]
            callback_id = data["callback_query"]["id"]

            logger.info(f"🎯 Callback received: {action} from chat_id={chat_id}")

            # ✅ Answer callback to remove Telegram spinner
            requests.post(
                f"{TELEGRAM_API_URL}/answerCallbackQuery",
                json={"callback_query_id": callback_id}
            )

            # -------------------- Get user and role --------------------
            user = get_user_by_chat(chat_id)
            if not user:
                logger.warning(f"⚠️ No user found for chat_id={chat_id}")
                send_message(chat_id, "❌ User not found in system.")
                return {"ok": True}

            logger.debug(f"👤 User found: {user.username}, role={user.role}, tenant_schema={getattr(user, 'tenant_schema', None)}")
            role = user.role

            # Initialize tenant_db as None — we’ll fetch it per action
            tenant_db = None

            # -------------------- Cancel button --------------------
            if action == "back_to_menu":
                user_states.pop(chat_id, None)
                kb_dict = main_menu(role)
                send_message(chat_id, "🏠 Main Menu:", kb_dict)
                return {"ok": True}

            # -------------------- Shop Setup (Owner only) --------------------
            elif action == "setup_shop" and role == "owner":
                send_message(chat_id, "🏪 Please enter your shop name:")
                user_states[chat_id] = {"action": "setup_shop", "step": 1, "data": {}}

            # -------------------- Create Shopkeeper --------------------
            elif action == "create_shopkeeper":
                if role != "owner":
                    send_message(chat_id, "❌ Only owners can create shopkeepers.")
                    return {"ok": True}

                user_states[chat_id] = {"action": "create_shopkeeper", "step": 1, "data": {}}
                send_message(chat_id, "👤 Enter a username for the new shopkeeper:")
                return {"ok": True}

            # -------------------- Add Product --------------------
            elif action == "add_product":
                send_message(chat_id, "➕ Add a new product! 🛒\n\nEnter product name:")
                user_states[chat_id] = {"action": "awaiting_product", "step": 1, "data": {}}

            # -------------------- Update Product --------------------
            elif action == "update_product":
                tenant_db = ensure_tenant_session(chat_id, db)
                if not tenant_db:
                    send_message(chat_id, "⚠️ Tenant database not linked. Please restart with /start.")
                    return {"ok": True}

                logger.debug(f"🧩 In update_product flow, tenant_db ready for chat_id={chat_id}")
                user_states[chat_id] = {"action": "awaiting_update", "step": 1, "data": {}}
                send_message(chat_id, "✏️ Enter the product name to update:")

            # -------------------- Paginated Product List --------------------
            elif action.startswith("products_page:"):
                try:
                    page = int(action.split(":")[1])
                except (IndexError, ValueError):
                    page = 1

                tenant_db = ensure_tenant_session(chat_id, db)
                if not tenant_db:
                    send_message(chat_id, "⚠️ Tenant database not linked. Please restart with /start.")
                    return {"ok": True}

                text, kb = products_page_view(tenant_db, page=page)
                send_message(chat_id, text, kb)

            # -------------------- Product Selection / Multiple Buttons --------------------
            elif action.startswith("select_update:") or action.startswith("select_product:"):
                logger.info(f"🧩 Callback triggered: {action} from chat_id {chat_id}")

                # Extract product ID
                try:
                    product_id = int(action.split(":")[1])
                    logger.info(f"🔹 Parsed product_id={product_id}")
                except (IndexError, ValueError):
                    send_message(chat_id, "⚠️ Invalid product selection.")
                    return {"ok": True}

                # Ensure tenant DB session
                user = db.query(User).filter(User.chat_id == chat_id).first()
                tenant_db_url = getattr(user, "tenant_schema", None)

                # 🔧 Reconstruct URL if needed
                if not tenant_db_url or "#" not in tenant_db_url:
                    base_url = os.getenv("DATABASE_URL")
                    schema_name = tenant_db_url if tenant_db_url and tenant_db_url.startswith("tenant_") else f"tenant_{chat_id}"
                    tenant_db_url = f"{base_url}#{schema_name}"
                    logger.info(f"🔗 Reconstructed tenant_db_url for {user.username}: {tenant_db_url}")

                # ✅ Create tenant session
                logger.info(f"🔗 Opening tenant session for product fetch: {tenant_db_url}")
                tenant_db = get_tenant_session(tenant_db_url, chat_id)

                # -------------------- Fetch product --------------------
                logger.info("🚦 Entering FETCH PRODUCT block with product_id=%s", product_id)
                try:
                    active_schema = tenant_db.execute(text("SHOW search_path")).scalar()
                    logger.info(f"🧭 Active search_path: {active_schema}")

                    # 📊 Quick row count
                    count = tenant_db.execute(text("SELECT COUNT(*) FROM products")).scalar()
                    logger.info(f"📦 Product count in schema: {count}")

                    # -------------------- Deep Debug Diagnostics --------------------
                    try:
                        logger.info("🔍 Running tenant ORM diagnostics...")

                        # Check search_path again for confirmation
                        current_schema = tenant_db.execute(text("SHOW search_path")).scalar()
                        logger.info(f"🧭 Active search_path (confirm): {current_schema}")

                        # List all schemas that have a 'products' table
                        schema_check = tenant_db.execute(text("""
                            SELECT table_schema, COUNT(*) AS total_tables
                            FROM information_schema.tables
                            WHERE table_name = 'products'
                            GROUP BY table_schema
                            ORDER BY table_schema;
                        """)).fetchall()
                        logger.info(f"🏗️ Table presence by schema: {schema_check}")

                        # ✅ NEW: Show visible products
                        visible_products = tenant_db.execute(text("SELECT product_id, name, stock FROM products ORDER BY product_id")).fetchall()
                        logger.info(f"📋 Visible products in current schema: {visible_products}")

                        # ✅ NEW: Check ORM perspective explicitly
                        all_orm_products = tenant_db.query(ProductORM).all()
                        logger.info(f"🧱 ORM sees {len(all_orm_products)} products: {[p.name for p in all_orm_products]}")

                    except Exception as diag_e:
                        logger.error(f"❌ Debug diagnostics failed: {diag_e}", exc_info=True)

                    # 🚀 Fetch the specific product
                    product = tenant_db.query(ProductORM).filter(ProductORM.product_id == product_id).first()
                    logger.info(f"📦 ORM product result for ID {product_id}: {product}")

                    if not product:
                        send_message(chat_id, "⚠️ No products found.")
                        return {"ok": True}

                except Exception as e:
                    logger.error(f"❌ DB fetch failed for product_id={product_id}: {e}", exc_info=True)
                    send_message(chat_id, "⚠️ Database error while fetching product.")
                    return {"ok": True}

                # Product not found
                if not product:
                    logger.warning(f"⚠️ Product with ID {product_id} not found in tenant schema.")
                    kb = types.InlineKeyboardMarkup()
                    kb.add(types.InlineKeyboardButton("🏠 Back to Main Menu", callback_data="back_to_menu"))
                    send_message(chat_id, f"⚠️ No product found matching ID {product_id}.", kb)
                    return {"ok": True}

                # Product found — start update flow
                safe_name_html = html.escape(product.name)
                text_msg = (
                    f"✏️ Updating <b>{safe_name_html}</b>\n\n"
                    f"💰 Price: {product.price}\n📦 Stock: {product.stock}\n🧾 Unit: {product.unit_type}\n\n"
                    "Please enter the new name or send '-' to keep the current name:"
                )

                user_states.pop(chat_id, None)
                user_states[chat_id] = {
                    "action": "awaiting_update",
                    "step": 1,
                    "data": {"product_id": product_id, "tenant_db_url": tenant_db_url},
                }

                send_message(chat_id, text_msg, parse_mode="HTML")
                return {"ok": True}

            # -------------------- Record Sale --------------------
            elif action == "record_sale":
                tenant_db = ensure_tenant_session(chat_id, db)
                if not tenant_db:
                    send_message(chat_id, "⚠️ Cannot record sale: tenant DB unavailable.")
                    return {"ok": True}

                send_message(chat_id, "💰 Record a new sale!\nEnter product name:")
                user_states[chat_id] = {"action": "awaiting_sale", "step": 1, "data": {}}

            # -------------------- Handle selected product from inline keyboard --------------------
            elif action.startswith("select_sale:"):
                tenant_db = ensure_tenant_session(chat_id, db)
                if not tenant_db:
                    send_message(chat_id, "⚠️ Cannot record sale: tenant DB unavailable.")
                    return {"ok": True}

                try:
                    product_id = int(action.split(":")[1])
                except (IndexError, ValueError):
                    send_message(chat_id, "⚠️ Invalid product selection.")
                    return {"ok": True}

                product = tenant_db.query(ProductORM).filter(ProductORM.product_id == product_id).first()
                if not product:
                    send_message(chat_id, "⚠️ Product not found. Try again.")
                    return {"ok": True}

                user_states[chat_id] = {
                    "action": "awaiting_sale",
                    "step": 2,
                    "data": {"product_id": product.product_id, "unit_type": product.unit_type},
                }

                send_message(chat_id, f"📦 Selected {product.name} ({product.unit_type}). Enter quantity sold:")
                return {"ok": True}

            # -------------------- View Stock --------------------
            elif action == "view_stock":
                tenant_db = ensure_tenant_session(chat_id, db)
                if not tenant_db:
                    send_message(chat_id, "⚠️ Cannot view stock: tenant DB unavailable.")
                    return {"ok": True}

                stock_list = get_stock_list(tenant_db)
                kb_dict = {"inline_keyboard": [[{"text": "⬅️ Back to Menu", "callback_data": "back_to_menu"}]]}
                send_message(chat_id, stock_list, kb_dict)

            # -------------------- Reports Menu --------------------
            elif action == "report_menu":
                kb_dict = report_menu_keyboard(role)
                send_message(chat_id, "📊 Select a report:", kb_dict)

            # -------------------- Help --------------------
            elif action == "help":
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

            else:
                logger.warning(f"⚠️ Unknown callback action received: {action}")
                send_message(chat_id, f"⚠️ Unknown action: {action}")

        return {"ok": True}

    except Exception as e:
        import traceback
        print("❌ Webhook crashed with error:", str(e))
        traceback.print_exc()
        return {"status": "error", "detail": str(e)}
