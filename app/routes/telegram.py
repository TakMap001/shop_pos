# app/routes/telegram.py

from fastapi import APIRouter, Request, Depends
import requests, os
from sqlalchemy.orm import Session
from decimal import Decimal
from datetime import datetime
from sqlalchemy import func, extract
from app.models.central_models import Tenant  # Central DB
from app.models.models import Base as TenantBase
from app.models.models import User, ProductORM, SaleORM  # Tenant DB
from app.database import get_db  # central DB session
from app.telegram_notifications import notify_low_stock, notify_top_product, notify_high_value_sale, send_message
from app.tenants import create_tenant_db, get_engine_for_tenant, get_session_for_tenant
from config import DATABASE_URL
from telebot import types

router = APIRouter()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"

# Tracks multi-step actions per user
user_states = {}  # chat_id -> {"action": "awaiting_shop_name" / "awaiting_product" / "awaiting_update" / "awaiting_sale"}


# -------------------- Helpers --------------------

def get_tenant_session(global_db: Session, owner_chat_id: int):
    tenant = global_db.query(Tenant).filter(Tenant.telegram_owner_id == owner_chat_id).first()
    if not tenant:
        return None
    SessionLocal = get_session_for_tenant(tenant.database_url)  # returns sessionmaker
    return SessionLocal()  # <-- create actual session

def role_menu(chat_id):
    """Role selection menu (Owner vs Shopkeeper)."""
    keyboard = types.InlineKeyboardMarkup()
    keyboard.add(
        types.InlineKeyboardButton("👑 Owner", callback_data="role_owner"),
        types.InlineKeyboardButton("🛍 Shopkeeper", callback_data="role_keeper")
    )
    send_message(chat_id, "👋 Welcome! Please choose your role:", keyboard)


def main_menu(role: str):
    """
    Returns a menu dictionary for the given role.
    """
    keyboard = []

    if role.lower() == "owner":
        keyboard.append([
            {"text": "🏪 Setup My Shop", "callback_data": "setup_shop"}
        ])
        keyboard.append([
            {"text": "➕ Add Product", "callback_data": "add_product"},
            {"text": "✏️ Update Product", "callback_data": "update_product"},
        ])
        keyboard.append([
            {"text": "🛒 Record Sale", "callback_data": "record_sale"},
            {"text": "📦 View Stock", "callback_data": "view_stock"},
        ])
        keyboard.append([
            {"text": "📊 Reports", "callback_data": "reports"},
            {"text": "ℹ️ Help", "callback_data": "help"},
        ])
    else:
        # Shopkeeper menu
        keyboard.append([
            {"text": "🛒 Record Sale", "callback_data": "record_sale"},
            {"text": "📦 View Stock", "callback_data": "view_stock"},
        ])
        keyboard.append([
            {"text": "📊 Reports", "callback_data": "reports"},
            {"text": "ℹ️ Help", "callback_data": "help"},
        ])

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


def add_product(db: Session, chat_id: int, text: str):
    """
    Add a product in a tenant-aware way.
    The `db` session is already connected to the tenant's DB.
    """
    try:
        name, price_str, stock_str = parse_input(text, 3)
        price = float(price_str)
        stock = int(stock_str)

        if price <= 0 or stock < 0:
            raise ValueError("Price must be > 0 and stock >= 0")
    except Exception as e:
        send_message(chat_id, f"❌ Invalid input: {str(e)}\nSend as: `name;price;stock` or `name,price,stock`")
        return

    # Tenant DB only contains products for this tenant
    existing = db.query(ProductORM).filter(func.lower(ProductORM.name) == name.lower()).first()
    if existing:
        send_message(chat_id, f"❌ Product '{name}' already exists.")
        return

    new_product = ProductORM(name=name, price=price, stock=stock)
    try:
        db.add(new_product)
        db.commit()
        db.refresh(new_product)
    except Exception as e:
        db.rollback()
        send_message(chat_id, f"❌ Database error: {str(e)}")
        return

    send_message(chat_id, f"✅ Product added: {name} — ${price}, Stock: {stock}")


def update_product(db: Session, chat_id: int, text: str):
    """
    Update a product in a tenant-aware way.
    Only products in the current tenant DB are affected.
    """
    try:
        prod_id_str, new_name, price_str, stock_str = parse_input(text, 4)
        prod_id = int(prod_id_str)
        price = float(price_str)
        stock = int(stock_str)

        if price <= 0 or stock < 0:
            raise ValueError("Price must be > 0 and stock >= 0")

        # Tenant DB only contains products for this tenant
        product = db.query(ProductORM).filter(ProductORM.product_id == prod_id).first()
        if not product:
            raise ValueError(f"No product found with ID {prod_id}")
    except Exception as e:
        send_message(chat_id, f"❌ Invalid input: {str(e)}\nSend as: `id;new_name;price;stock` or `id,new_name,price,stock`")
        return

    product.name = new_name
    product.price = price
    product.stock = stock

    try:
        db.commit()
        send_message(chat_id, f"✅ Product updated: {product.name} — ${product.price}, Stock: {product.stock}")
    except Exception as e:
        db.rollback()
        send_message(chat_id, f"❌ Database error: {str(e)}")


# -------------------- Sales --------------------
def record_sale(db: Session, chat_id: int, text: str):
    """
    Record a sale in a tenant-aware way.
    All queries affect only the current tenant DB.
    """
    try:
        product_name, qty_str = parse_input(text, 2)
        qty = int(qty_str)
        if qty <= 0:
            raise ValueError("Quantity must be > 0")
    except Exception as e:
        send_message(chat_id, f"❌ Invalid input: {str(e)}\nSend as: `product_name;quantity` or `product_name,quantity`")
        return

    # Find product in tenant DB
    product = db.query(ProductORM).filter(func.lower(ProductORM.name) == product_name.lower()).first()
    if not product:
        send_message(chat_id, f"❌ Product '{product_name}' not found.")
        return

    # Check stock
    if product.stock < qty:
        send_message(chat_id, f"❌ Insufficient stock. Available: {product.stock}")
        return

    # Find user in tenant DB
    user = db.query(User).filter(User.user_id == chat_id).first()
    if not user:
        send_message(chat_id, "❌ No users available in the system.")
        return

    # Calculate total and create sale
    total_amount = Decimal(product.price) * qty
    sale = SaleORM(
        user_id=user.user_id,
        product_id=product.product_id,
        quantity=qty,
        total_amount=total_amount
    )
    product.stock -= qty

    # Commit to DB
    try:
        db.add(sale)
        db.commit()
        db.refresh(sale)
    except Exception as e:
        db.rollback()
        send_message(chat_id, f"❌ Database error: {str(e)}")
        return

    # Notify user
    send_message(chat_id, f"✅ Sale recorded: {qty} × {product.name} = ${total_amount}")
    send_message(chat_id, get_stock_list(db))

    # --- Telegram Notifications ---
    notify_low_stock(db, product)
    notify_top_product(db, product)
    notify_high_value_sale(db, sale)

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

    else:
        return "❌ Unknown report type."

# -------------------- Webhook --------------------

@router.post("/telegram/webhook")
async def telegram_webhook(request: Request, db: Session = Depends(get_db)):
    try:
        data = await request.json()
        print("📩 Incoming Telegram update:", data)

        def get_user(chat_id: int):
            return db.query(User).filter(User.user_id == chat_id).first()

        def get_tenant_session(user: User):
            if not user or not user.tenant_db_url:
                return None
            return get_session_for_tenant(user.tenant_db_url)

        # -------------------- Handle messages --------------------
        if "message" in data:
            chat_id = data["message"]["chat"]["id"]
            text = data["message"].get("text", "").strip()
            user = get_user(chat_id)

            if not user:
                # Auto-register first-time user
                new_user = User(
                    user_id=chat_id,
                    name=f"User{chat_id}",
                    email=f"{chat_id}@example.com",
                    password_hash="",
                    role=None,
                    tenant_db_url=None
                )
                db.add(new_user)
                db.commit()
                db.refresh(new_user)

                role_menu(chat_id)
                return {"ok": True}

            role = user.role
            tenant_db = get_tenant_session(user)
            if not tenant_db and role == "owner":
                send_message(chat_id, "❌ No tenant DB found. Please register as owner first.")
                return {"ok": True}

            # -------------------- Multi-step flows --------------------
            if chat_id in user_states:
                state = user_states[chat_id]
                action = state.get("action")
                step = state.get("step", 1)
                data = state.get("data", {})

                # -------------------- Shop Setup (Owner only) --------------------
                if action == "setup_shop" and role == "owner":
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
                        if contact:
                            data["contact"] = contact
                            tenant = db.query(Tenant).filter(Tenant.telegram_owner_id == chat_id).first()
                            if tenant:
                                tenant.store_name = data["name"]
                                tenant.location = data["location"]
                                tenant.contact = data["contact"]
                                db.commit()

                                # 1️⃣ First send confirmation
                                send_message(
                                    chat_id,
                                    f"✅ Shop information saved successfully!\n\n🏪 {data['name']}\n📍 {data['location']}\n📞 {data['contact']}"
                                )

                                # 2️⃣ Then show main menu
                                kb_dict = main_menu(role="owner")
                                send_message(chat_id, "🏠 Main Menu:", kb_dict)

                            user_states.pop(chat_id)
                        else:
                            send_message(chat_id, "❌ Contact cannot be empty. Please enter the shop contact number:")

                    return {"ok": True}

                # -------------------- Add Product --------------------
                elif action == "awaiting_product":
                    if step == 1:  # Product Name
                        product_name = text.strip()
                        if product_name:
                            data["name"] = product_name
                            user_states[chat_id] = {"action": action, "step": 2, "data": data}
                            send_message(chat_id, "💲 Enter product price:")
                        else:
                            send_message(chat_id, "❌ Product name cannot be empty. Please enter again:")

                    elif step == 2:  # Product Price
                        try:
                            price = float(text.strip())
                            data["price"] = price
                            user_states[chat_id] = {"action": action, "step": 3, "data": data}
                            send_message(chat_id, "📦 Enter product quantity:")
                        except ValueError:
                            send_message(chat_id, "❌ Invalid price. Please enter a number:")

                    elif step == 3:  # Product Quantity
                        try:
                            qty = int(text.strip())
                            data["quantity"] = qty
                            add_product(tenant_db, chat_id, data)
                            send_message(chat_id, f"✅ Product *{data['name']}* added successfully.")
                            user_states.pop(chat_id)
                        except ValueError:
                            send_message(chat_id, "❌ Invalid quantity. Please enter a number:")

                    return {"ok": True}

                # -------------------- Update Product --------------------
                elif action == "awaiting_update":
                    if step == 1:  # Product ID
                        try:
                            product_id = int(text.strip())
                            data["id"] = product_id
                            user_states[chat_id] = {"action": action, "step": 2, "data": data}
                            send_message(chat_id, "✏️ Enter the field to update (name, price, quantity):")
                        except ValueError:
                            send_message(chat_id, "❌ Invalid product ID. Please enter a number:")

                    elif step == 2:  # Field to Update
                        field = text.strip().lower()
                        if field in ["name", "price", "quantity"]:
                            data["field"] = field
                            user_states[chat_id] = {"action": action, "step": 3, "data": data}
                            send_message(chat_id, f"✏️ Enter new value for {field}:")
                        else:
                            send_message(chat_id, "❌ Invalid field. Choose: name, price, or quantity.")

                    elif step == 3:  # New Value
                        field = data["field"]
                        new_value = text.strip()

                        if field == "price":
                            try:
                                new_value = float(new_value)
                            except ValueError:
                                send_message(chat_id, "❌ Invalid price. Please enter a number:")
                                return {"ok": True}

                        elif field == "quantity":
                            try:
                                new_value = int(new_value)
                            except ValueError:
                                send_message(chat_id, "❌ Invalid quantity. Please enter a number:")
                                return {"ok": True}

                        try:
                            update_product(tenant_db, chat_id, data["id"], field, new_value)
                            send_message(chat_id, f"✅ Product {field} updated successfully.")
                        except Exception as e:
                            send_message(chat_id, f"⚠️ Failed to update product: {str(e)}")

                        user_states.pop(chat_id)
                    return {"ok": True}

                # -------------------- Record Sale --------------------
                elif action == "awaiting_sale":
                    if step == 1:  # Product ID
                        try:
                            product_id = int(text.strip())
                            data["id"] = product_id
                            user_states[chat_id] = {"action": action, "step": 2, "data": data}
                            send_message(chat_id, "📦 Enter quantity sold:")
                        except ValueError:
                            send_message(chat_id, "❌ Invalid product ID. Please enter a number:")

                    elif step == 2:  # Quantity
                        try:
                            qty = int(text.strip())
                            data["quantity"] = qty
                            record_sale(tenant_db, chat_id, data)
                            send_message(chat_id, "✅ Sale recorded successfully.")
                            user_states.pop(chat_id)
                        except ValueError:
                            send_message(chat_id, "❌ Invalid quantity. Please enter a number:")
                        except Exception as e:
                            send_message(chat_id, f"⚠️ Failed to record sale: {str(e)}")
                            user_states.pop(chat_id)

                    return {"ok": True}

            # -------------------- Commands --------------------
            if text.lower() in ["/start", "menu"]:
                role_menu(chat_id)
            else:
                send_message(
                    chat_id,
                    f"⚠️ Invalid input or action not allowed for your role ({role}). Type *menu* to see instructions."
                )


        # -------------------- Handle callbacks --------------------
        if "callback_query" in data:
            chat_id = data["callback_query"]["message"]["chat"]["id"]
            action = data["callback_query"]["data"]
            callback_id = data["callback_query"]["id"]

            # ✅ Explicitly answer callback to remove spinner
            requests.post(
                f"{TELEGRAM_API_URL}/answerCallbackQuery",
                json={"callback_query_id": callback_id}
            )

            user = get_user(chat_id)
            if not user:
                return {"ok": True}

            role = user.role
            tenant_db = get_tenant_session(user)

            # -------------------- Role Selection --------------------
            if action == "role_owner":
                user.role = "owner"
                tenant_db_url = DATABASE_URL.rsplit("/", 1)[0] + f"/tenant_{chat_id}"
                create_tenant_db(tenant_db_url)
                engine = get_engine_for_tenant(tenant_db_url)
                TenantBase.metadata.create_all(bind=engine)

                user.tenant_db_url = tenant_db_url
                db.commit()

                kb_dict = main_menu(role=user.role)
                send_message(chat_id, "🏠 Main Menu:", kb_dict)

            elif action == "role_keeper":
                user.role = "keeper"
                db.commit()

                kb_dict = main_menu(role=user.role)
                send_message(chat_id, "🏠 Main Menu:", kb_dict)

            # -------------------- Shop Setup --------------------
            elif action == "setup_shop":
                send_message(chat_id, "🏪 Please enter your shop name:")
                user_states[chat_id] = {"action": "setup_shop"}

            # -------------------- Product Management --------------------
            elif action == "add_product":
                send_message(chat_id, "➕ Please enter the product details in the format:\n\n`Name, Price, Quantity`")
                user_states[chat_id] = {"action": "awaiting_product"}

            elif action == "update_product":
                send_message(chat_id, "✏️ Please enter the product update in the format:\n\n`ProductID, NewName, NewPrice, NewQuantity`\n\n"
                                      "👉 You can leave a field blank if not updating it.")
                user_states[chat_id] = {"action": "awaiting_update"}

            # -------------------- Sales --------------------
            elif action == "record_sale":
                send_message(chat_id, "🛒 Please enter the sale in the format:\n\n`ProductID, Quantity`")
                user_states[chat_id] = {"action": "awaiting_sale"}

            # -------------------- Stock --------------------
            elif action == "view_stock":
                if tenant_db:
                    stock_list = get_stock_list(tenant_db)
                    kb_dict = {
                        "inline_keyboard": [
                            [{"text": "⬅️ Back to Menu", "callback_data": "back_to_menu"}]
                        ]
                    }
                    send_message(chat_id, stock_list, kb_dict)

            # -------------------- Reports --------------------
            elif action.startswith("report_"):
                if role != "owner" and action not in ["report_daily", "report_weekly", "report_monthly"]:
                    send_message(chat_id, "❌ Only owners can access this report.")
                else:
                    report_text = generate_report(tenant_db, action)
                    kb_dict = {
                        "inline_keyboard": [
                            [{"text": "⬅️ Back to Menu", "callback_data": "back_to_menu"}]
                        ]
                    }
                    send_message(chat_id, report_text, kb_dict)

            # -------------------- Help --------------------
            elif action == "help":
                help_text = (
                    "❓ *Help & FAQs*\n\n"
                    "Here are some things you should know:\n\n"
                    "📌 *Getting Started*\n"
                    "• Owners must first *setup the shop* from the Main Menu.\n"
                    "• Shopkeepers can directly *record sales* and *check stock*.\n\n"
                    "🛒 *Managing Products*\n"
                    "• Use *Add Product* to register new items (Name, Price, Quantity).\n"
                    "• Use *Update Product* to adjust details (you can update just price or quantity).\n\n"
                    "📦 *Stock Management*\n"
                    "• Always check *View Stock* before recording a sale.\n"
                    "• Low stock alerts will appear automatically.\n\n"
                    "📊 *Reports*\n"
                    "• Daily, weekly, and monthly sales summaries are available.\n"
                    "• Owners can see all reports, Shopkeepers have limited access.\n\n"
                    "⚠️ *Common Issues*\n"
                    "• If the bot is unresponsive, type /start to reset.\n"
                    "• Always enter details in the format shown when prompted.\n\n"
                    "👨‍💻 Need more help? Contact support."
                )
                kb_dict = {
                    "inline_keyboard": [
                        [{"text": "⬅️ Back to Menu", "callback_data": "back_to_menu"}]
                    ]
                }
                send_message(chat_id, help_text, kb_dict)

            # -------------------- Back to Menu --------------------
            elif action == "back_to_menu":
                kb_dict = main_menu(role=user.role)
                send_message(chat_id, "🏠 Main Menu:", kb_dict)

        return {"ok": True}

    except Exception as e:
        import traceback
        print("❌ Webhook crashed with error:", str(e))
        traceback.print_exc()
        return {"status": "error", "detail": str(e)}
