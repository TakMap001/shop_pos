# app/routes/telegram.py

from fastapi import APIRouter, Request, Depends
import requests, os
from sqlalchemy.orm import Session
from decimal import Decimal
from datetime import datetime
from sqlalchemy import func, extract
from app.database import get_db
from app.models.models import Product as ProductORM, Sale as SaleORM, User
from app.telegram_notifications import notify_low_stock, notify_top_product, notify_high_value_sale, send_message

router = APIRouter()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"


# -------------------- Helpers --------------------

def send_message(chat_id, text, keyboard=None):
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}
    if keyboard:
        payload["reply_markup"] = keyboard
    requests.post(f"{TELEGRAM_API_URL}/sendMessage", json=payload)


def role_menu(chat_id):
    """Role selection menu (Owner vs Shopkeeper)."""
    keyboard = {
        "inline_keyboard": [
            [{"text": "👑 Owner", "callback_data": "role_owner"}],
            [{"text": "🛍 Shopkeeper", "callback_data": "role_keeper"}],
        ]
    }
    send_message(chat_id, "👋 Welcome! Please choose your role:", keyboard)


def main_menu(chat_id, role="keeper"):
    """Main menu changes depending on role."""
    if role == "owner":
        keyboard = {
            "inline_keyboard": [
                [{"text": "➕ Add Product", "callback_data": "add_product"}],
                [{"text": "✏️ Update Product", "callback_data": "update_product"}],
                [{"text": "🛒 Record Sale", "callback_data": "record_sale"}],
                [{"text": "📦 View Stock", "callback_data": "view_stock"}],
                [{"text": "📊 Reports", "callback_data": "reports"}],
                [{"text": "ℹ️ Help", "callback_data": "help"}],
            ]
        }
    else:  # shopkeeper
        keyboard = {
            "inline_keyboard": [
                [{"text": "🛒 Record Sale", "callback_data": "record_sale"}],
                [{"text": "📦 View Stock", "callback_data": "view_stock"}],
                [{"text": "ℹ️ Help", "callback_data": "help"}],
            ]
        }
    send_message(chat_id, "📋 Main Menu:", keyboard)

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

def register_new_user(db: Session, chat_id: int, text: str, role="keeper"):
    """
    Owner adds a new user (shopkeeper or owner) manually.
    Expected input: user_id;name
    """
    try:
        user_id_str, name = parse_input(text, 2)
        new_chat_id = int(user_id_str)
        name = name.strip()
        if not name:
            raise ValueError("Name cannot be empty")
    except Exception as e:
        send_message(chat_id, f"❌ Invalid input: {str(e)}\nSend as: `user_id;name`")
        return

    # Check if user already exists
    existing = db.query(User).filter(User.user_id == new_chat_id).first()
    if existing:
        send_message(chat_id, f"❌ User with ID {new_chat_id} already exists.")
        return

    # Create user
    new_user = User(
        user_id=new_chat_id,
        name=name,
        email=f"{new_chat_id}@example.com",
        password_hash="",
        role=role
    )
    try:
        db.add(new_user)
        db.commit()
        db.refresh(new_user)
    except Exception as e:
        db.rollback()
        send_message(chat_id, f"❌ Database error: {str(e)}")
        return

    send_message(chat_id, f"✅ {role.title()} '{name}' added successfully with ID {new_chat_id}.")

    # Optional: welcome message to the new user
    send_message(new_chat_id, f"👋 Hello {name}! You have been registered as a {role}. Use /start to begin.")



# -------------------- Products --------------------

def get_stock_list(db: Session):
    products = db.query(ProductORM).all()
    if not products:
        return "📦 No products found."
    lines = ["📦 *Stock Levels:*"]
    for p in products:
        lines.append(f"{p.name} — {p.stock}")
    return "\n".join(lines)


def add_product(db: Session, chat_id: int, text: str):
    try:
        name, price_str, stock_str = parse_input(text, 3)
        price = float(price_str)
        stock = int(stock_str)

        if price <= 0 or stock < 0:
            raise ValueError("Price must be > 0 and stock >= 0")
    except Exception as e:
        send_message(chat_id, f"❌ Invalid input: {str(e)}\nSend as: `name;price;stock` or `name,price,stock`")
        return

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
    try:
        prod_id_str, new_name, price_str, stock_str = parse_input(text, 4)
        prod_id = int(prod_id_str)
        price = float(price_str)
        stock = int(stock_str)

        if price <= 0 or stock < 0:
            raise ValueError("Price must be > 0 and stock >= 0")

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
    try:
        product_name, qty_str = parse_input(text, 2)  # local function
        qty = int(qty_str)
        if qty <= 0:
            raise ValueError("Quantity must be > 0")
    except Exception as e:
        send_message(chat_id, f"❌ Invalid input: {str(e)}\nSend as: `product_name;quantity` or `product_name,quantity`")
        return

    # Find product
    product = db.query(ProductORM).filter(func.lower(ProductORM.name) == product_name.lower()).first()
    if not product:
        send_message(chat_id, f"❌ Product '{product_name}' not found.")
        return

    # Check stock
    if product.stock < qty:
        send_message(chat_id, f"❌ Insufficient stock. Available: {product.stock}")
        return

    # Find user
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
    send_message(chat_id, get_stock_list(db))  # local function

    # --- Telegram Notifications ---
    notify_low_stock(db, product)
    notify_top_product(db, product)
    notify_high_value_sale(db, sale)

# -------------------- Generate Reports --------------------
def generate_report(db: Session, report_type: str):
    """Generate report text based on action, consistent with reports.py"""

    if report_type == "report_daily":
        results = (
            db.query(
                func.date(SaleORM.sale_date).label("day"),
                func.sum(SaleORM.quantity).label("total_qty"),
                func.sum(SaleORM.total_amount).label("total_revenue")
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

    elif report_type == "report_weekly":
        results = (
            db.query(
                extract("week", SaleORM.sale_date).label("week"),
                func.sum(SaleORM.quantity).label("total_qty"),
                func.sum(SaleORM.total_amount).label("total_revenue")
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

    elif report_type == "report_monthly":
        now = datetime.now()
        results = (
            db.query(
                ProductORM.name.label("product"),
                func.sum(SaleORM.quantity).label("total_qty"),
                func.sum(SaleORM.total_amount).label("total_revenue")
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

    elif report_type == "report_low_stock":
        products = db.query(ProductORM).filter(ProductORM.stock <= 10).all()
        if not products:
            return "All products have sufficient stock."
        lines = ["⚠️ *Low Stock Products:*"]
        for p in products:
            lines.append(f"{p.name}: {p.stock} units left")
        return "\n".join(lines)

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
            lines.append(f"{r.product}: {r.total_qty} sold, ${float(r.total_revenue)} revenue")
        return "\n".join(lines)

    elif report_type == "report_top_customers":
        results = (
            db.query(
                User.name.label("user"),
                func.sum(SaleORM.quantity).label("total_qty"),
                func.sum(SaleORM.total_amount).label("total_spent")
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

    elif report_type == "report_top_repeat_customers":
        customers = (
            db.query(
                SaleORM.user_id,
                func.count(SaleORM.sale_id).label("num_purchases"),
                func.sum(SaleORM.total_amount).label("total_spent")
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
            user = db.query(User).filter(User.user_id == c.user_id).first()
            name = user.name if user else f"User {c.user_id}"
            lines.append(f"{name}: {c.num_purchases} purchases, ${float(c.total_spent)} spent")
        return "\n".join(lines)

    elif report_type == "report_aov":
        total_orders = db.query(func.count(SaleORM.sale_id)).scalar() or 0
        total_revenue = db.query(func.sum(SaleORM.total_amount)).scalar() or 0
        aov = round(total_revenue / total_orders, 2) if total_orders > 0 else 0
        return f"💰 *Average Order Value*\nTotal Orders: {total_orders}\nTotal Revenue: ${total_revenue}\nAOV: ${aov}"

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

    else:
        return "❌ Unknown report type."

# -------------------- Webhook changes --------------------

@router.post("/telegram/webhook")
async def telegram_webhook(request: Request, db: Session = Depends(get_db)):
    data = await request.json()

    def get_user(chat_id: int):
        user = db.query(User).filter(User.user_id == chat_id).first()
        if not user:
            # Prompt role selection for first-time users
            role_menu(chat_id)
            return None
        return user

    if "message" in data:
        chat_id = data["message"]["chat"]["id"]
        text = data["message"].get("text", "").strip()
        user = get_user(chat_id)
        if not user:
            # User must choose role first
            return {"ok": True}
        role = user.role

        if text.lower() in ["/start", "menu"]:
            role_menu(chat_id)
        else:
            handled = False
            # Only owners can add/update products or register new users
            if role == "owner":
                try:
                    parse_input(text, 3)
                    add_product(db, chat_id, text)
                    handled = True
                except:
                    pass
                if not handled:
                    try:
                        parse_input(text, 4)
                        update_product(db, chat_id, text)
                        handled = True
                    except:
                        pass
                if not handled:
                    try:
                        parse_input(text, 2)
                        register_new_user(db, chat_id, text, role="keeper")
                        handled = True
                    except:
                        pass

            # Both roles can record sales
            if not handled:
                try:
                    parse_input(text, 2)
                    record_sale(db, chat_id, text)
                    handled = True
                except:
                    pass

            if not handled:
                send_message(chat_id, f"⚠️ Invalid input or action not allowed for your role ({role}). Type *menu* to see instructions.")

    elif "callback_query" in data:
        chat_id = data["callback_query"]["message"]["chat"]["id"]
        action = data["callback_query"]["data"]
        user = get_user(chat_id)
        if not user:
            return {"ok": True}
        role = user.role

        # Role selection
        if action == "role_owner":
            user.role = "owner"
            db.commit()
            main_menu(chat_id, role="owner")
        elif action == "role_keeper":
            user.role = "keeper"
            db.commit()
            main_menu(chat_id, role="keeper")

        # Owner-only actions
        elif action == "add_user":
            if role != "owner":
                send_message(chat_id, "❌ Only owners can add users.")
            else:
                keyboard = {"inline_keyboard": [[{"text": "⬅️ Back to Menu", "callback_data": "back_to_menu"}]]}
                send_message(chat_id, "➕ Send new user details as:\n`user_id;name`", keyboard)

        # Products (owner only)
        elif action == "add_product":
            if role != "owner":
                send_message(chat_id, "❌ Only owners can add products.")
            else:
                keyboard = {"inline_keyboard": [[{"text": "⬅️ Back to Menu", "callback_data": "back_to_menu"}]]}
                send_message(chat_id, "➕ Send product details as:\n`name;price;stock`", keyboard)

        elif action == "update_product":
            if role != "owner":
                send_message(chat_id, "❌ Only owners can update products.")
            else:
                keyboard = {"inline_keyboard": [[{"text": "⬅️ Back to Menu", "callback_data": "back_to_menu"}]]}
                send_message(chat_id, "✏️ Send update as:\n`id;new_name;price;stock`", keyboard)

        # Record sale (everyone)
        elif action == "record_sale":
            keyboard = {"inline_keyboard": [[{"text": "⬅️ Back to Menu", "callback_data": "back_to_menu"}]]}
            send_message(chat_id, "🛒 Send sale as:\n`product_name;quantity`", keyboard)

        # View stock (everyone)
        elif action == "view_stock":
            stock_list = get_stock_list(db)
            keyboard = {"inline_keyboard": [[{"text": "⬅️ Back to Menu", "callback_data": "back_to_menu"}]]}
            send_message(chat_id, stock_list, keyboard)

        # Reports with role filtering
        elif action == "reports":
            keyboard_buttons = [
                [{"text": "📅 Daily", "callback_data": "report_daily"}],
                [{"text": "📆 Weekly", "callback_data": "report_weekly"}],
                [{"text": "📊 Monthly", "callback_data": "report_monthly"}],
            ]
            if role == "owner":
                keyboard_buttons.extend([
                    [{"text": "⚠️ Low Stock", "callback_data": "report_low_stock"}],
                    [{"text": "🏆 Top Products", "callback_data": "report_top_products"}],
                    [{"text": "👥 Top Customers", "callback_data": "report_top_customers"}],
                    [{"text": "🔁 Top Repeat Customers", "callback_data": "report_top_repeat_customers"}],
                    [{"text": "💰 Average Order Value", "callback_data": "report_aov"}],
                    [{"text": "📦 Stock Turnover", "callback_data": "report_stock_turnover"}],
                ])
            keyboard_buttons.append([{"text": "⬅️ Back to Menu", "callback_data": "back_to_menu"}])
            keyboard = {"inline_keyboard": keyboard_buttons}
            send_message(chat_id, "📊 Choose report type:", keyboard)

        elif action in [
            "report_daily", "report_weekly", "report_monthly",
            "report_low_stock", "report_top_products", "report_top_customers",
            "report_top_repeat_customers", "report_aov", "report_stock_turnover"
        ]:
            if role != "owner" and action not in ["report_daily", "report_weekly", "report_monthly"]:
                send_message(chat_id, "❌ Only owners can access this report.")
            else:
                report_text = generate_report(db, action)
                keyboard = {"inline_keyboard": [[{"text": "⬅️ Back to Menu", "callback_data": "back_to_menu"}]]}
                send_message(chat_id, report_text, keyboard)

        # Help
        elif action == "help":
            keyboard = {"inline_keyboard": [[{"text": "⬅️ Back to Menu", "callback_data": "back_to_menu"}]]}
            send_message(chat_id, help_text(), keyboard)

        # Back button
        elif action == "back_to_menu":
            main_menu(chat_id, role)

    return {"ok": True}
