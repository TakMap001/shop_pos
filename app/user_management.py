app/user_management.py
"""
Multi-level user management for shops
Handles: owner, admin, and shopkeeper users
"""
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, text
import os
from app.models.central_models import User
from app.models.models import ShopORM
from typing import Dict, Optional, List
import logging
import random
import string
import bcrypt
import time

logger = logging.getLogger(__name__)


def hash_password(password: str) -> str:
    """Hash a password using bcrypt"""
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(password.encode('utf-8'), salt)
    return hashed.decode('utf-8')


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a password against its hash"""
    try:
        return bcrypt.checkpw(plain_password.encode('utf-8'), hashed_password.encode('utf-8'))
    except Exception:
        return False


def generate_username(prefix: str, shop_name: str, existing_count: int = 0) -> str:
    """Generate username like admin_main_store_001, shopkeeper_main_store_001"""
    # Clean shop name for username
    clean_name = shop_name.lower().replace(" ", "_").replace("-", "_")
    clean_name = ''.join(c for c in clean_name if c.isalnum() or c == '_')
    
    # Add timestamp for uniqueness
    timestamp = int(time.time()) % 10000
    
    if existing_count > 0:
        return f"{prefix}_{clean_name}_{existing_count:03d}"
    else:
        return f"{prefix}_{clean_name}_{timestamp:04d}"


def generate_password(length: int = 10) -> str:
    """Generate strong random password"""
    # Use only alphanumeric for simplicity in Telegram
    chars = string.ascii_letters + string.digits
    return ''.join(random.choice(chars) for _ in range(length))


def create_default_users(db: Session, tenant_db: Session, owner_user: User) -> Dict:
    """
    Create default admin and shopkeeper users for a new tenant
    Returns: dict with credentials for both users (including passwords)
    """
    try:
        # Get main shop (or create default if none exists)
        main_shop = tenant_db.query(ShopORM).filter(ShopORM.is_main == True).first()
        if not main_shop:
            # Get first shop or create default
            main_shop = tenant_db.query(ShopORM).first()
            if not main_shop:
                main_shop = ShopORM(
                    name="Main Store",
                    location="Headquarters",
                    is_main=True
                )
                tenant_db.add(main_shop)
                tenant_db.commit()
                tenant_db.refresh(main_shop)
        
        logger.info(f"📝 Creating default users for shop: {main_shop.name}")
        
        credentials = {}
        
        # Create admin user (skip if already exists)
        existing_admin = db.query(User).filter(
            User.tenant_schema == owner_user.tenant_schema,
            User.role == "admin",
            User.shop_id == main_shop.shop_id
        ).first()
        
        if not existing_admin:
            admin_username = generate_username("admin", main_shop.name, 1)
            admin_password = generate_password()
            
            # Make sure username is unique
            counter = 1
            while db.query(User).filter(User.username == admin_username).first():
                admin_username = generate_username("admin", main_shop.name, counter)
                counter += 1
            
            admin_user = User(
                name=f"Admin - {main_shop.name}",
                username=admin_username,
                email=None,
                password_hash=hash_password(admin_password),
                chat_id=None,
                role="admin",
                shop_id=main_shop.shop_id,
                tenant_schema=owner_user.tenant_schema
            )
            db.add(admin_user)
            
            credentials["admin"] = {
                "username": admin_username,
                "password": admin_password,  # ✅ Store actual password
                "role": "admin",
                "shop_id": main_shop.shop_id,
                "shop_name": main_shop.name
            }
            logger.info(f"✅ Created admin user: {admin_username}")
        else:
            logger.info(f"ℹ️ Admin user already exists: {existing_admin.username}")
            # Still return admin info (without password)
            credentials["admin"] = {
                "username": existing_admin.username,
                "password": "[Already exists - ask owner for password]",
                "role": existing_admin.role,
                "shop_id": main_shop.shop_id,
                "shop_name": main_shop.name
            }
        
        # Create shopkeeper user (skip if already exists)
        existing_shopkeeper = db.query(User).filter(
            User.tenant_schema == owner_user.tenant_schema,
            User.role == "shopkeeper",
            User.shop_id == main_shop.shop_id
        ).first()
        
        if not existing_shopkeeper:
            shopkeeper_username = generate_username("sk", main_shop.name, 1)
            shopkeeper_password = generate_password()
            
            # Make sure username is unique
            counter = 1
            while db.query(User).filter(User.username == shopkeeper_username).first():
                shopkeeper_username = generate_username("sk", main_shop.name, counter)
                counter += 1
            
            shopkeeper_user = User(
                name=f"Shopkeeper - {main_shop.name}",
                username=shopkeeper_username,
                email=None,
                password_hash=hash_password(shopkeeper_password),
                chat_id=None,
                role="shopkeeper",
                shop_id=main_shop.shop_id,
                tenant_schema=owner_user.tenant_schema
            )
            db.add(shopkeeper_user)
            
            credentials["shopkeeper"] = {
                "username": shopkeeper_username,
                "password": shopkeeper_password,  # ✅ Store actual password
                "role": "shopkeeper",
                "shop_id": main_shop.shop_id,
                "shop_name": main_shop.name
            }
            logger.info(f"✅ Created shopkeeper user: {shopkeeper_username}")
        else:
            logger.info(f"ℹ️ Shopkeeper user already exists: {existing_shopkeeper.username}")
            # Still return shopkeeper info (without password)
            credentials["shopkeeper"] = {
                "username": existing_shopkeeper.username,
                "password": "[Already exists - ask owner for password]",
                "role": existing_shopkeeper.role,
                "shop_id": main_shop.shop_id,
                "shop_name": main_shop.name
            }
        
        db.commit()
        
        if credentials:
            logger.info(f"✅ Created/retrieved {len(credentials)} users for tenant {owner_user.tenant_schema}")
        else:
            logger.info(f"ℹ️ No users created for tenant {owner_user.tenant_schema}")
        
        return credentials
        
    except Exception as e:
        logger.error(f"❌ Failed to create default users: {e}")
        db.rollback()
        return {}
        

def create_custom_user(db: Session, tenant_schema: str, shop_id: int, role: str, custom_name: str = None) -> Optional[Dict]:
    """
    Create a custom user with specific role
    Returns: user dict with credentials or None if failed
    """
    try:
        if role not in ["admin", "shopkeeper"]:
            logger.error(f"❌ Invalid role: {role}")
            return None
        
        # Get shop info from tenant DB
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            logger.error("❌ DATABASE_URL is missing")
            return None
            
        engine = create_engine(database_url)
        
        with engine.connect() as conn:
            conn.execute(text(f"SET search_path TO {tenant_schema},public"))
            result = conn.execute(
                text("SELECT name FROM shops WHERE shop_id = :sid"),
                {"sid": shop_id}
            ).fetchone()
            
            if not result:
                logger.error(f"❌ Shop {shop_id} not found in schema {tenant_schema}")
                return None
            
            shop_name = result[0]
        
        # Count existing users for this shop with same role
        existing_count = db.query(User).filter(
            User.tenant_schema == tenant_schema,
            User.shop_id == shop_id,
            User.role == role
        ).count()
        
        # Generate username
        if role == "admin":
            prefix = "admin"
        else:  # shopkeeper
            prefix = "sk"
        
        username = generate_username(prefix, shop_name, existing_count + 1)
        
        # Check if username exists
        counter = 1
        while db.query(User).filter(User.username == username).first():
            username = generate_username(prefix, shop_name, existing_count + counter)
            counter += 1
        
        password = generate_password()
        
        # Create user
        display_name = custom_name or f"{role.title()} - {shop_name}"
        
        user = User(
            name=display_name,
            username=username,
            email=None,
            password_hash=hash_password(password),
            chat_id=None,
            role=role,
            shop_id=shop_id,
            tenant_schema=tenant_schema
        )
        
        db.add(user)
        db.commit()
        db.refresh(user)
        
        logger.info(f"✅ Created {role} user {username} for shop {shop_name}")
        
        return {
            "user_id": user.user_id,
            "username": username,
            "password": password,
            "role": role,
            "shop_id": shop_id,
            "shop_name": shop_name,
            "display_name": display_name
        }
        
    except Exception as e:
        logger.error(f"❌ Failed to create custom user: {e}")
        return None


def get_users_for_shop(db: Session, tenant_schema: str, shop_id: int = None) -> List[Dict]:
    """
    Get all users for a tenant (optionally filtered by shop)
    Excludes primary owner users
    """
    try:
        query = db.query(User).filter(
            User.tenant_schema == tenant_schema,
            User.role.in_(["admin", "shopkeeper"])  # Exclude primary owner
        )
        
        if shop_id:
            query = query.filter(User.shop_id == shop_id)
        
        users = query.order_by(User.role, User.username).all()
        
        result = []
        for user in users:
            result.append({
                "user_id": user.user_id,
                "username": user.username,
                "name": user.name,
                "role": user.role,
                "shop_id": user.shop_id,
                "created_at": user.created_at,
                "has_telegram": user.chat_id is not None
            })
        
        return result
        
    except Exception as e:
        logger.error(f"❌ Failed to get users: {e}")
        return []


def delete_user(db: Session, username: str) -> bool:
    """Delete a user account (cannot delete primary owner)"""
    try:
        user = db.query(User).filter(
            User.username == username,
            User.role.in_(["admin", "shopkeeper"])  # Can't delete primary owner
        ).first()
        
        if not user:
            logger.warning(f"⚠️ User {username} not found or cannot be deleted")
            return False
        
        db.delete(user)
        db.commit()
        
        logger.info(f"✅ Deleted user {username}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to delete user: {e}")
        db.rollback()
        return False


def reset_user_password(db: Session, username: str) -> Optional[str]:
    """Reset password for a user"""
    try:
        user = db.query(User).filter(User.username == username).first()
        
        if not user:
            logger.warning(f"⚠️ User {username} not found")
            return None
        
        new_password = generate_password()
        user.password_hash = hash_password(new_password)
        db.commit()
        
        logger.info(f"✅ Reset password for user {username}")
        return new_password
        
    except Exception as e:
        logger.error(f"❌ Failed to reset password: {e}")
        db.rollback()
        return None


def update_user_role(db: Session, username: str, new_role: str) -> bool:
    """Update a user's role (e.g., promote shopkeeper to admin)"""
    try:
        if new_role not in ["admin", "shopkeeper"]:
            logger.error(f"❌ Invalid role: {new_role}")
            return False
        
        user = db.query(User).filter(
            User.username == username,
            User.role.in_(["admin", "shopkeeper"])
        ).first()
        
        if not user:
            logger.warning(f"⚠️ User {username} not found")
            return False
        
        user.role = new_role
        db.commit()
        
        logger.info(f"✅ Updated user {username} role to {new_role}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to update role: {e}")
        db.rollback()
        return False


def get_role_based_menu(role: str):
    """
    Get menu based on user role
    Returns: inline keyboard markup
    """
    if role == "owner":
        return {
            "inline_keyboard": [
                [{"text": "➕ Add Product", "callback_data": "add_product"}],
                [{"text": "✏️ Update Product", "callback_data": "update_product"}],
                [{"text": "📈 Quick Stock Update", "callback_data": "quick_stock_update"}],
                [{"text": "📦 View Stock", "callback_data": "view_stock"}],
                [{"text": "💰 Record Sale", "callback_data": "record_sale"}],
                [{"text": "📊 Reports", "callback_data": "report_menu"}],
                [{"text": "🏪 Manage Shops", "callback_data": "manage_shops"}],  # Owner only
                [{"text": "👥 Manage Users", "callback_data": "manage_users"}],  # Owner user management
                [{"text": "❓ Help", "callback_data": "help"}]
            ]
        }
    
    elif role == "admin":
        return {
            "inline_keyboard": [
                [{"text": "➕ Add Product", "callback_data": "add_product"}],
                [{"text": "✏️ Update Product", "callback_data": "update_product"}],
                [{"text": "📈 Quick Stock Update", "callback_data": "quick_stock_update"}],
                [{"text": "📦 View Stock", "callback_data": "view_stock"}],
                [{"text": "💰 Record Sale", "callback_data": "record_sale"}],
                [{"text": "📊 Reports", "callback_data": "report_menu"}],
                # ❌ NO "Manage Shops" for admin
                [{"text": "👥 Manage Users", "callback_data": "manage_users_admin"}],  # ✅ Admin-specific user management
                [{"text": "❓ Help", "callback_data": "help"}]
            ]
        }
    
    else:  # shopkeeper
        return {
            "inline_keyboard": [
                [{"text": "💰 Record Sale", "callback_data": "record_sale"}],
                [{"text": "📦 View Stock", "callback_data": "view_stock"}],
                [{"text": "📊 Reports", "callback_data": "report_menu"}],  # ✅ Changed to report_menu for consistency
                [{"text": "❓ Help", "callback_data": "help"}],
                [{"text": "🚪 Logout", "callback_data": "logout"}]
            ]
        }


def is_user_allowed_for_action(user: User, action: str) -> bool:
    """
    Check if user is allowed to perform an action based on role
    """
    # Actions allowed for everyone
    common_actions = ["record_sale", "view_stock", "report_daily", "help", "logout"]
    
    if action in common_actions:
        return True
    
    # Admin/Owner only actions
    admin_actions = [
        "add_product", "update_product", "quick_stock_update", 
        "report_menu", "manage_shops", "manage_users"
    ]
    
    if user.role in ["owner", "admin"] and action in admin_actions:
        return True
    
    return False


def get_user_by_username(db: Session, username: str) -> Optional[User]:
    """Get user by username"""
    try:
        return db.query(User).filter(User.username == username).first()
    except Exception as e:
        logger.error(f"❌ Failed to get user {username}: {e}")
        return None


def get_user_by_chat_id(db: Session, chat_id: int) -> Optional[User]:
    """Get user by Telegram chat ID"""
    try:
        return db.query(User).filter(User.chat_id == chat_id).first()
    except Exception as e:
        logger.error(f"❌ Failed to get user by chat_id {chat_id}: {e}")
        return None


def link_telegram_account(db: Session, username: str, chat_id: int) -> bool:
    """Link Telegram chat_id to a user account"""
    try:
        user = db.query(User).filter(User.username == username).first()
        if not user:
            logger.warning(f"⚠️ User {username} not found")
            return False
        
        # Check if chat_id is already linked to another account
        existing = db.query(User).filter(User.chat_id == chat_id).first()
        if existing and existing.user_id != user.user_id:
            logger.warning(f"⚠️ Chat ID {chat_id} already linked to user {existing.username}")
            return False
        
        user.chat_id = chat_id
        db.commit()
        
        logger.info(f"✅ Linked Telegram chat_id {chat_id} to user {username}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to link Telegram account: {e}")
        db.rollback()
        return False


def unlink_telegram_account(db: Session, username: str) -> bool:
    """Unlink Telegram chat_id from a user account"""
    try:
        user = db.query(User).filter(User.username == username).first()
        if not user:
            logger.warning(f"⚠️ User {username} not found")
            return False
        
        user.chat_id = None
        db.commit()
        
        logger.info(f"✅ Unlinked Telegram from user {username}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to unlink Telegram account: {e}")
        db.rollback()
        return False


def format_user_credentials_message(credentials: Dict) -> str:
    """Format user credentials into a nice Telegram message"""
    if not credentials:
        return "❌ No credentials provided"
    
    role_display = {
        "admin": "👑 Admin User (Full Access)",
        "shopkeeper": "👤 Shopkeeper User (Limited Access)",
        "owner": "👑 Owner User"
    }
    
    message = "✅ *User Credentials Created*\n\n"
    
    for role, creds in credentials.items():
        if role in role_display:
            message += f"**{role_display[role]}:**\n"
            message += f"• **Username:** `{creds['username']}`\n"
            message += f"• **Password:** `{creds['password']}`\n"
            message += f"• **Shop:** {creds['shop_name']}\n\n"
    
    message += "📝 **Instructions:**\n"
    message += "1. Share credentials with the user\n"
    message += "2. They use /start → 'I'm a Shop User'\n"
    message += "3. Enter username and password\n\n"
    message += "⚠️ **Save these credentials!**"
    
    return message