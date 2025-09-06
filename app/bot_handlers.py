# app/bot_handlers.py
import uuid
from app.models.central_models import Tenant, Base as CentralBase
from app.tenants import create_tenant_db, get_session_for_tenant
from app.database import CentralSessionLocal  # your central DB session
from app.bot import bot  # your existing telebot instance

@bot.message_handler(commands=['start'])
def start(message):
    telegram_id = message.from_user.id
    name = message.from_user.full_name
    
    central_db = CentralSessionLocal()
    tenant = central_db.query(Tenant).filter_by(telegram_owner_id=telegram_id).first()
    
    if tenant:
        bot.send_message(telegram_id, "Welcome back! Your store is ready.")
        return
    
    # Create new tenant
    tenant_id = str(uuid.uuid4())
    tenant_db_name = f"store_{tenant_id}"
    tenant_db_url = f"postgresql://user:pass@host:5432/{tenant_db_name}"

    create_tenant_db(tenant_db_url)

    # Initialize tenant tables
    from app.models.models import Base  # your existing models
    engine = get_engine_for_tenant(tenant_db_url)
    Base.metadata.create_all(bind=engine)
    
    # Register tenant in central DB
    new_tenant = Tenant(
        tenant_id=tenant_id,
        store_name=f"{name}'s Store",
        telegram_owner_id=telegram_id,
        database_url=tenant_db_url
    )
    central_db.add(new_tenant)
    central_db.commit()
    
    bot.send_message(telegram_id, f"🎉 Welcome! Your store has been created automatically.")

