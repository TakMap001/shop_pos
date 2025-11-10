# config.py
import os
from dotenv import load_dotenv
from twilio.rest import Client

# Load local .env file if running locally
load_dotenv()

# --- Database ---
DATABASE_URL = os.getenv("DATABASE_URL")

# If not set (local dev fallback), build manually
if not DATABASE_URL:
    DB_USER = os.getenv("DB_USER", "takuramapfumo")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "")
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_NAME = os.getenv("DB_NAME", "shopdb")
    DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Railway's actual connection string (for reference / override)
RAILWAY_DATABASE_URL = "postgresql://postgres:unAwubjufYzxonUSWZjdNvbWtuhwikQs@postgres.railway.internal:5432/railway"

# Prefer Railway injected DATABASE_URL over fallback
DATABASE_URL = DATABASE_URL or RAILWAY_DATABASE_URL

# --- Twilio ---
TWILIO_ACCOUNT_SID = "AC375e64fa3a9d802bf6c3622aad5293ce"
TWILIO_AUTH_TOKEN = "2def4f45b087ffe8754887c9b8"
TWILIO_WHATSAPP_NUMBER = "whatsapp:+14155238886"
twilio_client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN) if TWILIO_ACCOUNT_SID else None

# --- Telegram ---
TELEGRAM_BOT_TOKEN = "7266895714:AAGDz-vMwBGp8AGRgkB4FDF3Dl2Apw0r91s"
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}" if TELEGRAM_BOT_TOKEN else None

# --- FastAPI ---
FASTAPI_SECRET_KEY = os.getenv("FASTAPI_SECRET_KEY", "supersecret")

# --- Redis ---
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# --- Tenant DB Base URL (used for schema-based tenants) ---
BASE_TENANT_URL = DATABASE_URL.rsplit("/", 1)[0]
