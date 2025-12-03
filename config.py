# config.py - CORRECTED VERSION
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
    print("⚠️ Using local development database configuration")

# REMOVE THESE LINES - THEY'RE CAUSING THE PROBLEM!
# RAILWAY_DATABASE_URL = "postgresql://postgres:unAwubjufYzxonUSWZjdNvbWtuhwikQs@postgres.railway.internal:5432/railway"
# DATABASE_URL = DATABASE_URL or RAILWAY_DATABASE_URL  # ← DELETE THIS LINE!

# Debug output
print(f"🔧 Final DATABASE_URL: {DATABASE_URL[:60]}..." if DATABASE_URL and len(DATABASE_URL) > 60 else f"🔧 Final DATABASE_URL: {DATABASE_URL}")

# --- Twilio ---
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
TWILIO_WHATSAPP_NUMBER = os.getenv("TWILIO_WHATSAPP_NUMBER")
twilio_client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN) if TWILIO_ACCOUNT_SID else None

# --- Telegram ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}" if TELEGRAM_BOT_TOKEN else None

# --- FastAPI ---
FASTAPI_SECRET_KEY = os.getenv("FASTAPI_SECRET_KEY", "supersecret")

# --- Redis ---
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# --- Tenant DB Base URL (used for schema-based tenants) ---
if DATABASE_URL:
    BASE_TENANT_URL = DATABASE_URL.rsplit("/", 1)[0]
else:
    BASE_TENANT_URL = None
    print("⚠️ WARNING: No DATABASE_URL, BASE_TENANT_URL is None")