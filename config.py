# config.py
import os
from dotenv import load_dotenv
from twilio.rest import Client

# Load local .env file if running locally
load_dotenv()

# ======================================================
# 🔹 SAFE CONFIGURATION (No hardcoded secrets)
# ======================================================

# --- Telegram ---
# Get from environment, with fallback for Railway issues
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "7266895714:AAGDz-vMwBGp8AGRgkB4FDF3Dl2Apw0r91s")
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}" if TELEGRAM_BOT_TOKEN else None

# --- Database ---
# Use environment variable with Railway fallback
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:unAwubjufYzxonUSWZjdNvbWtuhwikQs@postgres.railway.internal:5432/railway")

# --- Twilio ---
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN") 
TWILIO_WHATSAPP_NUMBER = os.getenv("TWILIO_WHATSAPP_NUMBER")

# Initialize Twilio client only if credentials exist
if TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN:
    twilio_client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
    print("✅ Twilio client initialized")
else:
    twilio_client = None
    print("⚠️ Twilio credentials not set - WhatsApp features disabled")

# --- FastAPI ---
FASTAPI_SECRET_KEY = os.getenv("FASTAPI_SECRET_KEY", "supersecret")

# --- Redis ---  
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# --- Tenant DB Base URL ---
BASE_TENANT_URL = DATABASE_URL.rsplit("/", 1)[0]

print("✅ Configuration loaded")