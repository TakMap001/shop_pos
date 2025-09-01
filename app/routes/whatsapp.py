import os
from twilio.rest import Client
from fastapi import APIRouter, Request
from fastapi.responses import PlainTextResponse

router = APIRouter()

# --- Load Twilio credentials ---
account_sid = os.getenv("TWILIO_ACCOUNT_SID")
auth_token = os.getenv("TWILIO_AUTH_TOKEN")
TWILIO_WHATSAPP_NUMBER = os.getenv("TWILIO_WHATSAPP_NUMBER")

print("🔍 Debug: Loaded Environment Variables")
print(f"  TWILIO_ACCOUNT_SID: {account_sid}")
print(f"  TWILIO_AUTH_TOKEN: {'SET' if auth_token else 'MISSING'}")
print(f"  TWILIO_WHATSAPP_NUMBER: {TWILIO_WHATSAPP_NUMBER}")

# --- Initialize Twilio client ---
client = None
try:
    client = Client(account_sid, auth_token)
    print("✅ Debug: Twilio Client initialized successfully")
except Exception as e:
    print("❌ Debug: Failed to initialize Twilio Client")
    print(e)


@router.post("/whatsapp/webhook")
async def whatsapp_webhook(request: Request):
    print("📩 Debug: Incoming webhook received")

    try:
        form_data = await request.form()
        print("📋 Debug: Parsed request form data:", form_data)

        # --- For Twilio sandbox trial, always send to your verified number ---
        to_number = "whatsapp:+263719982845"  # your WhatsApp number
        body = form_data.get("Body")           # message content from sender

        print(f"📞 Debug: Sending to={to_number}, Body={body}")

        if not body:
            print("⚠️ Debug: Missing Body in form data")
            return PlainTextResponse("Invalid request: missing Body", status_code=400)

        # --- Send WhatsApp reply ---
        try:
            print("📤 Debug: Attempting to send WhatsApp reply...")
            message = client.messages.create(
                from_=TWILIO_WHATSAPP_NUMBER,
                to=to_number,
                body=f"Hello 👋! You said: {body}"
            )
            print(f"✅ Debug: Message sent successfully. SID={message.sid}")
        except Exception as e:
            print("❌ Debug: Failed to send WhatsApp message")
            print(e)
            return PlainTextResponse("Error sending message", status_code=500)

        return PlainTextResponse("Message processed successfully")

    except Exception as e:
        print("❌ Debug: Exception inside whatsapp_webhook")
        print(e)
        return PlainTextResponse("Error processing request", status_code=500)

