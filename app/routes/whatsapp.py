import os
from twilio.rest import Client
from fastapi import APIRouter, Request
from fastapi.responses import PlainTextResponse

router = APIRouter()

# Load Twilio credentials from environment variables
account_sid = os.getenv("TWILIO_ACCOUNT_SID")
auth_token = os.getenv("TWILIO_AUTH_TOKEN")
twilio_whatsapp_number = os.getenv("TWILIO_WHATSAPP_NUMBER")

print("🔍 Debug: Loaded Environment Variables")
print(f"  TWILIO_ACCOUNT_SID: {account_sid}")
print(f"  TWILIO_AUTH_TOKEN: {'SET' if auth_token else 'MISSING'}")
print(f"  TWILIO_WHATSAPP_NUMBER: {twilio_whatsapp_number}")

# Initialize Twilio client
client = None
try:
    client = Client(account_sid, auth_token)
    print("✅ Debug: Twilio Client initialized successfully")
except Exception as e:
    print("❌ Debug: Failed to initialize Twilio Client")
    print(e)


@router.post("/whatsapp")
async def whatsapp_webhook(request: Request):
    print("📩 Debug: Incoming webhook received")

    try:
        form_data = await request.form()
        print("📋 Debug: Parsed request form data:", form_data)

        from_number = form_data.get("From")
        body = form_data.get("Body")

        print(f"📞 Debug: From={from_number}, Body={body}")

        if not from_number or not body:
            print("⚠️ Debug: Missing required fields (From/Body)")
            return PlainTextResponse("Invalid request", status_code=400)

        # Send automated reply via Twilio
        try:
            print("📤 Debug: Attempting to send WhatsApp reply...")
            message = client.messages.create(
                from_=f"whatsapp:{twilio_whatsapp_number}",
                body=f"Hello 👋! You said: {body}",
                to=from_number,
            )
            print(f"✅ Debug: Message sent successfully. SID={message.sid}")
        except Exception as e:
            print("❌ Debug: Failed to send WhatsApp message")
            print(e)
            return PlainTextResponse("Error sending message", status_code=500)

        return PlainTextResponse("Message processed")

    except Exception as e:
        print("❌ Debug: Exception inside whatsapp_webhook")
        print(e)
        return PlainTextResponse("Error processing request", status_code=500)
