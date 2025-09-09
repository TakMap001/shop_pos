from fastapi import FastAPI
from app.routes import products, views, sales, reports, users, whatsapp, telegram
import uvicorn
import os

# ✅ Import central DB init function
from app.tenants import create_central_db

app = FastAPI(title="POS Backend API")

# -------------------- Initialize Central DB Tables --------------------
@app.on_event("startup")
def startup_event():
    create_central_db()  # creates central tables (Tenant)
    print("✅ Central database initialized successfully.")

# -------------------- Include Routers --------------------
app.include_router(products.router)
app.include_router(views.router)
app.include_router(sales.router)
app.include_router(reports.router)
# app.include_router(whatsapp.router)
app.include_router(telegram.router)  # Telegram router
app.include_router(users.router, prefix="/users", tags=["users"])

# -------------------- Root Endpoint --------------------
@app.get("/")
def root():
    return {"message": "POS Backend Running with Telegram!"}

# ---- Railway / Local deployment ----
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))  # Railway provides PORT
    uvicorn.run("app.main:app", host="0.0.0.0", port=port, reload=False)
