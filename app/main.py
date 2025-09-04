from fastapi import FastAPI
from app.routes import products, views, sales, reports, whatsapp, telegram  # 👈 added telegram
import uvicorn
import os

app = FastAPI(title="POS Backend API")

# Include routers
app.include_router(products.router)
app.include_router(views.router)
app.include_router(sales.router)
app.include_router(reports.router)
#app.include_router(whatsapp.router)
app.include_router(telegram.router)  # 👈 new Telegram router

# Root
@app.get("/")
def root():
    return {"message": "POS Backend Running with Telegram!"}

# ---- Railway deployment ----
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))  # Railway provides PORT
    uvicorn.run("app.main:app", host="0.0.0.0", port=port, reload=False)
