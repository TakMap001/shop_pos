import multiprocessing
try:
    multiprocessing.set_start_method("fork")
except RuntimeError:
    # Already set
    pass

from fastapi import FastAPI
from app.routes import products, views, sales, reports, whatsapp  # import routes here
import uvicorn
import os

app = FastAPI(title="POS Backend API")

# Include routers
app.include_router(products.router)
app.include_router(views.router)
app.include_router(sales.router)
app.include_router(reports.router)
app.include_router(whatsapp.router)

# Root
@app.get("/")
def root():
    return {"message": "WhatsApp POS Backend Running!"}

# ---- Railway deployment: use PORT ----
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))  # Use Railway's PORT variable
    uvicorn.run("main:app", host="0.0.0.0", port=port)
