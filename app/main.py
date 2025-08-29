import multiprocessing
try:
    multiprocessing.set_start_method("fork")
except RuntimeError:
    # Already set
    pass
from fastapi import FastAPI
from app.routes import products, views, sales, reports, whatsapp  # import routes here

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

