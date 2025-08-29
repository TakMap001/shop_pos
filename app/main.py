from fastapi import FastAPI
from app.routes import products, views, sales, reports  # import routes here

app = FastAPI(title="POS Backend API")

# Include routers
app.include_router(products.router)
app.include_router(views.router)
app.include_router(sales.router)
app.include_router(reports.router)

# Root
@app.get("/")
def root():
    return {"message": "WhatsApp POS Backend Running!"}

