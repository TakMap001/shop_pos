# app/dependencies.py
from fastapi import Depends
from app.tenants import get_session_for_tenant
from app.models.central_models import Tenant
from app.database import CentralSessionLocal
from fastapi import HTTPException

def get_tenant_db(telegram_id: int):
    central_db = CentralSessionLocal()
    tenant = central_db.query(Tenant).filter_by(telegram_owner_id=telegram_id).first()

    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")

    SessionLocal = get_session_for_tenant(tenant.database_url)
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

