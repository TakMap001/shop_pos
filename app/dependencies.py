# app/dependencies.py
from fastapi import Depends, HTTPException
from app.tenant_db import get_session_for_tenant  # updated import
from app.models.central_models import Tenant
from app.core import SessionLocal as CentralSessionLocal  # central DB session


def get_tenant_db(telegram_id: int):
    """FastAPI dependency: yield a tenant DB session based on Telegram owner ID."""
    central_db = CentralSessionLocal()
    try:
        tenant = central_db.query(Tenant).filter_by(telegram_owner_id=telegram_id).first()
        if not tenant:
            raise HTTPException(status_code=404, detail="Tenant not found")
    finally:
        central_db.close()

    # Use tenant_db_url to get a session
    tenant_sessionmaker = get_session_for_tenant(tenant.database_url)
    tenant_db = tenant_sessionmaker()
    try:
        yield tenant_db
    finally:
        tenant_db.close()
