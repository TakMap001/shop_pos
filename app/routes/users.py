from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.database import get_db
from app.models.central_models import User           # SQLAlchemy model
from app.schemas.schemas import User as UserSchema   # Pydantic schema

router = APIRouter()

@router.get("/", response_model=list[UserSchema])
def get_all_users(db: Session = Depends(get_db)):
    users = db.query(User).all()
    return users
