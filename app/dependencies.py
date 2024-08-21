from sqlalchemy.orm import Session
from fastapi import Depends, HTTPException
from database import SessionLocal


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
