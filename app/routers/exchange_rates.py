from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import date

import schemas, crud
from dependencies import get_db

router = APIRouter(
    prefix="/exchange_rates",
    tags=["exchange_rates"],
    responses={404: {"description": "Not found"}},
)

@router.post("/", response_model=schemas.ExchangeRateResponse)
def create_exchange_rate(
        rate: schemas.ExchangeRateCreate,
        db: Session = Depends(get_db)
):
    return crud.create_exchange_rate(db, rate)


@router.get("/", response_model=List[schemas.ExchangeRateResponse])
def get_exchange_rates(
        conversion_date: Optional[date] = None,
        source_currency_key: Optional[int] = None,
        destination_currency_key: Optional[int] = None,
        db: Session = Depends(get_db)
):
    if not conversion_date or not source_currency_key or not destination_currency_key:
        raise HTTPException(
            status_code=400,
            detail="conversion_date, source_currency_key, and destination_currency_key are required and cannot be empty."
        )

    return crud.get_exchange_rates(db, conversion_date, source_currency_key, destination_currency_key)
