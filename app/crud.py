from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from fastapi import HTTPException
from datetime import date
import models, schemas

def create_exchange_rate(db: Session, rate: schemas.ExchangeRateCreate):
    db_rate = models.CurrencyConversion(**rate.dict())

    # Calcula a taxa inversa se a taxa direta foi fornecida
    if db_rate.source_destination_exchrate:
        db_rate.destination_source_exchrate = 1.0 / db_rate.source_destination_exchrate

    try:
        db.add(db_rate)
        db.commit()
        db.refresh(db_rate)
        return db_rate
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=400, detail=f"Currency conversion already exists "
                                                    f"for the given date and currencies. db_rate='{db_rate.conversion_date}'"
                                                    f"source_currency_key='{db_rate.source_currency_key}',"
                                                    f"destination_currency_key='{db_rate.destination_currency_key}'")


def get_exchange_rates(db: Session, conversion_date: date, source_currency_key: int, destination_currency_key: int):
    rates = db.query(models.CurrencyConversion).filter(
        models.CurrencyConversion.conversion_date == conversion_date,
        models.CurrencyConversion.source_currency_key == source_currency_key,
        models.CurrencyConversion.destination_currency_key == destination_currency_key
    ).all()

    if not rates:
        raise HTTPException(status_code=404,
            detail=f"No exchange rates found matching the criteria. "
                   f"conversion_date='{conversion_date}', "
                   f"source_currency_key='{source_currency_key}', "
                   f"destination_currency_key='{destination_currency_key}'"
        )

    return rates
