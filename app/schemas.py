from pydantic import BaseModel
from datetime import date
from typing import Optional
class ExchangeRateCreate(BaseModel):
    conversion_date: date
    source_currency_key: int
    destination_currency_key: int
    source_destination_exchrate: Optional[float] = None
    destination_source_exchrate: float = None
    source_destination_month_avg: Optional[float] = None
    destination_source_month_avg: Optional[float] = None
    source_destination_year_avg: Optional[float] = None
    destination_source_year_avg: Optional[float] = None
    exchgrates_source: str = 'EXTERNAL_API'

class ExchangeRateResponse(BaseModel):
    conversion_date: date
    source_currency_key: int
    destination_currency_key: int
    source_destination_exchrate: Optional[float] = None
    destination_source_exchrate: Optional[float] = None
    source_destination_month_avg: Optional[float] = None
    destination_source_month_avg: Optional[float] = None
    source_destination_year_avg: Optional[float] = None
    destination_source_year_avg: Optional[float] = None
    exchgrates_source: Optional[str] = None