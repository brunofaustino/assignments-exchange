from sqlalchemy import Column, Integer, Numeric, String, Date, ForeignKey
from sqlalchemy.orm import relationship
from database import Base

class CurrencyConversion(Base):
    __tablename__ = 'currency_conversion_f'

    conversion_date = Column(Date, primary_key=True, nullable=False)
    source_currency_key = Column(Integer, ForeignKey('currency_d.currency_key'), primary_key=True, nullable=False)
    destination_currency_key = Column(Integer, ForeignKey('currency_d.currency_key'), primary_key=True, nullable=False)
    source_destination_exchrate = Column(Numeric, nullable=True)
    destination_source_exchrate = Column(Numeric, nullable=True)
    source_destination_month_avg = Column(Numeric, nullable=True)
    destination_source_month_avg = Column(Numeric, nullable=True)
    source_destination_year_avg = Column(Numeric, nullable=True)
    destination_source_year_avg = Column(Numeric, nullable=True)
    exchgrates_source = Column(String(255), nullable=True)

    # Relacionamentos
    source_currency = relationship("Currency", foreign_keys=[source_currency_key])
    destination_currency = relationship("Currency", foreign_keys=[destination_currency_key])


class Currency(Base):
    __tablename__ = 'currency_d'

    currency_key = Column(Integer, primary_key=True, nullable=False)
    currency_code = Column(String(3), unique=True, nullable=False)
    currency_name = Column(String(200), nullable=True)
    currency_symbol = Column(String(4), nullable=True)

    # Relacionamentos
    source_conversions = relationship("CurrencyConversion", foreign_keys=[CurrencyConversion.source_currency_key],
                                      back_populates="source_currency")
    destination_conversions = relationship("CurrencyConversion",
                                           foreign_keys=[CurrencyConversion.destination_currency_key],
                                           back_populates="destination_currency")
