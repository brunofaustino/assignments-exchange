from fastapi import FastAPI
from routers import exchange_rates

app = FastAPI()

app.include_router(exchange_rates.router)
