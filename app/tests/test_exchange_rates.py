import pytest
from fastapi.testclient import TestClient
from unittest.mock import MagicMock
from sqlalchemy.orm import Session

from app.main import app
from app.database import get_db
from app.crud import create_exchange_rate, get_exchange_rates
from app.schemas import ExchangeRateCreate, ExchangeRateResponse

client = TestClient(app)

mock_db = MagicMock(spec=Session)

@pytest.fixture(autouse=True)
def reset_mock():
    mock_db.reset_mock()

@pytest.fixture(scope="module")
def override_get_db():
    def _get_db():
        yield mock_db
    app.dependency_overrides[get_db] = _get_db

def test_create_exchange_rate(override_get_db):
    mock_exchange_rate = {
        "conversion_date": "1999-01-01",
        "source_currency_key": 27,
        "destination_currency_key": 32,
        "source_destination_exchrate": 1.25,
        "exchgrates_source": "Test API"
    }

    response = client.post("/exchange_rates/", json=mock_exchange_rate)

    print(response.json())

    assert response.status_code == 200
    data = response.json()
    assert data["conversion_date"] == mock_exchange_rate["conversion_date"]
    assert data["source_currency_key"] == mock_exchange_rate["source_currency_key"]
    assert data["destination_currency_key"] == mock_exchange_rate["destination_currency_key"]
    assert data["source_destination_exchrate"] == mock_exchange_rate["source_destination_exchrate"]
    assert data["exchgrates_source"] == mock_exchange_rate["exchgrates_source"]

def test_get_exchange_rates(override_get_db):
    mock_exchange_rate = {
        "conversion_date": "c",
        "source_currency_key": 27,
        "destination_currency_key": 72
    }

    mock_db.query.return_value.filter.return_value.all.return_value = [mock_exchange_rate]

    params = {
        "conversion_date": "2019-12-05",
        "source_currency_key": 26,
        "destination_currency_key": 72,
    }
    response = client.get("/exchange_rates/", params=params)

    print(response.json())

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["conversion_date"] == params["conversion_date"]
    assert data[0]["source_currency_key"] == params["source_currency_key"]
    assert data[0]["destination_currency_key"] == params["destination_currency_key"]

def test_create_exchange_rate_conflict(override_get_db):
    mock_exchange_rate = {
        "conversion_date": "2024-08-21",
        "source_currency_key": 27,
        "destination_currency_key": 32,
        "source_destination_exchrate": 1.25,
        "exchgrates_source": "Test API"
    }

    mock_db.query.return_value.filter.return_value.first.return_value = mock_exchange_rate

    response = client.post("/exchange_rates/", json=mock_exchange_rate)

    assert response.status_code == 400
    assert response.json()["detail"] == "Currency conversion already exists"

def test_get_nonexistent_exchange_rate(override_get_db):
    mock_db.query.return_value.filter.return_value.all.return_value = []

    params = {
        "conversion_date": "2024-08-22",
        "source_currency_key": 1,
        "destination_currency_key": 3,
    }
    response = client.get("/exchange_rates/", params=params)

    assert response.status_code == 404
    assert response.json()["detail"] == "No exchange rates found matching the criteria"
