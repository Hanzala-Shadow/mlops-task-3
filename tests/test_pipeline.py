"""Validate the data contract, failure handling, and idempotent write strategy."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import patch

import pytest
import requests

from dags.currency_etl.pipeline import CurrencyConfig, extract_rates, load_rates, transform_rates


@pytest.fixture
def raw():
    return {
        "retrieved_at": "2026-11-05T07:15:00+00:00",
        "rates": [
            {"base": "EUR", "quote": "USD", "rate": 1.15, "date": "2026-11-04"},
            {"base": "EUR", "quote": "GBP", "rate": 0.85, "date": "2026-11-04"},
            {"base": "EUR", "quote": "JPY", "rate": 175.2, "date": "2026-11-04"},
        ],
    }


def test_six_field_contract_and_publication_date(raw):
    result = transform_rates(raw, CurrencyConfig())
    assert len(result) == 3
    usd = next(row for row in result if row["target_currency"] == "USD")
    assert usd == {
        "base_currency": "EUR", "target_currency": "USD", "exchange_rate": 1.15,
        "rate_date": "2026-11-04", "source": "Frankfurter API",
        "retrieved_at": "2026-11-05T07:15:00+00:00",
    }


@pytest.mark.parametrize("rate", [0, -1, float("nan"), float("inf"), True, "1.15", None])
def test_invalid_rates_rejected(raw, rate):
    raw["rates"][0]["rate"] = rate
    with pytest.raises(ValueError, match="finite positive"):
        transform_rates(raw, CurrencyConfig())


def test_missing_currency_rejected(raw):
    raw["rates"].pop()
    with pytest.raises(ValueError, match="Missing requested"):
        transform_rates(raw, CurrencyConfig())


def test_duplicate_currency_rejected(raw):
    raw["rates"].append(deepcopy(raw["rates"][0]))
    with pytest.raises(ValueError, match="Duplicate"):
        transform_rates(raw, CurrencyConfig())


@pytest.mark.parametrize("field,value", [("base", "USD"), ("quote", "CAD"), ("date", "tomorrow"), ("date", "2027-01-01")])
def test_invalid_api_rows_rejected(raw, field, value):
    raw["rates"][0][field] = value
    with pytest.raises(ValueError):
        transform_rates(raw, CurrencyConfig())


@pytest.mark.parametrize("payload", [None, {}, {"rates": []}])
def test_missing_xcom_fails_clearly(payload):
    with pytest.raises(ValueError, match="XCom"):
        transform_rates(payload, CurrencyConfig())


def test_naive_retrieval_timestamp_rejected(raw):
    raw["retrieved_at"] = "2026-11-05T07:15:00"
    with pytest.raises(ValueError, match="timestamp"):
        transform_rates(raw, CurrencyConfig())


@pytest.mark.parametrize("targets", [(), ("EUR",), ("USD", "USD"), ("",), ("US",)])
def test_invalid_currency_config_rejected(targets):
    with pytest.raises(ValueError):
        CurrencyConfig(targets=targets)


def test_extraction_requests_v2_and_uses_timeouts(raw):
    with patch("dags.currency_etl.pipeline.requests.Session") as session_type:
        session = session_type.return_value.__enter__.return_value
        session.get.return_value.json.return_value = raw["rates"]
        result = extract_rates(CurrencyConfig())
        assert result["rates"] == raw["rates"]
        assert result["retrieved_at"].endswith("+00:00")
        assert session.get.call_args.kwargs["params"] == {"base": "EUR", "quotes": "USD,GBP,JPY"}
        assert session.get.call_args.kwargs["timeout"] == (10, 30)
        session.get.return_value.raise_for_status.assert_called_once()


def test_http_error_propagates():
    with patch("dags.currency_etl.pipeline.requests.Session") as session_type:
        response = session_type.return_value.__enter__.return_value.get.return_value
        response.raise_for_status.side_effect = requests.HTTPError("upstream unavailable")
        with pytest.raises(requests.HTTPError):
            extract_rates(CurrencyConfig())


def test_load_uses_unique_index_and_upsert(raw, monkeypatch):
    monkeypatch.setenv("MONGODB_URI", "mongodb://test.invalid")
    documents = transform_rates(raw, CurrencyConfig())
    with patch("dags.currency_etl.pipeline.MongoClient") as client_type:
        client = client_type.return_value.__enter__.return_value
        collection = client["currency_etl"]["exchange_rates"]
        collection.bulk_write.return_value = SimpleNamespace(upserted_count=3, matched_count=0, modified_count=0)
        summary = load_rates(documents)
        assert summary["upserted"] == 3
        assert collection.create_index.call_args.kwargs["unique"] is True
        operations = collection.bulk_write.call_args.args[0]
        assert len(operations) == 3
        assert all(operation._upsert for operation in operations)
        assert all(set(operation._filter) == {"base_currency", "target_currency", "rate_date"} for operation in operations)


def test_missing_mongodb_uri_fails(raw, monkeypatch):
    monkeypatch.delenv("MONGODB_URI", raising=False)
    with pytest.raises(ValueError, match="MONGODB_URI"):
        load_rates(transform_rates(raw, CurrencyConfig()))


def test_empty_load_fails():
    with pytest.raises(ValueError, match="XCom"):
        load_rates([])
