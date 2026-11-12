"""ETL logic kept independent of Airflow for focused testing."""

from __future__ import annotations

import logging
import math
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

import requests
from pymongo import ASCENDING, MongoClient, UpdateOne
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

LOGGER = logging.getLogger(__name__)
API_URL = "https://api.frankfurter.dev/v2/rates"
SOURCE = "Frankfurter API"
RAW_XCOM_KEY = "raw_exchange_rates"
TRANSFORMED_XCOM_KEY = "transformed_exchange_rates"
LOAD_XCOM_KEY = "mongodb_load_summary"


@dataclass(frozen=True)
class CurrencyConfig:
    base: str = "EUR"
    targets: tuple[str, ...] = ("USD", "GBP", "JPY")

    def __post_init__(self) -> None:
        if not re.fullmatch(r"[A-Z]{3}", self.base):
            raise ValueError("Base currency must be a three-letter uppercase currency code")
        if not self.targets or any(not re.fullmatch(r"[A-Z]{3}", code) for code in self.targets):
            raise ValueError("At least one valid target currency is required")
        if len(set(self.targets)) != len(self.targets) or self.base in self.targets:
            raise ValueError("Target currencies must be unique and different from the base")

    @classmethod
    def from_env(cls) -> CurrencyConfig:
        return cls(
            base=os.getenv("ETL_BASE_CURRENCY", "EUR").strip().upper(),
            targets=tuple(
                code.strip().upper()
                for code in os.getenv("ETL_TARGET_CURRENCIES", "USD,GBP,JPY").split(",")
            ),
        )


def extract_rates(config: CurrencyConfig) -> dict[str, Any]:
    """Fetch latest reference rates with finite timeouts and transient retries."""
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
    )
    with requests.Session() as session:
        session.mount("https://", HTTPAdapter(max_retries=retry))
        response = session.get(
            API_URL,
            params={"base": config.base, "quotes": ",".join(config.targets)},
            headers={"Accept": "application/json", "User-Agent": "task3-currency-etl/1.0"},
            timeout=(10, 30),
        )
        response.raise_for_status()
        rates = response.json()
    if not isinstance(rates, list) or not rates:
        raise ValueError("Frankfurter returned an empty or invalid rates array")
    retrieved_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    LOGGER.info("Extracted %s rates for %s at %s", len(rates), config.base, retrieved_at)
    return {"rates": rates, "retrieved_at": retrieved_at}


def transform_rates(raw: dict[str, Any], config: CurrencyConfig) -> list[dict[str, Any]]:
    """Validate a complete response and map it to the six assignment fields."""
    if not isinstance(raw, dict) or not isinstance(raw.get("rates"), list) or not raw["rates"]:
        raise ValueError("Missing or invalid raw_exchange_rates XCom payload")
    try:
        retrieved = datetime.fromisoformat(raw["retrieved_at"])
        if retrieved.tzinfo is None or retrieved.utcoffset() is None:
            raise ValueError("retrieved_at must include a timezone")
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("Invalid retrieved_at timestamp") from exc

    documents: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in raw["rates"]:
        if not isinstance(row, dict):
            raise ValueError("Each rate must be an object")
        base, target, rate = row.get("base"), row.get("quote"), row.get("rate")
        if base != config.base or target not in config.targets:
            raise ValueError("Unexpected base or target currency in API response")
        if target in seen:
            raise ValueError(f"Duplicate target currency: {target}")
        if isinstance(rate, bool) or not isinstance(rate, (int, float)) or not math.isfinite(rate) or rate <= 0:
            raise ValueError(f"Exchange rate for {target} must be a finite positive number")
        try:
            rate_date = date.fromisoformat(row["date"])
            if row["date"] != rate_date.isoformat():
                raise ValueError("Rate date must use YYYY-MM-DD")
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"Invalid rate date for {target}") from exc
        if rate_date > retrieved.astimezone(timezone.utc).date():
            raise ValueError("Rate date cannot be after retrieval date")
        seen.add(target)
        documents.append({
            "base_currency": base,
            "target_currency": target,
            "exchange_rate": float(rate),
            "rate_date": rate_date.isoformat(),
            "source": SOURCE,
            "retrieved_at": retrieved.astimezone(timezone.utc).isoformat(timespec="seconds"),
        })
    if seen != set(config.targets):
        raise ValueError(f"Missing requested currencies: {sorted(set(config.targets) - seen)}")
    LOGGER.info("Validated and transformed %s exchange-rate documents", len(documents))
    return sorted(documents, key=lambda document: document["target_currency"])


def load_rates(documents: list[dict[str, Any]]) -> dict[str, Any]:
    """Upsert by currency pair and publication date so retries cannot duplicate data."""
    if not isinstance(documents, list) or not documents:
        raise ValueError("Missing or empty transformed_exchange_rates XCom payload")
    uri = os.environ.get("MONGODB_URI")
    if not uri:
        raise ValueError("Set MONGODB_URI before loading exchange rates")
    database = os.getenv("MONGODB_DATABASE", "currency_etl")
    collection_name = os.getenv("MONGODB_COLLECTION", "exchange_rates")
    with MongoClient(uri, serverSelectionTimeoutMS=10000, connectTimeoutMS=10000, timeoutMS=30000) as client:
        client.admin.command("ping")
        collection = client[database][collection_name]
        collection.create_index(
            [("base_currency", ASCENDING), ("target_currency", ASCENDING), ("rate_date", ASCENDING)],
            unique=True,
            name="unique_currency_pair_date",
        )
        operations = [
            UpdateOne(
                {key: document[key] for key in ("base_currency", "target_currency", "rate_date")},
                {"$set": document},
                upsert=True,
            )
            for document in documents
        ]
        result = collection.bulk_write(operations, ordered=True)
    summary = {
        "database": database,
        "collection": collection_name,
        "processed": len(documents),
        "upserted": result.upserted_count,
        "matched": result.matched_count,
        "modified": result.modified_count,
    }
    LOGGER.info("MongoDB load completed: %s", summary)
    return summary
