"""Dagster integration for the UniRate currency-exchange API."""

from __future__ import annotations

from dagster_unirate.assets import currencies, exchange_rates
from dagster_unirate.client import (
    APIError,
    AuthenticationError,
    InvalidCurrencyError,
    InvalidDateError,
    RateLimitError,
    UniRateClient,
    UniRateError,
)
from dagster_unirate.resources import UniRateResource

__version__ = "0.1.0"

__all__ = [
    "APIError",
    "AuthenticationError",
    "InvalidCurrencyError",
    "InvalidDateError",
    "RateLimitError",
    "UniRateClient",
    "UniRateError",
    "UniRateResource",
    "__version__",
    "currencies",
    "exchange_rates",
]
