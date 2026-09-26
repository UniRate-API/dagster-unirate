"""Dagster resource wrapping the UniRate API."""

from __future__ import annotations

from typing import Any

from dagster import ConfigurableResource
from pydantic import Field

from dagster_unirate.client import (
    DEFAULT_BASE_URL,
    DEFAULT_TIMEOUT,
    UniRateClient,
)


class UniRateResource(ConfigurableResource):
    """Dagster resource for the UniRate currency-exchange API.

    Configure it in your ``Definitions`` and request it from any op or asset::

        from dagster import Definitions, EnvVar
        from dagster_unirate import UniRateResource

        defs = Definitions(
            assets=[...],
            resources={"unirate": UniRateResource(api_key=EnvVar("UNIRATE_API_KEY"))},
        )

    The resource exposes the free-tier surface (rates, convert, currencies, VAT)
    plus a Pro-gated historical helper that raises on the free tier.
    """

    api_key: str = Field(
        description="UniRate API key. Get a free one at https://unirateapi.com.",
    )
    base_url: str = Field(
        default=DEFAULT_BASE_URL,
        description="UniRate API base URL.",
    )
    timeout: float = Field(
        default=DEFAULT_TIMEOUT,
        description="Per-request timeout in seconds.",
    )

    def _client(self) -> UniRateClient:
        return UniRateClient(
            api_key=self.api_key,
            base_url=self.base_url,
            timeout=self.timeout,
        )

    # ------------------------------------------------------------------
    # Free-tier surface
    # ------------------------------------------------------------------

    def get_rate(
        self, from_currency: str = "USD", to_currency: str | None = None
    ) -> float | dict[str, float]:
        """Latest rate for a pair, or every rate for ``from_currency``."""
        with self._client() as client:
            return client.get_rate(from_currency, to_currency)

    def convert(
        self,
        to_currency: str,
        amount: float = 1.0,
        from_currency: str = "USD",
    ) -> float:
        """Convert ``amount`` from ``from_currency`` to ``to_currency``."""
        with self._client() as client:
            return client.convert(to_currency, amount, from_currency)

    def list_currencies(self) -> list[str]:
        """Return every supported currency code."""
        with self._client() as client:
            return client.list_currencies()

    def get_vat_rates(self, country: str | None = None) -> dict[str, Any]:
        """VAT rates for all countries or one ISO-3166 alpha-2 code."""
        with self._client() as client:
            return client.get_vat_rates(country)

    # ------------------------------------------------------------------
    # Pro-gated (feature-flagged; raises APIError 403 on the free tier)
    # ------------------------------------------------------------------

    def get_historical_rate(
        self,
        date: str,
        to_currency: str | None = None,
        amount: float = 1.0,
        from_currency: str = "USD",
    ) -> float | dict[str, float]:
        """Rate observed on ``date`` (YYYY-MM-DD). Requires a Pro plan."""
        with self._client() as client:
            return client.get_historical_rate(date, to_currency, amount, from_currency)
