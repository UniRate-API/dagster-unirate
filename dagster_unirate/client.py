"""Internal synchronous HTTP client for the UniRate API.

Dagster resources and assets execute synchronously, so this client wraps a
plain ``httpx.Client``. It carries the full UniRate error mapping and is used
by :class:`dagster_unirate.resources.UniRateResource`.
"""

from __future__ import annotations

from typing import Any

import httpx

DEFAULT_BASE_URL = "https://api.unirateapi.com"
DEFAULT_TIMEOUT = 30.0


class UniRateError(Exception):
    """Base class for every UniRate client error."""


class APIError(UniRateError):
    """Generic API error carrying the HTTP status code and response body."""

    def __init__(
        self, message: str, status_code: int | None = None, body: str | None = None
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.body = body


class InvalidDateError(UniRateError):
    """HTTP 400 — invalid request parameters."""


class AuthenticationError(UniRateError):
    """HTTP 401 — missing or invalid API key."""


class InvalidCurrencyError(UniRateError):
    """HTTP 404 — currency not found or no data available."""


class RateLimitError(UniRateError):
    """HTTP 429 — rate limit exceeded."""


class UniRateClient:
    """Thin synchronous client over the UniRate REST API.

    Args:
        api_key: Your UniRate API key. Sent as an ``api_key`` query parameter.
        base_url: API base URL. Defaults to ``https://api.unirateapi.com``.
        timeout: Per-request timeout in seconds.
        http_client: Optional pre-built ``httpx.Client`` (dependency injection
            for tests). When omitted, one is created and owned by this client.
    """

    def __init__(
        self,
        api_key: str,
        *,
        base_url: str = DEFAULT_BASE_URL,
        timeout: float = DEFAULT_TIMEOUT,
        http_client: httpx.Client | None = None,
    ) -> None:
        if not api_key:
            msg = "api_key is required"
            raise ValueError(msg)
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._owns_client = http_client is None
        self._client = http_client or httpx.Client(timeout=timeout)

    def close(self) -> None:
        """Close the underlying HTTP client if this instance owns it."""
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> UniRateClient:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def _request(
        self, path: str, params: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        full_params: dict[str, Any] = {"api_key": self.api_key}
        if params:
            full_params.update(params)
        try:
            response = self._client.get(
                f"{self.base_url}{path}",
                params=full_params,
                # Accept header is NOT optional: /api/currencies returns an
                # HTML 404 without it.
                headers={"Accept": "application/json"},
            )
        except httpx.HTTPError as exc:  # pragma: no cover - network errors
            raise UniRateError(f"UniRate request failed: {exc}") from exc

        status = response.status_code
        if status == 400:
            raise InvalidDateError("Invalid request parameters")
        if status == 401:
            raise AuthenticationError("Missing or invalid API key")
        if status == 403:
            raise APIError(
                "Endpoint requires a Pro subscription",
                status_code=403,
                body=response.text,
            )
        if status == 404:
            raise InvalidCurrencyError("Currency not found or no data available")
        if status == 429:
            raise RateLimitError("Rate limit exceeded")
        if status == 503:
            raise APIError("Service unavailable", status_code=503, body=response.text)
        if status >= 400:
            raise APIError(
                f"UniRate API error: HTTP {status}",
                status_code=status,
                body=response.text,
            )
        return response.json()

    # ------------------------------------------------------------------
    # Free-tier endpoints
    # ------------------------------------------------------------------

    def get_rate(
        self, from_currency: str = "USD", to_currency: str | None = None
    ) -> float | dict[str, float]:
        """Return the latest rate, or a mapping of every target rate.

        Returns a single ``float`` when ``to_currency`` is supplied, otherwise
        a ``dict`` of every supported target rate keyed by currency code.
        """
        params: dict[str, Any] = {"from": from_currency.upper()}
        if to_currency is not None:
            params["to"] = to_currency.upper()
        data = self._request("/api/rates", params)
        if to_currency is not None:
            return float(data["rate"])
        return {code: float(rate) for code, rate in data["rates"].items()}

    def convert(
        self,
        to_currency: str,
        amount: float = 1.0,
        from_currency: str = "USD",
    ) -> float:
        """Convert ``amount`` from one currency to another at the latest rate."""
        data = self._request(
            "/api/convert",
            {
                "from": from_currency.upper(),
                "to": to_currency.upper(),
                "amount": amount,
            },
        )
        return float(data["result"])

    def list_currencies(self) -> list[str]:
        """Return every supported currency/ticker code."""
        data = self._request("/api/currencies")
        return list(data["currencies"])

    def get_vat_rates(self, country: str | None = None) -> dict[str, Any]:
        """VAT rates for all countries, or one ISO-3166 alpha-2 country code."""
        params: dict[str, Any] = {}
        if country is not None:
            params["country"] = country.upper()
        return self._request("/api/vat/rates", params)

    # ------------------------------------------------------------------
    # Pro-gated endpoint (feature-flagged; tolerate 403 on free tier)
    # ------------------------------------------------------------------

    def get_historical_rate(
        self,
        date: str,
        to_currency: str | None = None,
        amount: float = 1.0,
        from_currency: str = "USD",
    ) -> float | dict[str, float]:
        """Rate observed on ``date`` (YYYY-MM-DD). Pro-gated — raises
        :class:`APIError` (status 403) on the free tier."""
        params: dict[str, Any] = {
            "date": date,
            "from": from_currency.upper(),
            "amount": amount,
        }
        if to_currency is not None:
            params["to"] = to_currency.upper()
        data = self._request("/api/historical/rates", params)
        if to_currency is not None:
            if "result" in data:
                return float(data["result"])
            return float(data["rate"])
        key = "results" if "results" in data else "rates"
        return {code: float(rate) for code, rate in data[key].items()}
