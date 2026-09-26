"""Mock tests for the internal UniRate HTTP client (respx-backed)."""

from __future__ import annotations

import httpx
import pytest
import respx

from dagster_unirate.client import (
    APIError,
    AuthenticationError,
    InvalidCurrencyError,
    InvalidDateError,
    RateLimitError,
    UniRateClient,
)

BASE = "https://api.unirateapi.com"


def _client() -> UniRateClient:
    return UniRateClient(api_key="test-key", base_url=BASE)


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


def test_requires_api_key():
    with pytest.raises(ValueError, match="api_key is required"):
        UniRateClient(api_key="")


def test_base_url_trailing_slash_stripped():
    client = UniRateClient(api_key="k", base_url="https://example.com/")
    assert client.base_url == "https://example.com"


# ---------------------------------------------------------------------------
# Success paths
# ---------------------------------------------------------------------------


@respx.mock
def test_get_rate_single_pair():
    respx.get(f"{BASE}/api/rates").mock(
        return_value=httpx.Response(200, json={"rate": "0.92"})
    )
    with _client() as client:
        assert client.get_rate("USD", "EUR") == 0.92


@respx.mock
def test_get_rate_all_targets():
    respx.get(f"{BASE}/api/rates").mock(
        return_value=httpx.Response(200, json={"rates": {"EUR": "0.92", "GBP": "0.79"}})
    )
    with _client() as client:
        assert client.get_rate("USD") == {"EUR": 0.92, "GBP": 0.79}


@respx.mock
def test_get_rate_uppercases_inputs():
    route = respx.get(f"{BASE}/api/rates").mock(
        return_value=httpx.Response(200, json={"rate": "0.92"})
    )
    with _client() as client:
        client.get_rate("usd", "eur")
    params = route.calls.last.request.url.params
    assert params["from"] == "USD"
    assert params["to"] == "EUR"


@respx.mock
def test_convert():
    route = respx.get(f"{BASE}/api/convert").mock(
        return_value=httpx.Response(200, json={"result": "92.50"})
    )
    with _client() as client:
        assert client.convert("EUR", 100.0, "USD") == 92.50
    params = route.calls.last.request.url.params
    assert params["amount"] == "100.0"


@respx.mock
def test_list_currencies_sends_accept_header():
    route = respx.get(f"{BASE}/api/currencies").mock(
        return_value=httpx.Response(200, json={"currencies": ["USD", "EUR", "GBP"]})
    )
    with _client() as client:
        assert client.list_currencies() == ["USD", "EUR", "GBP"]
    assert route.calls.last.request.headers["accept"] == "application/json"


@respx.mock
def test_get_vat_rates_all():
    respx.get(f"{BASE}/api/vat/rates").mock(
        return_value=httpx.Response(
            200,
            json={"total_countries": 1, "date": "2026-07-20", "vat_rates": {}},
        )
    )
    with _client() as client:
        result = client.get_vat_rates()
    assert result["total_countries"] == 1


@respx.mock
def test_get_vat_rates_single_country_uppercased():
    route = respx.get(f"{BASE}/api/vat/rates").mock(
        return_value=httpx.Response(
            200, json={"country": "DE", "vat_data": {"vat_rate": 19.0}}
        )
    )
    with _client() as client:
        client.get_vat_rates("de")
    assert route.calls.last.request.url.params["country"] == "DE"


@respx.mock
def test_api_key_sent_as_query_param():
    route = respx.get(f"{BASE}/api/currencies").mock(
        return_value=httpx.Response(200, json={"currencies": ["USD"]})
    )
    with _client() as client:
        client.list_currencies()
    assert route.calls.last.request.url.params["api_key"] == "test-key"


# ---------------------------------------------------------------------------
# Pro-gated historical
# ---------------------------------------------------------------------------


@respx.mock
def test_historical_rate_single():
    respx.get(f"{BASE}/api/historical/rates").mock(
        return_value=httpx.Response(200, json={"rate": "0.90"})
    )
    with _client() as client:
        assert client.get_historical_rate("2024-01-15", "EUR") == 0.90


@respx.mock
def test_historical_rate_403_on_free_tier():
    respx.get(f"{BASE}/api/historical/rates").mock(
        return_value=httpx.Response(403, text="pro only")
    )
    with _client() as client:
        with pytest.raises(APIError) as exc:
            client.get_historical_rate("2024-01-15", "EUR")
    assert exc.value.status_code == 403


# ---------------------------------------------------------------------------
# Error mapping
# ---------------------------------------------------------------------------


@respx.mock
def test_error_400():
    respx.get(f"{BASE}/api/rates").mock(return_value=httpx.Response(400))
    with _client() as client:
        with pytest.raises(InvalidDateError):
            client.get_rate("USD", "EUR")


@respx.mock
def test_error_401():
    respx.get(f"{BASE}/api/rates").mock(return_value=httpx.Response(401))
    with _client() as client:
        with pytest.raises(AuthenticationError):
            client.get_rate("USD", "EUR")


@respx.mock
def test_error_403():
    respx.get(f"{BASE}/api/rates").mock(return_value=httpx.Response(403, text="pro"))
    with _client() as client:
        with pytest.raises(APIError) as exc:
            client.get_rate("USD", "EUR")
    assert exc.value.status_code == 403


@respx.mock
def test_error_404():
    respx.get(f"{BASE}/api/rates").mock(return_value=httpx.Response(404))
    with _client() as client:
        with pytest.raises(InvalidCurrencyError):
            client.get_rate("USD", "FAKE")


@respx.mock
def test_error_429():
    respx.get(f"{BASE}/api/rates").mock(return_value=httpx.Response(429))
    with _client() as client:
        with pytest.raises(RateLimitError):
            client.get_rate("USD", "EUR")


@respx.mock
def test_error_503():
    respx.get(f"{BASE}/api/rates").mock(return_value=httpx.Response(503, text="down"))
    with _client() as client:
        with pytest.raises(APIError) as exc:
            client.get_rate("USD", "EUR")
    assert exc.value.status_code == 503


@respx.mock
def test_error_generic_500():
    respx.get(f"{BASE}/api/rates").mock(return_value=httpx.Response(500, text="boom"))
    with _client() as client:
        with pytest.raises(APIError) as exc:
            client.get_rate("USD", "EUR")
    assert exc.value.status_code == 500
    assert exc.value.body == "boom"
