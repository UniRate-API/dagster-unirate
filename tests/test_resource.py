"""Mock tests for UniRateResource (respx-backed HTTP layer)."""

from __future__ import annotations

import httpx
import pytest
import respx

from dagster_unirate import UniRateResource
from dagster_unirate.client import APIError, AuthenticationError

BASE = "https://api.unirateapi.com"


def _resource() -> UniRateResource:
    return UniRateResource(api_key="test-key", base_url=BASE)


@respx.mock
def test_resource_get_rate():
    respx.get(f"{BASE}/api/rates").mock(
        return_value=httpx.Response(200, json={"rate": "0.92"})
    )
    assert _resource().get_rate("USD", "EUR") == 0.92


@respx.mock
def test_resource_convert():
    respx.get(f"{BASE}/api/convert").mock(
        return_value=httpx.Response(200, json={"result": "150.0"})
    )
    assert _resource().convert("EUR", 100.0, "USD") == 150.0


@respx.mock
def test_resource_list_currencies():
    respx.get(f"{BASE}/api/currencies").mock(
        return_value=httpx.Response(200, json={"currencies": ["USD", "EUR"]})
    )
    assert _resource().list_currencies() == ["USD", "EUR"]


@respx.mock
def test_resource_get_vat_rates():
    respx.get(f"{BASE}/api/vat/rates").mock(
        return_value=httpx.Response(
            200, json={"country": "DE", "vat_data": {"vat_rate": 19.0}}
        )
    )
    result = _resource().get_vat_rates("DE")
    assert result["vat_data"]["vat_rate"] == 19.0


@respx.mock
def test_resource_historical_403_tolerated():
    respx.get(f"{BASE}/api/historical/rates").mock(
        return_value=httpx.Response(403, text="pro")
    )
    with pytest.raises(APIError) as exc:
        _resource().get_historical_rate("2024-01-15", "EUR")
    assert exc.value.status_code == 403


@respx.mock
def test_resource_propagates_auth_error():
    respx.get(f"{BASE}/api/rates").mock(return_value=httpx.Response(401))
    with pytest.raises(AuthenticationError):
        _resource().get_rate("USD", "EUR")


def test_resource_config_defaults():
    r = UniRateResource(api_key="k")
    assert r.base_url == "https://api.unirateapi.com"
    assert r.timeout == 30.0
