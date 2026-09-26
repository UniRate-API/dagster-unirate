"""Mock tests for the example assets, materialized with the resource."""

from __future__ import annotations

import httpx
import respx
from dagster import materialize

from dagster_unirate import UniRateResource, currencies, exchange_rates
from dagster_unirate.assets import DEFAULT_TARGETS

BASE = "https://api.unirateapi.com"


def _resource() -> UniRateResource:
    return UniRateResource(api_key="test-key", base_url=BASE)


@respx.mock
def test_materialize_exchange_rates():
    respx.get(f"{BASE}/api/rates").mock(
        return_value=httpx.Response(200, json={"rate": "0.92"})
    )
    result = materialize([exchange_rates], resources={"unirate": _resource()})
    assert result.success
    output = result.output_for_node("exchange_rates")
    assert set(output) == set(DEFAULT_TARGETS)
    assert all(v == 0.92 for v in output.values())


@respx.mock
def test_materialize_exchange_rates_metadata():
    respx.get(f"{BASE}/api/rates").mock(
        return_value=httpx.Response(200, json={"rate": "1.10"})
    )
    result = materialize([exchange_rates], resources={"unirate": _resource()})
    assert result.success
    mats = result.asset_materializations_for_node("exchange_rates")
    meta = mats[0].metadata
    assert meta["num_currencies"].value == len(DEFAULT_TARGETS)
    assert meta["base"].value == "USD"


@respx.mock
def test_materialize_currencies():
    codes = ["USD", "EUR", "GBP", "JPY"]
    respx.get(f"{BASE}/api/currencies").mock(
        return_value=httpx.Response(200, json={"currencies": codes})
    )
    result = materialize([currencies], resources={"unirate": _resource()})
    assert result.success
    assert result.output_for_node("currencies") == codes


@respx.mock
def test_materialize_currencies_metadata():
    codes = [f"C{i}" for i in range(30)]
    respx.get(f"{BASE}/api/currencies").mock(
        return_value=httpx.Response(200, json={"currencies": codes})
    )
    result = materialize([currencies], resources={"unirate": _resource()})
    mats = result.asset_materializations_for_node("currencies")
    assert mats[0].metadata["count"].value == 30


@respx.mock
def test_materialize_both_assets():
    respx.get(f"{BASE}/api/rates").mock(
        return_value=httpx.Response(200, json={"rate": "0.5"})
    )
    respx.get(f"{BASE}/api/currencies").mock(
        return_value=httpx.Response(200, json={"currencies": ["USD", "EUR"]})
    )
    result = materialize(
        [exchange_rates, currencies], resources={"unirate": _resource()}
    )
    assert result.success
