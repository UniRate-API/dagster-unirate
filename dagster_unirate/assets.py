"""Example Dagster assets built on :class:`UniRateResource`.

These are intentionally small, self-contained assets you can drop into a
``Definitions`` (see ``examples/definitions.py``) or copy into your own project
as a starting point.
"""

# NOTE: no ``from __future__ import annotations`` here. Dagster inspects the
# ``context`` parameter's annotation at definition time and requires the real
# ``AssetExecutionContext`` class object, not a stringized (PEP 563) annotation.
from dagster import AssetExecutionContext, MetadataValue, asset

from dagster_unirate.resources import UniRateResource

# Currencies fetched by the ``exchange_rates`` asset. Override by copying the
# asset into your project and editing this list.
DEFAULT_TARGETS = ["EUR", "GBP", "JPY", "CAD", "AUD"]


@asset(
    group_name="unirate",
    description="Latest USD-based exchange rates for a fixed set of currencies.",
)
def exchange_rates(
    context: AssetExecutionContext, unirate: UniRateResource
) -> dict[str, float]:
    """Materialize the latest USD→target rates as a ``{code: rate}`` mapping."""
    rates: dict[str, float] = {}
    for code in DEFAULT_TARGETS:
        result = unirate.get_rate("USD", code)
        # get_rate returns a float when a target is given.
        rates[code] = float(result) if not isinstance(result, dict) else result[code]

    context.add_output_metadata(
        {
            "num_currencies": len(rates),
            "base": "USD",
            "rates": MetadataValue.json(rates),
        }
    )
    return rates


@asset(
    group_name="unirate",
    description="Every currency code supported by the UniRate API.",
)
def currencies(context: AssetExecutionContext, unirate: UniRateResource) -> list[str]:
    """Materialize the full list of supported currency codes."""
    codes = unirate.list_currencies()
    context.add_output_metadata(
        {
            "count": len(codes),
            "preview": MetadataValue.json(codes[:20]),
        }
    )
    return codes
