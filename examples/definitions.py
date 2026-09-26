"""Runnable Dagster ``Definitions`` for dagster-unirate.

Point Dagster at this file:

    export UNIRATE_API_KEY=your-key-here
    dagster dev -f examples/definitions.py

Then materialize the ``exchange_rates`` and ``currencies`` assets from the UI,
or headless::

    dagster asset materialize --select '*' -f examples/definitions.py

Get a free API key at https://unirateapi.com.
"""

from __future__ import annotations

from dagster import Definitions, EnvVar

from dagster_unirate import UniRateResource, currencies, exchange_rates

defs = Definitions(
    assets=[exchange_rates, currencies],
    resources={
        "unirate": UniRateResource(api_key=EnvVar("UNIRATE_API_KEY")),
    },
)
