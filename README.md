# dagster-unirate

[![PyPI](https://img.shields.io/pypi/v/dagster-unirate.svg)](https://pypi.org/project/dagster-unirate/)
[![Dagster](https://img.shields.io/badge/Dagster-1.8%2B-blue?logo=dagster)](https://dagster.io/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Dagster integration for **[UniRateAPI](https://unirateapi.com)** — real-time
currency exchange rates, conversion, supported currencies, and VAT rates. Ships
a `ConfigurableResource` plus a pair of ready-to-run example assets.

## Install

```bash
pip install dagster-unirate
```

Requires `dagster >= 1.8` and Python 3.10+. Get a free API key at
<https://unirateapi.com>.

## Quick start

```python
from dagster import Definitions, EnvVar
from dagster_unirate import UniRateResource, exchange_rates, currencies

defs = Definitions(
    assets=[exchange_rates, currencies],
    resources={
        "unirate": UniRateResource(api_key=EnvVar("UNIRATE_API_KEY")),
    },
)
```

```bash
export UNIRATE_API_KEY=your-key-here
dagster dev -f examples/definitions.py
```

## Resource — `UniRateResource`

A Dagster `ConfigurableResource`. Configure it once in your `Definitions`, then
request it from any op or asset by name.

| Config    | Default                         | Notes                        |
|-----------|---------------------------------|------------------------------|
| `api_key` | — (required)                    | Use `EnvVar("UNIRATE_API_KEY")` in prod |
| `base_url`| `https://api.unirateapi.com`    | Override for testing         |
| `timeout` | `30.0`                          | Per-request timeout, seconds |

Methods (free tier):

```python
from dagster import asset
from dagster_unirate import UniRateResource

@asset
def my_rates(unirate: UniRateResource):
    rate = unirate.get_rate("USD", "EUR")        # -> 0.92 (float)
    all_rates = unirate.get_rate("USD")          # -> {"EUR": 0.92, ...}
    amount = unirate.convert("EUR", 100, "USD")  # -> 92.5 (convert amount)
    codes = unirate.list_currencies()            # -> ["USD", "EUR", ...]
    vat = unirate.get_vat_rates("DE")            # -> {"vat_data": {...}}
    return all_rates
```

Pro-gated (raises `APIError` with `status_code == 403` on the free tier):

```python
unirate.get_historical_rate("2024-01-15", "EUR")   # requires a Pro plan
```

Currency and country codes are uppercased automatically before the request.

## Example assets

- **`exchange_rates`** — latest USD-based rates for a fixed set of currencies,
  returned as a `{code: rate}` dict with per-materialization metadata.
- **`currencies`** — the full list of supported currency codes.

Both live in `dagster_unirate.assets`; copy them into your project and edit the
target list, or use them as-is. A runnable `Definitions` is in
`examples/definitions.py`.

## Error handling

All errors inherit from `UniRateError`:

| HTTP | Exception               |
|------|-------------------------|
| 400  | `InvalidDateError`      |
| 401  | `AuthenticationError`   |
| 403  | `APIError` (`status_code=403`) — Pro-gated endpoints |
| 404  | `InvalidCurrencyError`  |
| 429  | `RateLimitError`        |
| 503  | `APIError` (`status_code=503`) |
| other| `APIError` (with status + body) |

```python
from dagster_unirate import RateLimitError, UniRateError

try:
    unirate.get_rate("USD", "EUR")
except RateLimitError:
    ...  # back off
except UniRateError:
    ...  # any UniRate failure
```

## Testing

```bash
pip install -e ".[test]"
pytest
```

The suite mocks the HTTP layer with `respx` (no network) and materializes the
example assets via `dagster.materialize`.

## Status

`v0.1.0` — initial release. `UniRateResource` + 2 example assets. 33 mock tests.

## Other UniRate clients

UniRate ships official client libraries and framework integrations across the
ecosystem, all maintained under the
[UniRate-API](https://github.com/UniRate-API) org.

- **Languages:** [Python](https://github.com/UniRate-API/unirate-api-python) · [Node.js / TypeScript](https://github.com/UniRate-API/unirate-api-nodejs) · [Go](https://github.com/UniRate-API/unirate-api-go) · [Rust](https://github.com/UniRate-API/unirate-api-rust) · [Java](https://github.com/UniRate-API/unirate-api-java) · [Ruby](https://github.com/UniRate-API/unirate-api-ruby) · [PHP](https://github.com/UniRate-API/unirate-api-php) · [.NET](https://github.com/UniRate-API/unirate-api-dotnet) · [Swift](https://github.com/UniRate-API/unirate-api-swift)
- **Data / orchestration:** [Airflow](https://github.com/UniRate-API/airflow-provider-unirate) · [dbt](https://github.com/UniRate-API/dbt-unirate) · [Dagster](https://github.com/UniRate-API/dagster-unirate) · [LangChain](https://github.com/UniRate-API/langchain-unirate)
- **Web frameworks:** [FastAPI](https://github.com/UniRate-API/fastapi-unirate) · [Flask](https://github.com/UniRate-API/flask-unirate) · [Django / Wagtail](https://github.com/UniRate-API/wagtail-unirate) · [Streamlit](https://github.com/UniRate-API/streamlit-unirate)

Get a free API key at [unirateapi.com](https://unirateapi.com).

## License

MIT — see [LICENSE](LICENSE).

## Links

- UniRate API docs: <https://unirateapi.com/docs>
- Dagster resources guide: <https://docs.dagster.io/concepts/resources>
- Issues: <https://github.com/UniRate-API/dagster-unirate/issues>
