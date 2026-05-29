# italian_our_world_data

`italian_our_world_data` retrieves public data relevant to Italy as pandas
`DataFrame` and GeoPandas `GeoDataFrame` objects. It supports ISTAT, OECD,
Eurostat, ECB, World Bank, FRED, INPS Open Data, OpenPNRR, and Italian
administrative boundaries. It also includes Bank of Italy exchange rates,
OpenCoesione, generic CKAN catalogues such as dati.gov.it and OpenBDAP, and
generic Socrata portals such as Regione Lombardia Open Data.

Full documentation: [docs/API.md](docs/API.md)

## Installation

Install the published release from PyPI:

```bash
python3 -m pip install italian-our-world-data
```

The PyPI command is available after the first release has been published.
Until then, or to install the current `main` branch directly from GitHub:

```bash
python3 -m pip install "git+https://github.com/NazarenoLecis/italian_our_world_data.git"
```

Install a tagged GitHub release for reproducible use:

```bash
python3 -m pip install "git+https://github.com/NazarenoLecis/italian_our_world_data.git@v2.1.0"
```

For contributors working from a clone:

```bash
python3 -m pip install -e .
```

In Python, the import name uses underscores:

```python
import italian_our_world_data
```

## Examples

For a single standard entry point, use `list_indicators()` to discover the
identifiers a source accepts, then pass one of them to `fetch_data()`:

```python
from italian_our_world_data import fetch_data, list_indicators, list_sources, source_info

print(list_sources()[["source", "item_name", "identifier_column", "fetch_parameter"]])
print(source_info("world_bank")["example"])

indicators = list_indicators("world_bank", per_page=20000)
gdp_indicator = indicators.loc[
    indicators["name"].eq("GDP (current US$)"),
    "indicator_id",
].iloc[0]

italian_gdp = fetch_data(
    "world_bank",
    indicator=gdp_indicator,
    country="ITA",
    start_year=2022,
    end_year=2023,
)
```

The same discovery layer is available from the terminal:

```bash
python3 -m italian_our_world_data sources
python3 -m italian_our_world_data info eurostat
python3 -m italian_our_world_data indicators ameco --format csv | grep NPTD
python3 -m italian_our_world_data indicators imf -p dataset=WEO --format csv | grep NGDP_RPCH
python3 -m italian_our_world_data indicators world_bank -p per_page=20000 --format csv | grep "GDP (current US$)"
python3 -m italian_our_world_data fetch ameco -p full_variable=1.0.0.0.NPTD -p countries=ITA -p years='[2022, 2023]'
python3 -m italian_our_world_data fetch world_bank -p indicator=NY.GDP.MKTP.CD -p country=ITA --head 5
```

The provider-specific functions remain available when you want explicit
source APIs:

```python
from italian_our_world_data import (
    fetch_ameco_data,
    fetch_bankitalia_exchange_rates,
    fetch_bis_data,
    fetch_ecb_data,
    fetch_eurostat_data,
    fetch_imf_data,
    fetch_istat_data,
    fetch_lombardy_data,
    fetch_opencoesione_data,
    fetch_world_bank_data,
)

employment = fetch_istat_data("150_915", ".......", start_period="2023", end_period="2023")

italian_gdp = fetch_eurostat_data(
    "nama_10_gdp",
    filters={"geo": "IT", "unit": "CP_MEUR", "na_item": "B1GQ"},
    start_period="2022",
    end_period="2023",
)

exchange_rate = fetch_ecb_data(
    "EXR", "D.USD.EUR.SP00.A", start_period="2023-01-01", end_period="2023-01-31"
)

world_bank_gdp = fetch_world_bank_data(
    "NY.GDP.MKTP.CD", country="ITA", start_year=2022, end_year=2023
)

ameco_population = fetch_ameco_data("1.0.0.0.NPTD", countries="ITA", years=[2022, 2023])
imf_growth = fetch_imf_data("NGDP_RPCH", countries="ITA", periods=[2022, 2023])
bis_exchange_rate_index = fetch_bis_data(
    "BIS,WS_EER,1.0", "M.N.B.IT", start_period="2023-01", end_period="2023-03"
)

exchange_rates = fetch_bankitalia_exchange_rates(
    reference_date="2023-01-03", base_currency="EUR", target_currency="USD"
)

cohesion_themes = fetch_opencoesione_data("temi", params={"page_size": 2})
lombardy_weather = fetch_lombardy_data("y856-h426", limit=10)
```

OECD's current SDMX API requires the complete dataflow reference and an
ordered key matching its dimensions:

```python
from italian_our_world_data import fetch_oecd_data

finance = fetch_oecd_data(
    "OECD.SDD.STES,DSD_STES@DF_FINMARK,",
    "............",
    start_period="2024",
    end_period="2024",
)
```

FRED observations for a known series can be downloaded without a key:

```python
from italian_our_world_data import fetch_fred_data

gdp = fetch_fred_data("GDP", start_period="2023-01-01", end_period="2023-12-31")
```

When `api_key=` is supplied, or `FRED_API_KEY` is set, the documented FRED
API endpoint is used instead. The official API requires a key; no-key
retrieval uses the public CSV download made available by the FRED site.
INPS functions are `list_inps_datasets()`, `get_inps_dataset_metadata()`, and
`fetch_inps_data()`; CSV and Excel resources are supported. OpenPNRR
functions are `list_pnrr_resources()` and `fetch_pnrr_data()`. CKAN helpers
are available through generic functions (`list_ckan_datasets()`,
`fetch_ckan_resource()`) and named wrappers for the national catalogue and
OpenBDAP.

Observation APIs normalize their period and numeric observation columns to
`time_period` and `value`. Periods remain strings so annual, quarterly, and
monthly data are represented without loss.

## Finding Available Data

Discovery follows a consistent naming rule: `list_<provider>_<resource>()`
returns catalogue information as a `DataFrame`; FRED supports
`search_fred_series()` when an API key is provided.

```python
from italian_our_world_data import (
    list_ameco_variables,
    list_bis_dataflows,
    list_ecb_dataflows,
    list_imf_indicators,
    list_inps_datasets,
    list_italian_open_data_datasets,
    list_world_bank_indicators,
    list_opencoesione_resources,
    list_pnrr_resources,
)

print(list_ecb_dataflows().head())
print(list_ameco_variables().head())
print(list_imf_indicators(dataset="WEO").head())
print(list_bis_dataflows().head())
print(list_world_bank_indicators(per_page=10).head())
print(list_inps_datasets(limit=10))
print(list_pnrr_resources())
print(list_opencoesione_resources())
print(list_italian_open_data_datasets(rows=5))
```

The complete discovery table, source-browser links, SDMX key guidance, and
examples for every provider are in [docs/API.md](docs/API.md).

## GeoDataFrames

The geospatial helpers integrate administrative boundary GeoJSON from
[confini-amministrativi.it](https://www.confini-amministrativi.it/):

```python
from italian_our_world_data import fetch_administrative_boundaries

regions = fetch_administrative_boundaries("regioni")
print(regions.head())
```

Use `attach_administrative_boundaries(...)` to join a table with matching
administrative codes to a boundary layer and get a GeoDataFrame.
Boundary helpers use the latest available release by default; pass
`release="20200101"` or another release ID when you need a pinned layer.

## Notes

Use selective SDMX keys and date ranges for large datasets. In particular,
ISTAT documents a low request-rate limit for its SDMX service.

Runnable provider examples are grouped under [examples/](examples/README.md)
and import the public library implementation.

## Testing

Install the package and run deterministic tests:

```bash
python3 -m pip install -e .
python3 -m unittest discover -s tests -v
```

Run bounded live checks against every supported provider:

```bash
python3 -m italian_our_world_data.verify
```

The live command makes network requests and reports one `PASS` or `FAIL` line
per provider. The INPS check verifies its live dataset catalogue; the unit
suite covers loading INPS downloadable CSV resources without depending on a
particular remote file remaining available.

## Publishing A Release

The repository includes a GitHub Actions release workflow for PyPI Trusted
Publishing. The maintainer must configure a trusted publisher on PyPI once,
then publish a GitHub release tagged with the matching version, for example
`v2.1.0`. Detailed steps are in
[docs/RELEASING.md](docs/RELEASING.md).
