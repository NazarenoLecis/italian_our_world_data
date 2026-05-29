# Library Usage And Data Discovery

## Public Naming Convention

The importable API follows three verbs:

| Verb | Purpose | Return value |
| --- | --- | --- |
| `fetch_<provider>_data(...)` | Retrieve observations or table rows | `pandas.DataFrame` |
| `list_<provider>_<resource>(...)` | Discover available data objects | `pandas.DataFrame` |
| `get_<provider>_<resource>_metadata(...)` | Retrieve metadata for one object | `dict` |

Provider terminology is retained because it tells you what identifier is
needed: ISTAT, OECD, Eurostat, and ECB expose SDMX **dataflows**; World Bank
exposes **indicators**; FRED exposes **series**; INPS, dati.gov.it, and
OpenBDAP expose catalogue **datasets**; OpenPNRR and OpenCoesione expose API
**resources**; Socrata portals expose tabular **dataset IDs**.

`get_inps_dataset()` remains available as a compatibility alias for
`get_inps_dataset_metadata()`.

## Imports

```python
from italian_our_world_data import (
    attach_administrative_boundaries,
    discover_data,
    fetch_data,
    fetch_bankitalia_exchange_rates,
    fetch_bdap_data,
    fetch_ckan_resource,
    fetch_ecb_data,
    fetch_eurostat_data,
    fetch_fred_data,
    fetch_inps_data,
    fetch_italian_open_data_resource,
    fetch_istat_data,
    fetch_lombardy_data,
    fetch_oecd_data,
    fetch_opencoesione_data,
    fetch_pnrr_data,
    fetch_socrata_data,
    fetch_world_bank_data,
    fetch_administrative_boundaries,
    fetch_administrative_boundary_metadata,
    get_bdap_dataset_metadata,
    get_ckan_dataset_metadata,
    get_ckan_resource_metadata,
    get_source_info,
    get_inps_dataset_metadata,
    get_italian_open_data_dataset_metadata,
    get_lombardy_dataset_metadata,
    get_socrata_dataset_metadata,
    list_administrative_boundary_divisions,
    list_indicators,
    list_source_items,
    list_sources,
    list_bankitalia_currencies,
    list_bdap_datasets,
    list_ckan_datasets,
    list_ecb_dataflows,
    list_eurostat_dataflows,
    list_inps_datasets,
    list_italian_open_data_datasets,
    list_istat_dataflows,
    list_lombardy_datasets,
    list_oecd_dataflows,
    list_opencoesione_resources,
    list_pnrr_resources,
    list_socrata_datasets,
    list_world_bank_indicators,
    search_fred_series,
    source_info,
)
```

Retrieval functions return a `DataFrame`. Observation-oriented responses use
`time_period` for the period identifier and `value` for numeric observations.
`time_period` is intentionally a string because sources publish annual,
quarterly, monthly, and daily frequencies.

## Unified Gateway

The provider-specific functions remain available, but users can start with a
single gateway:

| Function | Purpose |
| --- | --- |
| `list_sources()` | Return a table of supported source IDs, categories, and fetch/discovery functions |
| `source_info(source=None)` | Return all sources when no source is passed, or detailed help for one source |
| `list_indicators(source=None, **params)` | Show identifier columns for all sources, or list usable identifiers for one source |
| `list_source_items(source=None, **params)` | Neutral alias for `list_indicators()`; useful because some sources expose datasets, resources, or series |
| `discover_data(source, **params)` | Run the source's catalogue/listing function |
| `fetch_data(source, **params)` | Run the source's fetch function |

```python
from italian_our_world_data import fetch_data, list_indicators, list_sources, source_info

print(list_sources()[["source", "item_name", "identifier_column", "fetch_parameter"]])
print(source_info("world_bank")["example"])

indicators = list_indicators("world_bank", per_page=20000)
gdp_indicator = indicators.loc[
    indicators["name"].eq("GDP (current US$)"),
    "indicator_id",
].iloc[0]

gdp = fetch_data(
    "world_bank",
    indicator=gdp_indicator,
    country="ITA",
    start_year=2022,
    end_year=2023,
)
```

Source aliases are accepted for common variants such as `world-bank`,
`worldbank`, `bank_of_italy`, `openpnrr`, and `boundaries`.

The command-line interface exposes the same idea:

```bash
python3 -m italian_our_world_data sources
python3 -m italian_our_world_data info bankitalia
python3 -m italian_our_world_data indicators
python3 -m italian_our_world_data indicators world_bank -p per_page=20000 --format csv | grep "GDP (current US$)"
python3 -m italian_our_world_data discover pnrr -p params='{"page_size": 2}'
python3 -m italian_our_world_data fetch world_bank -p indicator=NY.GDP.MKTP.CD -p country=ITA --head 5
```

After installation, the console script is equivalent:

```bash
italian-our-world-data sources
```

## GeoDataFrame Support

The geospatial helpers use administrative boundary data from
[confini-amministrativi.it](https://www.confini-amministrativi.it/) by
OnData. These endpoints expose ISTAT/ANPR-derived administrative divisions
as metadata tables and GeoJSON FeatureCollections. GeoPandas is a normal
dependency of the library, so GeoDataFrame support is available after the
standard installation.

```python
import pandas as pd
from italian_our_world_data import (
    attach_administrative_boundaries,
    fetch_administrative_boundaries,
    list_administrative_boundary_divisions,
)

print(list_administrative_boundary_divisions())

regions = fetch_administrative_boundaries("regioni")
print(type(regions))  # geopandas.GeoDataFrame

data = pd.DataFrame({"region_code": ["3"], "value": [10]})
mapped = attach_administrative_boundaries(
    data,
    division="regioni",
    data_key="region_code",
    boundary_key="cod_reg",
)
```

Supported divisions are `regioni`, `comuni`,
`ripartizioni-geografiche`, and `unita-territoriali-sovracomunali`, with
English aliases for common use (`regions`, `municipalities`,
`macroregions`, and `supra-municipal-units`). The default boundary release
is `latest`; pass an explicit release such as `release="20200101"` when you
need a reproducible historical boundary layer.

Geographic joins require compatible administrative codes. For example, ISTAT
regional datasets often expose region identifiers through `ref_area`, while
the boundary layer uses `cod_reg`; municipality boundaries use `pro_com_t`
for zero-padded municipality codes.

## Availability By Provider

| Provider | Discover in Python | Identifier used by retrieval | Online discovery |
| --- | --- | --- | --- |
| ISTAT | `list_istat_dataflows()` | `dataflow_id` and ordered SDMX `key` | [ISTAT data browser](https://esploradati.istat.it/) |
| OECD | `list_oecd_dataflows()` | full SDMX dataflow reference and ordered `key` | [OECD Data Explorer](https://data-explorer.oecd.org/) |
| Eurostat | `list_eurostat_dataflows()` | `dataflow_id` and filters | [Eurostat Data Browser](https://ec.europa.eu/eurostat/databrowser/) |
| ECB | `list_ecb_dataflows()` | dataflow ID and SDMX `key` | [ECB Data Portal](https://data.ecb.europa.eu/) |
| World Bank | `list_world_bank_indicators()` | `indicator_id` | [World Bank Indicators](https://data.worldbank.org/indicator) |
| FRED | `search_fred_series("GDP", api_key=...)` | `series_id` | [FRED search](https://fred.stlouisfed.org/) |
| INPS | `list_inps_datasets()` | `dataset_id` | [INPS Open Data](https://www.inps.it/it/it/dati-e-bilanci/open-data.html) |
| OpenPNRR | `list_pnrr_resources()` | `resource` | [OpenPNRR](https://openpnrr.it/) |
| OpenCoesione | `list_opencoesione_resources()` | `resource` | [OpenCoesione API](https://opencoesione.gov.it/it/api/) |
| Bank of Italy exchange rates | `list_bankitalia_currencies()` | date and currency codes | [Bank of Italy exchange-rate portal](https://tassidicambio.bancaditalia.it/) |
| dati.gov.it / CKAN | `list_italian_open_data_datasets()` or `list_ckan_datasets(...)` | `dataset_id`, `resource_id`, or resource URL | [dati.gov.it](https://www.dati.gov.it/) |
| OpenBDAP | `list_bdap_datasets()` | `dataset_id`, `resource_id`, or resource URL | [OpenBDAP](https://bdap-opendata.rgs.mef.gov.it/) |
| Socrata portals | `list_socrata_datasets(...)` | dataset ID | [Regione Lombardia Open Data](https://www.dati.lombardia.it/) |
| Administrative boundaries | `list_administrative_boundary_divisions()` | division and code column | [confini-amministrativi.it](https://www.confini-amministrativi.it/) |

Catalogue calls for ISTAT, OECD, and Eurostat can return thousands of
dataflows. Run them intentionally and filter their returned frames locally.
Eurostat also supports inspecting a known identifier without retrieving the
whole catalogue:

```python
from italian_our_world_data import list_eurostat_dataflows

metadata = list_eurostat_dataflows("nama_10_gdp")
print(metadata)
```

FRED is a special case. A known series can be fetched without credentials
through FRED's public CSV download, but the official API endpoint that
searches available series requires a FRED API key.

## Retrieval Examples

### ISTAT

```python
from italian_our_world_data import list_istat_dataflows, fetch_istat_data

flows = list_istat_dataflows()
print(flows[flows["name"].str.contains("Employment", case=False, na=False)].head())

data = fetch_istat_data("150_915", "A.IT.....", start_period="2023", end_period="2023")
```

The ISTAT `key` is an ordered SDMX dimension selection. `.` leaves a
dimension unrestricted. Use the ISTAT browser to select dimension values and
keep queries restricted, because the public service applies rate limits.

### OECD

```python
from italian_our_world_data import list_oecd_dataflows, fetch_oecd_data

flows = list_oecd_dataflows()
print(flows.head())

data = fetch_oecd_data(
    "OECD.SDD.STES,DSD_STES@DF_FINMARK,",
    "............",
    start_period="2024-01",
    end_period="2024-01",
)
```

OECD retrieval uses the complete Data Explorer dataflow reference, not old
`stats.oecd.org` dataset codes. Its catalogue includes multiple publishing
agency IDs; use the full reference shown in Data Explorer before narrowing
the SDMX key.

### Eurostat

```python
from italian_our_world_data import fetch_eurostat_data

data = fetch_eurostat_data(
    "nama_10_gdp",
    filters={"geo": "IT", "unit": "CP_MEUR", "na_item": "B1GQ"},
    start_period="2022",
    end_period="2023",
)
```

Eurostat filters are dimension-code pairs shown in its Data Browser.

### ECB

```python
from italian_our_world_data import list_ecb_dataflows, fetch_ecb_data

print(list_ecb_dataflows().head())
data = fetch_ecb_data(
    "EXR", "D.USD.EUR.SP00.A", start_period="2023-01-02", end_period="2023-01-06"
)
```

### World Bank

```python
from italian_our_world_data import list_world_bank_indicators, fetch_world_bank_data

indicators = list_world_bank_indicators(per_page=100)
print(indicators[indicators["name"].str.contains("GDP", case=False, na=False)].head())

data = fetch_world_bank_data("NY.GDP.MKTP.CD", country="ITA", start_year=2022, end_year=2023)
```

### FRED

```python
from italian_our_world_data import fetch_fred_data, search_fred_series

# No key needed once a public series ID is known.
data = fetch_fred_data("GDP", start_period="2023-01-01", end_period="2023-12-31")

# A key is required by the official FRED series-search API.
matches = search_fred_series("Italian GDP", api_key="your_fred_api_key")
```

### INPS

```python
from italian_our_world_data import (
    fetch_inps_data,
    get_inps_dataset_metadata,
    list_inps_datasets,
)

datasets = list_inps_datasets(limit=10)
dataset_id = datasets.loc[0, "dataset_id"]
metadata = get_inps_dataset_metadata(dataset_id)
data = fetch_inps_data(dataset_id)
```

`fetch_inps_data()` selects tabular CSV or Excel resources; a dataset that
publishes only other formats raises `DataSourceError`.

### OpenPNRR

```python
from italian_our_world_data import fetch_pnrr_data, list_pnrr_resources

print(list_pnrr_resources())
data = fetch_pnrr_data("missioni")
```

Pass `fetch_all_pages=True` when you intentionally want a complete
paginated OpenPNRR resource.

### OpenCoesione

```python
from italian_our_world_data import fetch_opencoesione_data, list_opencoesione_resources

print(list_opencoesione_resources())
themes = fetch_opencoesione_data("temi", params={"page_size": 2})
```

Pass `fetch_all_pages=True` for intentionally complete API resources such as
project or subject lists.

### Bank of Italy Exchange Rates

```python
from italian_our_world_data import fetch_bankitalia_exchange_rates, list_bankitalia_currencies

currencies = list_bankitalia_currencies()
usd = fetch_bankitalia_exchange_rates(
    reference_date="2023-01-03",
    base_currency="EUR",
    target_currency="USD",
)
latest = fetch_bankitalia_exchange_rates(target_currency="USD")
```

Daily historical calls use `value`; latest calls expose the source columns
`eur_rate` and `usd_rate`.

### CKAN Catalogues

```python
from italian_our_world_data import (
    fetch_ckan_resource,
    fetch_italian_open_data_resource,
    list_bdap_datasets,
    list_ckan_datasets,
    list_italian_open_data_datasets,
)

national = list_italian_open_data_datasets(rows=5)
bdap = list_bdap_datasets(rows=5)

data = fetch_italian_open_data_resource(
    resource_url="https://example.gov.it/path/data.csv",
    resource_format="csv",
)

other_catalogue = list_ckan_datasets("https://catalogue.example.org", rows=5)
other_data = fetch_ckan_resource(
    "https://catalogue.example.org",
    dataset_id="known-dataset-id",
)
```

The generic CKAN helpers also work with portals that expose a non-root CKAN
action endpoint by passing the full `/api/3/action` URL.

### Socrata Portals

```python
from italian_our_world_data import (
    fetch_lombardy_data,
    fetch_socrata_data,
    list_lombardy_datasets,
    list_socrata_datasets,
)

lombardy = list_lombardy_datasets(limit=10)
weather = fetch_lombardy_data("y856-h426", limit=10)

other_portal = list_socrata_datasets("https://www.exampledata.org", limit=10)
other_data = fetch_socrata_data("https://www.exampledata.org", "abcd-1234", limit=100)
```

Socrata supports SoQL-style query parameters; pass them through `params=`.

## Testing The Library

Deterministic unit tests validate request construction and response parsing
without relying on changing public services:

```bash
python3 -m unittest discover -s tests -v
```

The live verification command fetches a small representative result from
every supported provider:

```bash
python3 -m italian_our_world_data.verify
```

Live tests demonstrate connectivity and current source compatibility. They
can fail temporarily when a provider is unavailable even when the unit tests
pass.

Runnable examples live under `examples/<provider>/`; SDMX-specific examples
are in `examples/sdmx/`. Automated tests live only in `tests/`; modules named
`test_*.py` there contain assertions and are run by the command above.
