# Library Usage And Data Discovery

## Public Naming Convention

The importable API follows three verbs:

| Verb | Purpose | Return value |
| --- | --- | --- |
| `fetch_<provider>_data(...)` | Retrieve observations or table rows | `pandas.DataFrame` |
| `list_<provider>_<resource>(...)` | Discover available data objects | `pandas.DataFrame` |
| `get_<provider>_<resource>_metadata(...)` | Retrieve metadata for one object | `dict` |

Provider terminology is retained because it tells you what identifier is
needed: ISTAT, OECD, Eurostat, ECB, and BIS expose SDMX **dataflows**; World
Bank, IMF, and UN Population expose **indicators**; AMECO exposes macro
**variables**; FRED exposes **series**; INPS, dati.gov.it, and OpenBDAP
expose catalogue **datasets**; OpenPNRR and OpenCoesione expose API
**resources**; the Bank of Italy Statistical Database exposes statistical
**cubes**; Socrata portals expose tabular **dataset IDs**.

`get_inps_dataset()` remains available as a compatibility alias for
`get_inps_dataset_metadata()`.

## Imports

```python
from italian_our_world_data import (
    attach_administrative_boundaries,
    discover_data,
    fetch_ameco_data,
    fetch_data,
    fetch_bankitalia_exchange_rates,
    fetch_bdap_data,
    fetch_bis_data,
    fetch_ckan_resource,
    fetch_ecb_data,
    fetch_eurostat_data,
    fetch_fred_data,
    fetch_imf_data,
    fetch_inps_data,
    fetch_italian_open_data_resource,
    fetch_istat_data,
    fetch_lombardy_data,
    fetch_oecd_data,
    fetch_opencoesione_data,
    fetch_pnrr_data,
    fetch_socrata_data,
    fetch_un_population_data,
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
    list_ameco_variables,
    list_bankitalia_bds_catalogue,
    list_bankitalia_bds_cubes,
    list_indicators,
    list_source_items,
    list_sources,
    list_bankitalia_currencies,
    list_bdap_datasets,
    list_bis_dataflows,
    list_ckan_datasets,
    list_ecb_dataflows,
    list_eurostat_dataflows,
    list_imf_countries,
    list_imf_indicators,
    list_inps_datasets,
    list_italian_open_data_datasets,
    list_istat_dataflows,
    list_lombardy_datasets,
    list_oecd_dataflows,
    list_opencoesione_resources,
    list_pnrr_resources,
    list_socrata_datasets,
    list_un_population_indicators,
    list_un_population_locations,
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
`worldbank`, `weo`, `ecfin`, `bank_of_italy`, `openpnrr`, and
`boundaries`.

The command-line interface exposes the same idea:

```bash
python3 -m italian_our_world_data sources
python3 -m italian_our_world_data info bankitalia
python3 -m italian_our_world_data indicators
python3 -m italian_our_world_data indicators ameco --format csv | grep NPTD
python3 -m italian_our_world_data indicators imf -p dataset=WEO --format csv | grep NGDP_RPCH
python3 -m italian_our_world_data indicators world_bank -p per_page=20000 --format csv | grep "GDP (current US$)"
python3 -m italian_our_world_data discover pnrr -p params='{"page_size": 2}'
python3 -m italian_our_world_data fetch ameco -p full_variable=1.0.0.0.NPTD -p countries=ITA -p years='[2022, 2023]'
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
| AMECO | `list_ameco_variables()` | `full_variable` | [AMECO database](https://economy-finance.ec.europa.eu/economic-research-and-databases/economic-databases/ameco-database_en) |
| World Bank | `list_world_bank_indicators()` | `indicator_id` | [World Bank Indicators](https://data.worldbank.org/indicator) |
| IMF DataMapper | `list_imf_indicators()` | `indicator_id` | [IMF DataMapper](https://www.imf.org/external/datamapper/) |
| UN Population Data Portal | `list_un_population_indicators()` | `indicator_id` and `location_id` | [UN Population Data Portal](https://population.un.org/dataportal/) |
| FRED | `search_fred_series("GDP", api_key=...)` | `series_id` | [FRED search](https://fred.stlouisfed.org/) |
| BIS | `list_bis_dataflows()` | dataflow reference and ordered SDMX `key` | [BIS statistics](https://stats.bis.org/) |
| INPS | `list_inps_datasets()` | `dataset_id` | [INPS Open Data](https://www.inps.it/it/it/dati-e-bilanci/open-data.html) |
| OpenPNRR | `list_pnrr_resources()` | `resource` | [OpenPNRR](https://openpnrr.it/) |
| OpenCoesione | `list_opencoesione_resources()` | `resource` | [OpenCoesione API](https://opencoesione.gov.it/it/api/) |
| Bank of Italy Statistical Database | `list_bankitalia_bds_cubes()` | `cube_id` for BDS cubes | [Bank of Italy BDS](https://a2a.bancaditalia.it/infostat/) |
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

`EXR` is only one ECB dataflow. Use `list_ecb_dataflows()` to discover the
full ECB Data Portal catalogue, then pass the chosen dataflow ID as `dataset`
with the SDMX key shown by the ECB portal for that dataset.

### AMECO

```python
from italian_our_world_data import fetch_ameco_data, list_ameco_variables

variables = list_ameco_variables()
print(variables[variables["description"].str.contains("population", case=False, na=False)].head())

data = fetch_ameco_data("1.0.0.0.NPTD", countries="ITA", years=[2022, 2023])
```

AMECO retrieval uses `full_variable`, for example `1.0.0.0.NPTD`. The
catalogue returned by `list_ameco_variables()` includes that full code and
the shorter AMECO variable mnemonic.

### World Bank

```python
from italian_our_world_data import list_world_bank_indicators, fetch_world_bank_data

indicators = list_world_bank_indicators(per_page=100)
print(indicators[indicators["name"].str.contains("GDP", case=False, na=False)].head())

data = fetch_world_bank_data("NY.GDP.MKTP.CD", country="ITA", start_year=2022, end_year=2023)
```

### IMF DataMapper

```python
from italian_our_world_data import fetch_imf_data, list_imf_indicators

indicators = list_imf_indicators(dataset="WEO")
print(indicators[indicators["name"].str.contains("GDP", case=False, na=False)].head())

data = fetch_imf_data("NGDP_RPCH", countries="ITA", periods=[2022, 2023])
```

The IMF DataMapper gateway is useful for WEO macro indicators. Use
`list_imf_indicators()` to find the `indicator_id`, then pass one or more
country codes through `countries`.

### UN Population

```python
from italian_our_world_data import list_un_population_indicators, list_un_population_locations

indicators = list_un_population_indicators(page_size=20)
locations = list_un_population_locations(page_size=20)
```

The UN Population catalogue endpoints are public. The data endpoint can
require a bearer token from the portal; pass `auth_token=` or set
`UN_POPULATION_TOKEN` when calling `fetch_un_population_data()`.

### FRED

```python
from italian_our_world_data import fetch_fred_data, search_fred_series

# No key needed once a public series ID is known.
data = fetch_fred_data("GDP", start_period="2023-01-01", end_period="2023-12-31")

# A key is required by the official FRED series-search API.
matches = search_fred_series("Italian GDP", api_key="your_fred_api_key")
```

### BIS

```python
from italian_our_world_data import fetch_bis_data, list_bis_dataflows

flows = list_bis_dataflows()
print(flows.head())

data = fetch_bis_data(
    "BIS,WS_EER,1.0",
    "M.N.B.IT",
    start_period="2023-01",
    end_period="2023-03",
)
```

BIS retrieval follows SDMX conventions: the `dataflow` identifies the series
family and `key` is the ordered dimension key.

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

### Bank of Italy Statistical Database

```python
from italian_our_world_data import list_bankitalia_bds_catalogue, list_bankitalia_bds_cubes

taxonomy = list_bankitalia_bds_catalogue(max_depth=2)
print(taxonomy[["local_id", "name", "node_type", "path"]].head())

cubes = list_bankitalia_bds_cubes(max_depth=3, limit=20)
print(cubes[["cube_id", "local_id", "name", "last_update"]].head())
```

The BDS catalogue is much broader than exchange rates. It includes Bank of
Italy statistical cubes for interest rates, money and banking, public finance,
external accounts, and other published series. Use `cube_id` from the returned
frame as the stable identifier for a cube. Add `query="debito"` or another
search term when you want to filter the traversal.

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
