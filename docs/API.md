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
exposes **indicators**; FRED exposes **series**; INPS exposes **datasets**;
OpenPNRR exposes API **resources**.

`get_inps_dataset()` remains available as a compatibility alias for
`get_inps_dataset_metadata()`.

## Imports

```python
from italian_our_world_data import (
    attach_administrative_boundaries,
    fetch_ecb_data,
    fetch_eurostat_data,
    fetch_fred_data,
    fetch_inps_data,
    fetch_istat_data,
    fetch_oecd_data,
    fetch_pnrr_data,
    fetch_world_bank_data,
    fetch_administrative_boundaries,
    fetch_administrative_boundary_metadata,
    get_inps_dataset_metadata,
    list_administrative_boundary_divisions,
    list_ecb_dataflows,
    list_eurostat_dataflows,
    list_inps_datasets,
    list_istat_dataflows,
    list_oecd_dataflows,
    list_pnrr_resources,
    list_world_bank_indicators,
    search_fred_series,
)
```

Retrieval functions return a `DataFrame`. Observation-oriented responses use
`time_period` for the period identifier and `value` for numeric observations.
`time_period` is intentionally a string because sources publish annual,
quarterly, monthly, and daily frequencies.

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
currently used by the API helper is `20200101`.

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
