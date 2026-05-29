"""Unified source discovery and retrieval helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Optional

import pandas as pd

from ._common import DataSourceError
from .geo import fetch_administrative_boundaries, list_administrative_boundary_divisions
from .sources import (
    fetch_ameco_data,
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
    list_ameco_variables,
    list_bankitalia_bds_cubes,
    list_bankitalia_currencies,
    list_bdap_datasets,
    list_bis_dataflows,
    list_ckan_datasets,
    list_ecb_dataflows,
    list_eurostat_dataflows,
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
    list_world_bank_indicators,
    search_fred_series,
)


ProviderFunction = Callable[..., pd.DataFrame]


@dataclass(frozen=True)
class SourceSpec:
    """Static description of one supported source."""

    source: str
    name: str
    category: str
    description: str
    fetch: Optional[ProviderFunction]
    discovery: Optional[ProviderFunction]
    item_name: str
    identifier_column: str
    fetch_parameter: str
    required: tuple[str, ...]
    optional: tuple[str, ...]
    discovery_required: tuple[str, ...]
    discovery_optional: tuple[str, ...]
    returns: str
    example: str
    aliases: tuple[str, ...] = ()

    @property
    def fetch_function(self) -> Optional[str]:
        return self.fetch.__name__ if self.fetch is not None else None

    @property
    def discovery_function(self) -> Optional[str]:
        return self.discovery.__name__ if self.discovery is not None else None


def _normalise_source(source: str) -> str:
    return source.strip().lower().replace("-", "_").replace(" ", "_")


SOURCE_SPECS: tuple[SourceSpec, ...] = (
    SourceSpec(
        source="istat",
        name="ISTAT",
        category="sdmx",
        description="Italian national statistics dataflows from ISTAT.",
        fetch=fetch_istat_data,
        discovery=list_istat_dataflows,
        item_name="dataflow",
        identifier_column="dataflow_id",
        fetch_parameter="dataflow_id",
        required=("dataflow_id",),
        optional=("key", "start_period", "end_period", "params"),
        discovery_required=(),
        discovery_optional=(),
        returns="Observation DataFrame with time_period and value when present.",
        example='fetch_data("istat", dataflow_id="150_915", key="A.IT.....", start_period="2023")',
    ),
    SourceSpec(
        source="oecd",
        name="OECD Data Explorer",
        category="sdmx",
        description="International OECD SDMX dataflows.",
        fetch=fetch_oecd_data,
        discovery=list_oecd_dataflows,
        item_name="dataflow",
        identifier_column="dataflow_id",
        fetch_parameter="dataflow",
        required=("dataflow",),
        optional=("key", "start_period", "end_period", "params"),
        discovery_required=(),
        discovery_optional=("agency_id",),
        returns="Observation DataFrame with time_period and value when present.",
        example=(
            'fetch_data("oecd", dataflow="OECD.SDD.STES,DSD_STES@DF_FINMARK,", '
            'key="............", start_period="2024-01")'
        ),
    ),
    SourceSpec(
        source="eurostat",
        name="Eurostat",
        category="jsonstat",
        description="Eurostat JSON-stat dissemination datasets.",
        fetch=fetch_eurostat_data,
        discovery=list_eurostat_dataflows,
        item_name="dataset",
        identifier_column="dataflow_id",
        fetch_parameter="dataset",
        required=("dataset",),
        optional=("filters", "start_period", "end_period", "params"),
        discovery_required=(),
        discovery_optional=("dataflow_id",),
        returns="Observation DataFrame decoded from JSON-stat.",
        example=(
            'fetch_data("eurostat", dataset="nama_10_gdp", '
            'filters={"geo": "IT", "unit": "CP_MEUR", "na_item": "B1GQ"})'
        ),
    ),
    SourceSpec(
        source="ecb",
        name="European Central Bank",
        category="sdmx",
        description="ECB Data Portal SDMX dataflows.",
        fetch=fetch_ecb_data,
        discovery=list_ecb_dataflows,
        item_name="dataset",
        identifier_column="dataflow_id",
        fetch_parameter="dataset",
        required=("dataset",),
        optional=("key", "start_period", "end_period", "params"),
        discovery_required=(),
        discovery_optional=(),
        returns="Observation DataFrame with time_period and value when present.",
        example='fetch_data("ecb", dataset="EXR", key="D.USD.EUR.SP00.A")',
    ),
    SourceSpec(
        source="ameco",
        name="AMECO",
        category="macro",
        description="European Commission DG ECFIN annual macro-economic variables.",
        fetch=fetch_ameco_data,
        discovery=list_ameco_variables,
        item_name="variable",
        identifier_column="full_variable",
        fetch_parameter="full_variable",
        required=("full_variable",),
        optional=("countries", "years", "last_year", "year_order", "params"),
        discovery_required=(),
        discovery_optional=(),
        returns="Annual AMECO observations with country, indicator, unit, time_period, and value.",
        example='fetch_data("ameco", full_variable="1.0.0.0.NPTD", countries="ITA", years=[2022, 2023])',
        aliases=("ecfin", "dg_ecfin"),
    ),
    SourceSpec(
        source="world_bank",
        name="World Bank",
        category="indicator",
        description="World Bank indicators; Italy is the default country.",
        fetch=fetch_world_bank_data,
        discovery=list_world_bank_indicators,
        item_name="indicator",
        identifier_column="indicator_id",
        fetch_parameter="indicator",
        required=("indicator",),
        optional=("country", "start_year", "end_year", "fetch_all_pages"),
        discovery_required=(),
        discovery_optional=("page", "per_page"),
        returns="Indicator DataFrame with country, time_period, and value.",
        example='fetch_data("world_bank", indicator="NY.GDP.MKTP.CD", country="ITA")',
        aliases=("worldbank", "wb"),
    ),
    SourceSpec(
        source="imf",
        name="IMF DataMapper",
        category="indicator",
        description="IMF DataMapper indicators, including WEO macro-economic series.",
        fetch=fetch_imf_data,
        discovery=list_imf_indicators,
        item_name="indicator",
        identifier_column="indicator_id",
        fetch_parameter="indicator",
        required=("indicator",),
        optional=("countries", "periods"),
        discovery_required=(),
        discovery_optional=("dataset",),
        returns="Indicator DataFrame with country_id, time_period, and value.",
        example='fetch_data("imf", indicator="NGDP_RPCH", countries="ITA", periods=[2022, 2023])',
        aliases=("imf_datamapper", "weo"),
    ),
    SourceSpec(
        source="un_population",
        name="UN Population Data Portal",
        category="demography",
        description="UN DESA Population Division indicator catalogue and data endpoint.",
        fetch=fetch_un_population_data,
        discovery=list_un_population_indicators,
        item_name="indicator",
        identifier_column="indicator_id",
        fetch_parameter="indicator_id",
        required=("indicator_id",),
        optional=("location_id", "start_year", "end_year", "auth_token", "params"),
        discovery_required=(),
        discovery_optional=("page_size", "page_number", "fetch_all_pages"),
        returns="UN population rows with time_period and value when the data endpoint is authorised.",
        example='fetch_data("un_population", indicator_id=46, location_id=380, start_year=2020, end_year=2023, auth_token="...")',
        aliases=("undesa", "wpp", "un_wpp"),
    ),
    SourceSpec(
        source="fred",
        name="FRED",
        category="series",
        description="FRED series observations; known series can use the public CSV endpoint.",
        fetch=fetch_fred_data,
        discovery=search_fred_series,
        item_name="series",
        identifier_column="series_id",
        fetch_parameter="series_id",
        required=("series_id",),
        optional=("api_key", "start_period", "end_period"),
        discovery_required=("search_text",),
        discovery_optional=("api_key", "limit"),
        returns="Series DataFrame with time_period and value.",
        example='fetch_data("fred", series_id="GDP", start_period="2023-01-01")',
    ),
    SourceSpec(
        source="bis",
        name="Bank for International Settlements",
        category="sdmx",
        description="BIS SDMX dataflows, useful for exchange rates and financial statistics.",
        fetch=fetch_bis_data,
        discovery=list_bis_dataflows,
        item_name="dataflow",
        identifier_column="dataflow",
        fetch_parameter="dataflow",
        required=("dataflow",),
        optional=("key", "start_period", "end_period", "params"),
        discovery_required=(),
        discovery_optional=("provider",),
        returns="Observation DataFrame with time_period and value when present.",
        example='fetch_data("bis", dataflow="BIS,WS_EER,1.0", key="M.N.B.IT", start_period="2023-01")',
    ),
    SourceSpec(
        source="inps",
        name="INPS Open Data",
        category="ckan_like",
        description="INPS dataset catalogue and downloadable CSV/Excel resources.",
        fetch=fetch_inps_data,
        discovery=list_inps_datasets,
        item_name="dataset",
        identifier_column="dataset_id",
        fetch_parameter="dataset_id",
        required=("dataset_id",),
        optional=("resource_index",),
        discovery_required=(),
        discovery_optional=("limit", "offset"),
        returns="Downloaded tabular resource as a DataFrame.",
        example='fetch_data("inps", dataset_id="known-dataset-id")',
    ),
    SourceSpec(
        source="pnrr",
        name="OpenPNRR",
        category="api_resource",
        description="OpenPNRR public API resources such as missioni.",
        fetch=fetch_pnrr_data,
        discovery=list_pnrr_resources,
        item_name="resource",
        identifier_column="resource",
        fetch_parameter="resource",
        required=("resource",),
        optional=("params", "fetch_all_pages"),
        discovery_required=(),
        discovery_optional=(),
        returns="JSON-normalized API resource rows.",
        example='fetch_data("pnrr", resource="missioni", params={"page_size": 2})',
        aliases=("openpnrr",),
    ),
    SourceSpec(
        source="opencoesione",
        name="OpenCoesione",
        category="api_resource",
        description="OpenCoesione API resources for cohesion-policy projects and metadata.",
        fetch=fetch_opencoesione_data,
        discovery=list_opencoesione_resources,
        item_name="resource",
        identifier_column="resource",
        fetch_parameter="resource",
        required=("resource",),
        optional=("params", "fetch_all_pages"),
        discovery_required=(),
        discovery_optional=(),
        returns="JSON-normalized API resource rows.",
        example='fetch_data("opencoesione", resource="temi", params={"page_size": 2})',
        aliases=("open_coesione",),
    ),
    SourceSpec(
        source="bankitalia",
        name="Bank of Italy Statistical Database",
        category="statistical_database",
        description=(
            "Bank of Italy BDS taxonomy and statistical cube catalogue, including "
            "rates, banking, public finance, and external accounts."
        ),
        fetch=None,
        discovery=list_bankitalia_bds_cubes,
        item_name="cube",
        identifier_column="cube_id",
        fetch_parameter="cube_id",
        required=(),
        optional=(),
        discovery_required=(),
        discovery_optional=("max_depth", "query", "limit"),
        returns="Catalogue rows for BDS statistical cubes and their metadata.",
        example='list_indicators("bankitalia", max_depth=3, limit=20)',
        aliases=("bank_of_italy", "bancaditalia", "banca_ditalia", "bds"),
    ),
    SourceSpec(
        source="bankitalia_exchange_rates",
        name="Bank of Italy exchange rates",
        category="exchange_rate",
        description="Bank of Italy exchange-rate portal daily and latest rates.",
        fetch=fetch_bankitalia_exchange_rates,
        discovery=list_bankitalia_currencies,
        item_name="currency",
        identifier_column="currency_code",
        fetch_parameter="base_currency or target_currency",
        required=(),
        optional=("reference_date", "base_currency", "target_currency", "latest", "lang"),
        discovery_required=(),
        discovery_optional=("lang",),
        returns="Exchange-rate rows with time_period/value or eur_rate/usd_rate.",
        example=(
            'fetch_data("bankitalia_exchange_rates", reference_date="2023-01-03", '
            'base_currency="EUR", target_currency="USD")'
        ),
        aliases=("bankitalia_fx", "bank_of_italy_fx", "bancaditalia_fx"),
    ),
    SourceSpec(
        source="italian_open_data",
        name="dati.gov.it",
        category="ckan",
        description="National CKAN catalogue harvested from Italian public administrations.",
        fetch=fetch_italian_open_data_resource,
        discovery=list_italian_open_data_datasets,
        item_name="dataset",
        identifier_column="dataset_id",
        fetch_parameter="dataset_id, resource_id, or resource_url",
        required=(),
        optional=("dataset_id", "resource_id", "resource_index", "resource_url", "resource_format"),
        discovery_required=(),
        discovery_optional=("query", "rows", "start", "params"),
        returns="Downloaded CKAN resource as a DataFrame.",
        example='fetch_data("italian_open_data", resource_url="https://example.gov.it/data.csv")',
        aliases=("dati_gov_it", "dati.gov.it", "national_open_data"),
    ),
    SourceSpec(
        source="bdap",
        name="OpenBDAP",
        category="ckan",
        description="RGS/MEF OpenBDAP public-finance CKAN catalogue.",
        fetch=fetch_bdap_data,
        discovery=list_bdap_datasets,
        item_name="dataset",
        identifier_column="dataset_id",
        fetch_parameter="dataset_id, resource_id, or resource_url",
        required=(),
        optional=(
            "dataset_id",
            "resource_id",
            "resource_index",
            "resource_url",
            "resource_format",
            "datastore",
            "limit",
            "offset",
        ),
        discovery_required=(),
        discovery_optional=("query", "rows", "start", "params"),
        returns="Downloaded OpenBDAP resource as a DataFrame.",
        example='fetch_data("bdap", dataset_id="known-dataset-id")',
        aliases=("openbdap",),
    ),
    SourceSpec(
        source="lombardy",
        name="Regione Lombardia Open Data",
        category="socrata",
        description="Regione Lombardia Socrata/SODA datasets.",
        fetch=fetch_lombardy_data,
        discovery=list_lombardy_datasets,
        item_name="dataset",
        identifier_column="dataset_id",
        fetch_parameter="dataset_id",
        required=("dataset_id",),
        optional=("limit", "offset", "params"),
        discovery_required=(),
        discovery_optional=("limit", "offset", "params"),
        returns="Socrata resource rows as a DataFrame.",
        example='fetch_data("lombardy", dataset_id="y856-h426", limit=10)',
        aliases=("regione_lombardia", "lombardia"),
    ),
    SourceSpec(
        source="ckan",
        name="Generic CKAN",
        category="ckan",
        description="Any CKAN-compatible catalogue exposing /api/3/action.",
        fetch=fetch_ckan_resource,
        discovery=list_ckan_datasets,
        item_name="dataset",
        identifier_column="dataset_id",
        fetch_parameter="dataset_id, resource_id, or resource_url",
        required=("base_url",),
        optional=("dataset_id", "resource_id", "resource_url", "query", "rows", "start"),
        discovery_required=("base_url",),
        discovery_optional=("query", "rows", "start", "params"),
        returns="CKAN catalogue results or downloaded resource rows.",
        example='fetch_data("ckan", base_url="https://catalogue.example.org", dataset_id="id")',
    ),
    SourceSpec(
        source="socrata",
        name="Generic Socrata",
        category="socrata",
        description="Any Socrata/SODA portal exposing /resource/{dataset_id}.json.",
        fetch=fetch_socrata_data,
        discovery=list_socrata_datasets,
        item_name="dataset",
        identifier_column="dataset_id",
        fetch_parameter="dataset_id",
        required=("domain", "dataset_id"),
        optional=("limit", "offset", "params"),
        discovery_required=("domain",),
        discovery_optional=("limit", "offset", "params"),
        returns="Socrata resource rows as a DataFrame.",
        example='fetch_data("socrata", domain="https://www.exampledata.org", dataset_id="abcd-1234")',
    ),
    SourceSpec(
        source="administrative_boundaries",
        name="Italian administrative boundaries",
        category="geospatial",
        description="GeoDataFrames for Italian administrative boundary divisions.",
        fetch=fetch_administrative_boundaries,
        discovery=list_administrative_boundary_divisions,
        item_name="division",
        identifier_column="division",
        fetch_parameter="division",
        required=("division",),
        optional=("release",),
        discovery_required=(),
        discovery_optional=(),
        returns="GeoPandas GeoDataFrame with EPSG:4326 geometries.",
        example='fetch_data("administrative_boundaries", division="regioni")',
        aliases=("boundaries", "geo"),
    ),
)


_SOURCE_LOOKUP: dict[str, SourceSpec] = {}
for _spec in SOURCE_SPECS:
    _SOURCE_LOOKUP[_normalise_source(_spec.source)] = _spec
    for _alias in _spec.aliases:
        _SOURCE_LOOKUP[_normalise_source(_alias)] = _spec


def _source_spec(source: str) -> SourceSpec:
    try:
        return _SOURCE_LOOKUP[_normalise_source(source)]
    except KeyError as exc:
        choices = ", ".join(spec.source for spec in SOURCE_SPECS)
        raise ValueError(f"Unknown source {source!r}. Available sources: {choices}") from exc


def list_sources(*, category: Optional[str] = None) -> pd.DataFrame:
    """List supported sources and their unified gateway functions."""
    rows = []
    normalised_category = _normalise_source(category) if category else None
    for spec in SOURCE_SPECS:
        if normalised_category and _normalise_source(spec.category) != normalised_category:
            continue
        rows.append(
            {
                "source": spec.source,
                "name": spec.name,
                "category": spec.category,
                "description": spec.description,
                "item_name": spec.item_name,
                "identifier_column": spec.identifier_column,
                "fetch_parameter": spec.fetch_parameter,
                "fetch_function": spec.fetch_function,
                "discovery_function": spec.discovery_function,
                "required": ", ".join(spec.required),
                "discovery_required": ", ".join(spec.discovery_required),
            }
        )
    return pd.DataFrame(rows)


def source_info(source: Optional[str] = None) -> Any:
    """Return source help.

    With no ``source``, returns the same summary table as ``list_sources()``.
    With a source name or alias, returns a dictionary describing required
    parameters, optional parameters, discovery support, and example usage.
    """
    if source is None:
        return list_sources()
    spec = _source_spec(source)
    return {
        "source": spec.source,
        "name": spec.name,
        "category": spec.category,
        "description": spec.description,
        "item_name": spec.item_name,
        "identifier_column": spec.identifier_column,
        "fetch_parameter": spec.fetch_parameter,
        "fetch_function": spec.fetch_function,
        "discovery_function": spec.discovery_function,
        "required": list(spec.required),
        "optional": list(spec.optional),
        "discovery_required": list(spec.discovery_required),
        "discovery_optional": list(spec.discovery_optional),
        "returns": spec.returns,
        "example": spec.example,
        "aliases": list(spec.aliases),
    }


def get_source_info(source: Optional[str] = None) -> Any:
    """Compatibility alias for :func:`source_info`."""
    return source_info(source)


def discover_data(source: str, /, *args: Any, **kwargs: Any) -> pd.DataFrame:
    """Run the discovery/listing function for one source."""
    spec = _source_spec(source)
    if spec.discovery is None:
        raise DataSourceError(f"Source {spec.source!r} does not expose a discovery function")
    missing = [
        name
        for index, name in enumerate(spec.discovery_required)
        if len(args) <= index and name not in kwargs
    ]
    if missing:
        raise ValueError(
            f"Source {spec.source!r} discovery requires: {', '.join(missing)}"
        )
    return spec.discovery(*args, **kwargs)


def list_source_items(source: Optional[str] = None, /, *args: Any, **kwargs: Any) -> pd.DataFrame:
    """List extractable item identifiers.

    With no ``source``, returns one row per source explaining which catalogue
    column contains the identifier to pass to ``fetch_data``. With ``source``,
    calls that source's discovery function and returns the available items.
    """
    if source is None:
        rows = []
        for spec in SOURCE_SPECS:
            rows.append(
                {
                    "source": spec.source,
                    "item_name": spec.item_name,
                    "identifier_column": spec.identifier_column,
                    "fetch_parameter": spec.fetch_parameter,
                    "discovery_function": spec.discovery_function,
                    "discovery_required": ", ".join(spec.discovery_required),
                    "discovery_optional": ", ".join(spec.discovery_optional),
                }
            )
        return pd.DataFrame(rows)

    spec = _source_spec(source)
    frame = discover_data(source, *args, **kwargs)
    if spec.identifier_column in frame.columns:
        first_columns = [spec.identifier_column]
        remaining = [column for column in frame.columns if column not in first_columns]
        frame = frame[first_columns + remaining]
    return frame


def list_indicators(source: Optional[str] = None, /, *args: Any, **kwargs: Any) -> pd.DataFrame:
    """Compatibility name for :func:`list_source_items`.

    Many sources call their extractable objects indicators, while others call
    them dataflows, datasets, resources, series, currencies, or divisions.
    """
    return list_source_items(source, *args, **kwargs)


def fetch_data(source: str, /, *args: Any, **kwargs: Any) -> pd.DataFrame:
    """Fetch rows from any supported source using a unified entry point."""
    spec = _source_spec(source)
    if spec.fetch is None:
        raise DataSourceError(f"Source {spec.source!r} does not expose a fetch function")
    return spec.fetch(*args, **kwargs)
