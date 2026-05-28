"""Easy DataFrame access to public data sources relevant to Italy."""

from ._common import DataSourceError
from .sources import (
    fetch_ecb_data,
    fetch_eurostat_data,
    fetch_fred_data,
    fetch_inps_data,
    fetch_istat_data,
    fetch_oecd_data,
    fetch_pnrr_data,
    fetch_world_bank_data,
    get_inps_dataset,
    get_inps_dataset_metadata,
    list_ecb_dataflows,
    list_eurostat_dataflows,
    list_inps_datasets,
    list_istat_dataflows,
    list_oecd_dataflows,
    list_pnrr_resources,
    list_world_bank_indicators,
    search_fred_series,
)

_GEO_EXPORTS = {
    "attach_administrative_boundaries",
    "fetch_administrative_boundaries",
    "fetch_administrative_boundary_metadata",
    "list_administrative_boundary_divisions",
}

__all__ = [
    "DataSourceError",
    "fetch_ecb_data",
    "fetch_eurostat_data",
    "fetch_fred_data",
    "fetch_inps_data",
    "fetch_istat_data",
    "fetch_oecd_data",
    "fetch_pnrr_data",
    "fetch_world_bank_data",
    "attach_administrative_boundaries",
    "fetch_administrative_boundaries",
    "fetch_administrative_boundary_metadata",
    "get_inps_dataset",
    "get_inps_dataset_metadata",
    "list_administrative_boundary_divisions",
    "list_ecb_dataflows",
    "list_eurostat_dataflows",
    "list_inps_datasets",
    "list_istat_dataflows",
    "list_oecd_dataflows",
    "list_pnrr_resources",
    "list_world_bank_indicators",
    "search_fred_series",
]

__version__ = "0.1.0"


def __getattr__(name):
    if name in _GEO_EXPORTS:
        from . import geo

        return getattr(geo, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
