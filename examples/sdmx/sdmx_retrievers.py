"""Index of retrieval functions for SDMX-capable providers."""

from italian_our_world_data import (
    fetch_ecb_data,
    fetch_eurostat_data,
    fetch_istat_data,
    fetch_oecd_data,
    fetch_world_bank_data,
)

SDMX_RETRIEVERS = {
    "ISTAT": fetch_istat_data,
    "OECD": fetch_oecd_data,
    "EUROSTAT": fetch_eurostat_data,
    "ECB": fetch_ecb_data,
    "WORLD_BANK": fetch_world_bank_data,
}
