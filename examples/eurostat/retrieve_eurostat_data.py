"""Eurostat retrieval example using the public library API."""

from italian_our_world_data import fetch_eurostat_data as _fetch_eurostat_data


def fetch_eurostat_data(dataset, start_date=None, end_date=None, filters=None):
    """Call Eurostat using the historical date argument names."""
    return _fetch_eurostat_data(
        dataset,
        filters=filters,
        start_period=start_date,
        end_period=end_date,
    )

__all__ = ["fetch_eurostat_data"]


if __name__ == "__main__":
    frame = fetch_eurostat_data(
        "nama_10_gdp",
        filters={"geo": "IT", "unit": "CP_MEUR", "na_item": "B1GQ"},
        start_date="2022",
        end_date="2023",
    )
    print(frame)
