"""OECD retrieval example using the public library API."""

from italian_our_world_data import fetch_oecd_data as _fetch_oecd_data


def fetch_oecd_data(dataset, filter_conditions="", start_date=None, end_date=None):
    """Call the current OECD SDMX API using the historical argument names."""
    return _fetch_oecd_data(
        dataset,
        filter_conditions,
        start_period=start_date,
        end_period=end_date,
    )

__all__ = ["fetch_oecd_data"]


if __name__ == "__main__":
    frame = fetch_oecd_data(
        "OECD.SDD.STES,DSD_STES@DF_FINMARK,",
        "............",
        start_date="2024",
        end_date="2024",
    )
    print(frame.head())
