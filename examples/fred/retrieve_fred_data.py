"""FRED retrieval example using the public library API."""

from italian_our_world_data import fetch_fred_data as _fetch_fred_data


def fetch_fred_data(series_id, api_key=None, start_date=None, end_date=None):
    return _fetch_fred_data(
        series_id,
        api_key,
        start_period=start_date,
        end_period=end_date,
    )

__all__ = ["fetch_fred_data"]


if __name__ == "__main__":
    print(fetch_fred_data("GDP", start_date="2023-01-01", end_date="2023-12-31"))
