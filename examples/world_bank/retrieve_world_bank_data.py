"""World Bank retrieval example using the public library API."""

from italian_our_world_data import fetch_world_bank_data as _fetch_world_bank_data


def fetch_world_bank_data(indicator, country="all", start_year=None, end_year=None):
    return _fetch_world_bank_data(
        indicator,
        country=country,
        start_year=start_year,
        end_year=end_year,
    )

__all__ = ["fetch_world_bank_data"]


if __name__ == "__main__":
    frame = fetch_world_bank_data(
        "NY.GDP.MKTP.CD",
        country="ITA",
        start_year=2020,
        end_year=2023,
    )
    print(frame)
