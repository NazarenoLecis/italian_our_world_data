"""A minimal ISTAT retrieval example using the public library API."""

from italian_our_world_data import fetch_istat_data


def main():
    return fetch_istat_data("150_915", ".......", start_period="2023", end_period="2023")


if __name__ == "__main__":
    print(main().head())
