"""ISTAT SDMX retrieval example using the public library API."""

from italian_our_world_data import fetch_istat_data

__all__ = ["fetch_istat_data"]


if __name__ == "__main__":
    frame = fetch_istat_data("150_915", "A.IT.....", start_period="2023", end_period="2023")
    print(frame.head())
