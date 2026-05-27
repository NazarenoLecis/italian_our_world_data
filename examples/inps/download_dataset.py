"""INPS tabular resource retrieval example."""

from italian_our_world_data import fetch_inps_data, get_inps_dataset_metadata

__all__ = ["fetch_inps_data", "get_inps_dataset_metadata"]


if __name__ == "__main__":
    print(fetch_inps_data("82").head())
