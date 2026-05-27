"""INPS dataset listing example."""

from italian_our_world_data import get_inps_dataset_metadata, list_inps_datasets

__all__ = ["get_inps_dataset_metadata", "list_inps_datasets"]


if __name__ == "__main__":
    print(list_inps_datasets(limit=50))
