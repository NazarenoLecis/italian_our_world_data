"""ISTAT retrieval example that leaves storage to the caller."""

from italian_our_world_data import fetch_istat_data


def main(dataset_id, key="", start_period=None, end_period=None):
    """Fetch an ISTAT dataset as a DataFrame for caller-controlled storage."""
    return fetch_istat_data(
        dataset_id,
        key,
        start_period=start_period,
        end_period=end_period,
    )


if __name__ == "__main__":
    print(main("150_915", ".......", "2023", "2023").head())
