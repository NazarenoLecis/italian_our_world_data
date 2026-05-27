"""ECB retrieval example using the public library API."""

from italian_our_world_data import fetch_ecb_data as _fetch_ecb_data


def fetch_ecb_data(dataset, resource_id, start_period=None, end_period=None):
    return _fetch_ecb_data(
        dataset,
        resource_id,
        start_period=start_period,
        end_period=end_period,
    )

__all__ = ["fetch_ecb_data"]


if __name__ == "__main__":
    frame = fetch_ecb_data(
        "EXR",
        "D.USD.EUR.SP00.A",
        start_period="2023-01-01",
        end_period="2023-01-31",
    )
    print(frame.head())
