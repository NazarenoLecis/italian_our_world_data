"""OpenPNRR retrieval example using the public library API."""

from italian_our_world_data import fetch_pnrr_data, list_pnrr_resources

__all__ = ["fetch_pnrr_data", "list_pnrr_resources"]


if __name__ == "__main__":
    print(fetch_pnrr_data("missioni").head())
