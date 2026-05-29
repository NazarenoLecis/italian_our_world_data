"""Retrieve IMF DataMapper data through the public library API."""

from italian_our_world_data import fetch_imf_data, list_imf_indicators


def main() -> None:
    indicators = list_imf_indicators(dataset="WEO")
    print(indicators[["indicator_id", "name", "unit", "dataset"]].head())

    data = fetch_imf_data("NGDP_RPCH", countries="ITA", periods=[2022, 2023])
    print(data.head())


if __name__ == "__main__":
    main()
