"""Retrieve rows from Regione Lombardia's Socrata portal."""

from italian_our_world_data import fetch_lombardy_data, list_lombardy_datasets


def main() -> None:
    datasets = list_lombardy_datasets(limit=5)
    print(datasets.head())

    weather = fetch_lombardy_data("y856-h426", limit=10)
    print(weather.head())


if __name__ == "__main__":
    main()
