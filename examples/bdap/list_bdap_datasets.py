"""List OpenBDAP public-finance datasets."""

from italian_our_world_data import list_bdap_datasets


def main() -> None:
    datasets = list_bdap_datasets(rows=5)
    print(datasets.head())


if __name__ == "__main__":
    main()
