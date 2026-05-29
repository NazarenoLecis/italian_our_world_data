"""Retrieve a tabular resource from the national dati.gov.it CKAN catalogue."""

from italian_our_world_data import (
    fetch_italian_open_data_resource,
    list_italian_open_data_datasets,
)


def main() -> None:
    datasets = list_italian_open_data_datasets(rows=5)
    print(datasets.head())

    turnout = fetch_italian_open_data_resource(
        resource_url=(
            "https://elezionibarcellonapozzodigotto.risele.it/web2605/"
            "comunali/AfflCOM_83005.csv"
        ),
        resource_format="csv",
    )
    print(turnout.head())


if __name__ == "__main__":
    main()
