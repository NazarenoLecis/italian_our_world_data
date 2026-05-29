"""Retrieve BIS SDMX data through the public library API."""

from italian_our_world_data import fetch_bis_data, list_bis_dataflows


def main() -> None:
    flows = list_bis_dataflows()
    print(flows[["dataflow", "name"]].head())

    data = fetch_bis_data(
        "BIS,WS_EER,1.0",
        "M.N.B.IT",
        start_period="2023-01",
        end_period="2023-03",
    )
    print(data.head())


if __name__ == "__main__":
    main()
