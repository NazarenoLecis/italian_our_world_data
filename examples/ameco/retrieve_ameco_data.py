"""Retrieve AMECO data through the public library API."""

from italian_our_world_data import fetch_ameco_data, list_ameco_variables


def main() -> None:
    variables = list_ameco_variables()
    print(variables[["full_variable", "variable", "description"]].head())

    data = fetch_ameco_data("1.0.0.0.NPTD", countries="ITA", years=[2022, 2023])
    print(data.head())


if __name__ == "__main__":
    main()
