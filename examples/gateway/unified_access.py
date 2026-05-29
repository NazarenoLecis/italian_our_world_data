"""Use the unified discovery and retrieval gateway."""

from italian_our_world_data import fetch_data, list_indicators, list_sources, source_info


def main() -> None:
    print(list_sources()[["source", "item_name", "identifier_column", "fetch_parameter"]].head())
    print(source_info("world_bank")["example"])

    indicators = list_indicators("world_bank", per_page=20000)
    gdp_indicator = indicators.loc[
        indicators["name"].eq("GDP (current US$)"),
        "indicator_id",
    ].iloc[0]
    print(indicators[indicators["indicator_id"].eq(gdp_indicator)])

    data = fetch_data(
        "world_bank",
        indicator=gdp_indicator,
        country="ITA",
        start_year=2022,
        end_year=2023,
    )
    print(data.head())


if __name__ == "__main__":
    main()
