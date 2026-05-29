"""List UN Population Data Portal indicators and locations."""

from italian_our_world_data import list_un_population_indicators, list_un_population_locations


def main() -> None:
    indicators = list_un_population_indicators(page_size=5)
    print(indicators[["indicator_id", "name", "short_name", "source"]].head())

    locations = list_un_population_locations(page_size=5)
    print(locations[["location_id", "location", "iso3"]].head())


if __name__ == "__main__":
    main()
