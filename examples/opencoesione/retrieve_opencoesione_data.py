"""Retrieve OpenCoesione API data."""

from italian_our_world_data import fetch_opencoesione_data, list_opencoesione_resources


def main() -> None:
    print(list_opencoesione_resources())

    themes = fetch_opencoesione_data("temi", params={"page_size": 2})
    print(themes.head())


if __name__ == "__main__":
    main()
