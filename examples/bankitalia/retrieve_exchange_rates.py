"""Retrieve Bank of Italy exchange rates."""

from italian_our_world_data import fetch_bankitalia_exchange_rates, list_bankitalia_currencies


def main() -> None:
    currencies = list_bankitalia_currencies()
    print(currencies.head())

    rates = fetch_bankitalia_exchange_rates(
        reference_date="2023-01-03",
        base_currency="EUR",
        target_currency="USD",
    )
    print(rates)


if __name__ == "__main__":
    main()
