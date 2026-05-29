"""Run bounded live checks against the supported public providers."""

from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import Callable, Optional

import pandas as pd

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from italian_our_world_data import (
    DataSourceError,
    fetch_ameco_data,
    fetch_bankitalia_exchange_rates,
    fetch_bis_data,
    fetch_ecb_data,
    fetch_eurostat_data,
    fetch_fred_data,
    fetch_imf_data,
    fetch_administrative_boundaries,
    fetch_italian_open_data_resource,
    fetch_istat_data,
    fetch_lombardy_data,
    fetch_oecd_data,
    fetch_opencoesione_data,
    fetch_pnrr_data,
    fetch_world_bank_data,
    list_bdap_datasets,
    list_inps_datasets,
    list_un_population_indicators,
)


def _retrieve_with_retries(
    retrieve: Callable[[], pd.DataFrame],
    *,
    attempts: int = 3,
    delay_seconds: float = 1.0,
) -> pd.DataFrame:
    """Retry transient provider failures during live verification."""
    last_error: Optional[Exception] = None
    for attempt in range(1, attempts + 1):
        try:
            return retrieve()
        except DataSourceError as exc:
            last_error = exc
            if attempt == attempts:
                break
            time.sleep(delay_seconds)
    assert last_error is not None
    raise last_error


def _checks() -> list[tuple[str, Callable[[], pd.DataFrame]]]:
    return [
        (
            "ISTAT",
            lambda: fetch_istat_data("150_915", "A.IT.....", start_period="2023", end_period="2023"),
        ),
        (
            "OECD",
            lambda: fetch_oecd_data(
                "OECD.SDD.STES,DSD_STES@DF_FINMARK,",
                "............",
                start_period="2024-01",
                end_period="2024-01",
            ),
        ),
        (
            "Eurostat",
            lambda: fetch_eurostat_data(
                "nama_10_gdp",
                filters={"geo": "IT", "unit": "CP_MEUR", "na_item": "B1GQ"},
                start_period="2022",
                end_period="2023",
            ),
        ),
        (
            "ECB",
            lambda: fetch_ecb_data(
                "EXR",
                "D.USD.EUR.SP00.A",
                start_period="2023-01-02",
                end_period="2023-01-06",
            ),
        ),
        (
            "World Bank",
            lambda: fetch_world_bank_data(
                "NY.GDP.MKTP.CD", country="ITA", start_year=2022, end_year=2023
            ),
        ),
        (
            "AMECO",
            lambda: fetch_ameco_data("1.0.0.0.NPTD", countries="ITA", years=[2022, 2023]),
        ),
        (
            "IMF DataMapper",
            lambda: fetch_imf_data("NGDP_RPCH", countries="ITA", periods=[2022, 2023]),
        ),
        (
            "UN Population catalogue",
            lambda: list_un_population_indicators(page_size=2),
        ),
        (
            "BIS",
            lambda: fetch_bis_data(
                "BIS,WS_EER,1.0",
                "M.N.B.IT",
                start_period="2023-01",
                end_period="2023-03",
            ),
        ),
        (
            "FRED (public CSV, no key)",
            lambda: fetch_fred_data("GDP", start_period="2023-01-01", end_period="2023-12-31"),
        ),
        ("INPS catalogue", lambda: list_inps_datasets(limit=2)),
        ("OpenPNRR", lambda: fetch_pnrr_data("missioni", params={"page_size": 2})),
        (
            "Bank of Italy exchange rates",
            lambda: fetch_bankitalia_exchange_rates(
                reference_date="2023-01-03",
                base_currency="EUR",
                target_currency="USD",
            ),
        ),
        (
            "dati.gov.it CSV resource",
            lambda: fetch_italian_open_data_resource(
                resource_url=(
                    "https://elezionibarcellonapozzodigotto.risele.it/web2605/"
                    "comunali/AfflCOM_83005.csv"
                ),
                resource_format="csv",
            ),
        ),
        ("OpenBDAP catalogue", lambda: list_bdap_datasets(rows=1)),
        ("Regione Lombardia Socrata", lambda: fetch_lombardy_data("y856-h426", limit=2)),
        (
            "OpenCoesione",
            lambda: fetch_opencoesione_data("temi", params={"page_size": 2}),
        ),
    ]


def main() -> int:
    """Print one line per live provider and return a shell-friendly status."""
    failures = 0
    print("Live provider verification")
    for name, retrieve in _checks():
        try:
            frame = _retrieve_with_retries(retrieve)
            if frame.empty:
                raise RuntimeError("empty DataFrame returned")
            print(f"PASS  {name:<25} rows={len(frame):>5} columns={len(frame.columns):>2}")
        except Exception as exc:
            failures += 1
            print(f"FAIL  {name:<25} {type(exc).__name__}: {exc}")
    try:
        frame = fetch_administrative_boundaries("regioni")
        if frame.empty:
            raise RuntimeError("empty GeoDataFrame returned")
        print(f"PASS  {'Geo boundaries':<25} rows={len(frame):>5} columns={len(frame.columns):>2}")
    except Exception as exc:
        failures += 1
        print(f"FAIL  {'Geo boundaries':<25} {type(exc).__name__}: {exc}")
    if failures:
        print(f"\n{failures} provider check(s) failed.")
        return 1
    print("\nAll live provider checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
