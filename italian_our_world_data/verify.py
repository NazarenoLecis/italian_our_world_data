"""Run bounded live checks against the supported public providers."""

from __future__ import annotations

from typing import Callable

import pandas as pd

from . import (
    fetch_ecb_data,
    fetch_eurostat_data,
    fetch_fred_data,
    fetch_istat_data,
    fetch_oecd_data,
    fetch_pnrr_data,
    fetch_world_bank_data,
    list_inps_datasets,
)


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
            "FRED (public CSV, no key)",
            lambda: fetch_fred_data("GDP", start_period="2023-01-01", end_period="2023-12-31"),
        ),
        ("INPS catalogue", lambda: list_inps_datasets(limit=2)),
        ("OpenPNRR", lambda: fetch_pnrr_data("missioni", params={"page_size": 2})),
    ]


def main() -> int:
    """Print one line per live provider and return a shell-friendly status."""
    failures = 0
    print("Live provider verification")
    for name, retrieve in _checks():
        try:
            frame = retrieve()
            if frame.empty:
                raise RuntimeError("empty DataFrame returned")
            print(f"PASS  {name:<25} rows={len(frame):>5} columns={len(frame.columns):>2}")
        except Exception as exc:
            failures += 1
            print(f"FAIL  {name:<25} {type(exc).__name__}: {exc}")
    if failures:
        print(f"\n{failures} provider check(s) failed.")
        return 1
    print("\nAll live provider checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
