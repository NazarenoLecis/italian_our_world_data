"""GeoPandas helpers for Italian administrative boundaries."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import geopandas as gpd

try:
    from ._common import DEFAULT_TIMEOUT, DataSourceError, get_json
except ImportError:
    if __package__:
        raise
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from italian_our_world_data._common import DEFAULT_TIMEOUT, DataSourceError, get_json


CONFINI_AMMINISTRATIVI_URL = "https://www.confini-amministrativi.it/api/v1"
DEFAULT_BOUNDARY_RELEASE = "20200101"
BOUNDARY_DIVISIONS = {
    "regions": "regioni",
    "regioni": "regioni",
    "municipalities": "comuni",
    "comuni": "comuni",
    "macroregions": "ripartizioni-geografiche",
    "ripartizioni-geografiche": "ripartizioni-geografiche",
    "supra-municipal-units": "unita-territoriali-sovracomunali",
    "unita-territoriali-sovracomunali": "unita-territoriali-sovracomunali",
}


def _division_path(division: str) -> str:
    try:
        return BOUNDARY_DIVISIONS[division]
    except KeyError as exc:
        allowed = ", ".join(sorted(BOUNDARY_DIVISIONS))
        raise ValueError(f"Unknown boundary division {division!r}. Use one of: {allowed}") from exc


def _boundary_url(release: str, division: str, data_format: str) -> str:
    path = _division_path(division)
    return f"{CONFINI_AMMINISTRATIVI_URL}/{release}/{data_format}/{path}/{path}.json"


def list_administrative_boundary_divisions() -> pd.DataFrame:
    """List boundary divisions supported by the confini-amministrativi API."""
    rows = [
        {
            "division": "regioni",
            "alias": "regions",
            "description": "Italian regions",
            "default_code_column": "cod_reg",
        },
        {
            "division": "comuni",
            "alias": "municipalities",
            "description": "Italian municipalities",
            "default_code_column": "pro_com_t",
        },
        {
            "division": "ripartizioni-geografiche",
            "alias": "macroregions",
            "description": "Italian geographic macroregions",
            "default_code_column": "cod_rip",
        },
        {
            "division": "unita-territoriali-sovracomunali",
            "alias": "supra-municipal-units",
            "description": "Italian supra-municipal territorial units",
            "default_code_column": "cod_uts",
        },
    ]
    return pd.DataFrame(rows)


def fetch_administrative_boundary_metadata(
    division: str = "regioni",
    *,
    release: str = DEFAULT_BOUNDARY_RELEASE,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """Fetch non-geometric administrative boundary metadata as a DataFrame."""
    payload = get_json(
        _boundary_url(release, division, "json"),
        session=session,
        timeout=timeout,
    )
    return pd.DataFrame(payload)


def fetch_administrative_boundaries(
    division: str = "regioni",
    *,
    release: str = DEFAULT_BOUNDARY_RELEASE,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
):
    """Fetch Italian administrative boundaries as a GeoDataFrame.

    Boundaries are provided by https://www.confini-amministrativi.it/ from
    ISTAT/ANPR-derived data.
    """
    payload = get_json(
        _boundary_url(release, division, "geojson"),
        session=session,
        timeout=timeout,
    )
    if payload.get("type") != "FeatureCollection":
        raise DataSourceError("Boundary endpoint did not return a GeoJSON FeatureCollection")
    frame = gpd.GeoDataFrame.from_features(payload["features"], crs="EPSG:4326")
    frame.columns = [str(column).lower() for column in frame.columns]
    return frame


def attach_administrative_boundaries(
    data: pd.DataFrame,
    *,
    division: str = "regioni",
    data_key: str,
    boundary_key: Optional[str] = None,
    release: str = DEFAULT_BOUNDARY_RELEASE,
    how: str = "left",
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
):
    """Join a data table to administrative boundaries and return a GeoDataFrame.

    ``data_key`` is the column in ``data`` containing administrative codes.
    ``boundary_key`` defaults to the division's usual code column.
    """
    boundaries = fetch_administrative_boundaries(
        division,
        release=release,
        session=session,
        timeout=timeout,
    )
    if boundary_key is None:
        matches = list_administrative_boundary_divisions()
        canonical = _division_path(division)
        boundary_key = matches.loc[
            matches["division"] == canonical, "default_code_column"
        ].iloc[0]
    if data_key not in data.columns:
        raise KeyError(f"{data_key!r} is not a column in data")
    if boundary_key not in boundaries.columns:
        raise KeyError(f"{boundary_key!r} is not a column in boundary data")

    data_for_join = data.copy()
    data_for_join[data_key] = data_for_join[data_key].astype(str)
    boundaries = boundaries.copy()
    boundaries[boundary_key] = boundaries[boundary_key].astype(str)
    return boundaries.merge(
        data_for_join,
        how=how,
        left_on=boundary_key,
        right_on=data_key,
    )


def main() -> int:
    """Print available boundary divisions for command-line smoke checks."""
    print(list_administrative_boundary_divisions().to_string(index=False))
    print(
        "\nTo fetch geometries, use for example:\n"
        "python3 - <<'PY'\n"
        "from italian_our_world_data import fetch_administrative_boundaries\n"
        "print(fetch_administrative_boundaries('regioni').head())\n"
        "PY"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
