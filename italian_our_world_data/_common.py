"""Shared transport and data-frame helpers for public data APIs."""

from __future__ import annotations

from io import BytesIO, StringIO
from typing import Any, Iterable, Mapping, Optional

import pandas as pd
import requests


DEFAULT_TIMEOUT = 30


class DataSourceError(RuntimeError):
    """Raised when a public data source cannot satisfy a request."""


def get_response(
    url: str,
    *,
    params: Optional[Mapping[str, Any]] = None,
    headers: Optional[Mapping[str, str]] = None,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> Any:
    """Return an HTTP response or raise a source-oriented error."""
    client = session or requests
    try:
        response = client.get(url, params=params, headers=headers, timeout=timeout)
        response.raise_for_status()
        return response
    except requests.RequestException as exc:
        raise DataSourceError(f"Request failed for {url}: {exc}") from exc


def get_json(url: str, **kwargs: Any) -> Any:
    """Retrieve a JSON response with a useful error for invalid payloads."""
    response = get_response(url, **kwargs)
    try:
        return response.json()
    except ValueError as exc:
        raise DataSourceError(f"Invalid JSON returned by {url}") from exc


def csv_frame(response: Any, **kwargs: Any) -> pd.DataFrame:
    """Load text or binary CSV response content into a DataFrame."""
    kwargs.setdefault("dtype", str)
    text = getattr(response, "text", None)
    if text is not None:
        return pd.read_csv(StringIO(text), **kwargs)
    return pd.read_csv(BytesIO(response.content), **kwargs)


def observations_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """Use common names for observation period and value columns."""
    names = {column: str(column).lower() for column in frame.columns}
    frame = frame.rename(columns=names)
    frame = frame.rename(
        columns={
            "obs_value": "value",
            "date": "time_period",
            "time": "time_period",
        }
    )
    if "value" in frame:
        frame["value"] = pd.to_numeric(frame["value"], errors="coerce")
    return frame


def _codes_by_position(dimension: Mapping[str, Any]) -> list[str]:
    category = dimension.get("category", {})
    index = category.get("index", [])
    if isinstance(index, dict):
        return [code for code, _ in sorted(index.items(), key=lambda item: item[1])]
    return list(index)


def _coordinates(position: int, sizes: Iterable[int]) -> list[int]:
    coordinates: list[int] = []
    remaining = position
    sizes_list = list(sizes)
    for size in reversed(sizes_list):
        coordinates.append(remaining % size)
        remaining //= size
    return list(reversed(coordinates))


def jsonstat_frame(payload: Mapping[str, Any]) -> pd.DataFrame:
    """Convert the JSON-stat2 dataset representation used by Eurostat."""
    dimension_ids = list(payload.get("id", []))
    sizes = list(payload.get("size", []))
    dimensions = payload.get("dimension", {})
    if not dimension_ids or len(dimension_ids) != len(sizes):
        raise DataSourceError("JSON-stat response does not describe its dimensions")

    codes = [_codes_by_position(dimensions[name]) for name in dimension_ids]
    raw_values = payload.get("value", {})
    if isinstance(raw_values, list):
        values = {index: value for index, value in enumerate(raw_values) if value is not None}
    else:
        values = {int(index): value for index, value in raw_values.items()}

    rows = []
    for position, value in values.items():
        coordinate = _coordinates(position, sizes)
        row = {
            name: codes[index][coordinate[index]]
            for index, name in enumerate(dimension_ids)
        }
        row["value"] = value
        rows.append(row)
    return observations_frame(pd.DataFrame(rows, columns=[*dimension_ids, "value"]))
