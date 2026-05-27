"""Retrieval functions for Italian and international open-data providers."""

from __future__ import annotations

import os
from io import BytesIO
from typing import Any, Mapping, Optional
from urllib.parse import quote
from xml.etree import ElementTree

import pandas as pd

from ._common import (
    DEFAULT_TIMEOUT,
    DataSourceError,
    csv_frame,
    get_json,
    get_response,
    jsonstat_frame,
    observations_frame,
)


ISTAT_URL = "https://esploradati.istat.it/SDMXWS/rest/data"
ISTAT_DATAFLOW_URL = "https://esploradati.istat.it/SDMXWS/rest/dataflow"
OECD_URL = "https://sdmx.oecd.org/public/rest/v1/data"
OECD_DATAFLOW_URL = "https://sdmx.oecd.org/public/rest/v1/dataflow"
EUROSTAT_URL = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
EUROSTAT_DATAFLOW_URL = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/dataflow"
ECB_URL = "https://data-api.ecb.europa.eu/service/data"
ECB_DATAFLOW_URL = "https://data-api.ecb.europa.eu/service/dataflow"
WORLD_BANK_URL = "https://api.worldbank.org/v2"
FRED_API_URL = "https://api.stlouisfed.org/fred/series/observations"
FRED_DOWNLOAD_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"
INPS_URL = "https://serviziweb2.inps.it/odapi"
PNRR_URL = "https://openpnrr.it/api/v1"


def _sdmx_json_dataflows(payload: Mapping[str, Any]) -> pd.DataFrame:
    flows = payload.get("data", {}).get("dataflows", [])
    return pd.DataFrame(
        [
            {
                "agency_id": flow.get("agencyID"),
                "dataflow_id": flow.get("id"),
                "version": flow.get("version"),
                "name": flow.get("name"),
                "description": flow.get("description"),
            }
            for flow in flows
        ]
    )


def _sdmx_xml_dataflows(content: bytes) -> pd.DataFrame:
    try:
        root = ElementTree.fromstring(content)
    except ElementTree.ParseError as exc:
        raise DataSourceError("Invalid SDMX structure document returned") from exc
    rows = []
    for flow in root.iter():
        if flow.tag.rsplit("}", 1)[-1] != "Dataflow":
            continue
        name_elements = [
            child for child in flow
            if child.tag.rsplit("}", 1)[-1] == "Name" and child.text
        ]
        english_names = [
            child.text
            for child in name_elements
            if child.attrib.get("{http://www.w3.org/XML/1998/namespace}lang") == "en"
        ]
        names = english_names or [child.text for child in name_elements]
        rows.append(
            {
                "agency_id": flow.attrib.get("agencyID"),
                "dataflow_id": flow.attrib.get("id"),
                "version": flow.attrib.get("version"),
                "name": names[0] if names else None,
            }
        )
    return pd.DataFrame(rows)


def list_istat_dataflows(*, session: Any = None, timeout: int = DEFAULT_TIMEOUT) -> pd.DataFrame:
    """List available ISTAT SDMX dataflows."""
    payload = get_json(
        f"{ISTAT_DATAFLOW_URL}/IT1/all/latest",
        headers={"Accept": "application/vnd.sdmx.structure+json; version=2"},
        session=session,
        timeout=timeout,
    )
    return _sdmx_json_dataflows(payload)


def fetch_istat_data(
    dataflow_id: str,
    key: str = "",
    *,
    start_period: Optional[str] = None,
    end_period: Optional[str] = None,
    params: Optional[Mapping[str, Any]] = None,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """Retrieve an ISTAT SDMX dataflow as observations.

    ``key`` is the ordered SDMX key for the dataflow, for example ``.......``
    selects every dimension value in a seven-dimension dataflow. Prefer
    filtered keys because ISTAT applies request limits and some flows are big.
    """
    url = f"{ISTAT_URL}/{quote(dataflow_id, safe='_-')}"
    if key:
        url = f"{url}/{quote(key, safe='.+_-')}"
    query = dict(params or {})
    if start_period is not None:
        query["startPeriod"] = start_period
    if end_period is not None:
        query["endPeriod"] = end_period
    response = get_response(
        url,
        params=query,
        headers={"Accept": "text/csv"},
        session=session,
        timeout=timeout,
    )
    return observations_frame(csv_frame(response))


def list_oecd_dataflows(
    agency_id: str = "all",
    *,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """List SDMX dataflows published through OECD Data Explorer.

    The Data Explorer catalogue includes flows from several agency IDs. Pass
    one of the returned ``agency_id`` values to restrict a later catalogue
    request when supported by that provider.
    """
    payload = get_json(
        f"{OECD_DATAFLOW_URL}/{quote(agency_id, safe='._-')}/all/latest",
        headers={"Accept": "application/vnd.sdmx.structure+json; version=1.0"},
        session=session,
        timeout=timeout,
    )
    return _sdmx_json_dataflows(payload)


def fetch_oecd_data(
    dataflow: str,
    key: str = "",
    *,
    start_period: Optional[str] = None,
    end_period: Optional[str] = None,
    params: Optional[Mapping[str, Any]] = None,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """Retrieve a current OECD Data Explorer SDMX flow.

    ``dataflow`` is the full OECD flow reference, such as
    ``OECD.SDD.STES,DSD_STES@DF_FINMARK,``. ``key`` follows that flow's
    ordered dimensions; use dots as wildcards.
    """
    url = f"{OECD_URL}/{quote(dataflow, safe=',@._-')}"
    if key:
        url = f"{url}/{quote(key, safe='.+_-')}"
    query = dict(params or {})
    if start_period is not None:
        query["startPeriod"] = start_period
    if end_period is not None:
        query["endPeriod"] = end_period
    response = get_response(
        url,
        params=query,
        headers={"Accept": "text/csv"},
        session=session,
        timeout=timeout,
    )
    return observations_frame(csv_frame(response))


def fetch_eurostat_data(
    dataset: str,
    *,
    filters: Optional[Mapping[str, Any]] = None,
    start_period: Optional[str] = None,
    end_period: Optional[str] = None,
    params: Optional[Mapping[str, Any]] = None,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """Retrieve a Eurostat dataset using its supported JSON-stat API."""
    query = dict(params or {})
    query.update(filters or {})
    if start_period is not None:
        query["sinceTimePeriod"] = start_period
    if end_period is not None:
        query["untilTimePeriod"] = end_period
    payload = get_json(
        f"{EUROSTAT_URL}/{quote(dataset, safe='_-')}",
        params=query,
        session=session,
        timeout=timeout,
    )
    return jsonstat_frame(payload)


def list_ecb_dataflows(*, session: Any = None, timeout: int = DEFAULT_TIMEOUT) -> pd.DataFrame:
    """List dataflows exposed by the ECB Data Portal."""
    response = get_response(
        f"{ECB_DATAFLOW_URL}/ECB/all/latest",
        headers={"Accept": "application/vnd.sdmx.structure+xml;version=2.1"},
        session=session,
        timeout=timeout,
    )
    return _sdmx_xml_dataflows(response.content)


def fetch_ecb_data(
    dataset: str,
    key: str = "",
    *,
    start_period: Optional[str] = None,
    end_period: Optional[str] = None,
    params: Optional[Mapping[str, Any]] = None,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """Retrieve observations from the ECB Data Portal SDMX API."""
    url = f"{ECB_URL}/{quote(dataset, safe='_-')}"
    if key:
        url = f"{url}/{quote(key, safe='.+_-')}"
    query = dict(params or {})
    query["format"] = "csvdata"
    if start_period is not None:
        query["startPeriod"] = start_period
    if end_period is not None:
        query["endPeriod"] = end_period
    response = get_response(url, params=query, session=session, timeout=timeout)
    return observations_frame(csv_frame(response))


def list_eurostat_dataflows(
    dataflow_id: str = "all",
    *,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """List Eurostat dataflows, or inspect a known ``dataflow_id``."""
    response = get_response(
        f"{EUROSTAT_DATAFLOW_URL}/ESTAT/{quote(dataflow_id, safe='_-')}/latest",
        params={"detail": "allstubs"} if dataflow_id == "all" else None,
        headers={"Accept": "application/vnd.sdmx.structure+xml;version=2.1"},
        session=session,
        timeout=timeout,
    )
    return _sdmx_xml_dataflows(response.content)


def fetch_world_bank_data(
    indicator: str,
    country: str = "ITA",
    *,
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
    fetch_all_pages: bool = True,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """Retrieve one World Bank indicator; Italy is the default geography."""
    url = (
        f"{WORLD_BANK_URL}/country/{quote(country, safe=';_-')}"
        f"/indicator/{quote(indicator, safe='._-')}"
    )
    params: dict[str, Any] = {"format": "json", "per_page": 1000, "page": 1}
    if start_year is not None or end_year is not None:
        first = start_year if start_year is not None else end_year
        last = end_year if end_year is not None else start_year
        params["date"] = f"{first}:{last}"

    observations = []
    payload = get_json(url, params=params, session=session, timeout=timeout)
    if not isinstance(payload, list) or len(payload) < 2:
        raise DataSourceError("World Bank returned an unexpected payload")
    metadata, page_data = payload[0], payload[1] or []
    observations.extend(page_data)
    pages = int(metadata.get("pages", 1)) if fetch_all_pages else 1
    for page in range(2, pages + 1):
        params["page"] = page
        next_payload = get_json(url, params=params, session=session, timeout=timeout)
        observations.extend(next_payload[1] or [])

    rows = []
    for item in observations:
        country_data = item.get("country", {})
        indicator_data = item.get("indicator", {})
        rows.append(
            {
                "country_id": country_data.get("id"),
                "country": country_data.get("value"),
                "indicator_id": indicator_data.get("id", indicator),
                "indicator": indicator_data.get("value"),
                "time_period": item.get("date"),
                "value": item.get("value"),
            }
        )
    return observations_frame(pd.DataFrame(rows))


def list_world_bank_indicators(
    *,
    page: int = 1,
    per_page: int = 100,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """List World Bank indicator IDs and descriptions one page at a time."""
    payload = get_json(
        f"{WORLD_BANK_URL}/indicator",
        params={"format": "json", "page": page, "per_page": per_page},
        session=session,
        timeout=timeout,
    )
    if not isinstance(payload, list) or len(payload) < 2:
        raise DataSourceError("World Bank returned an unexpected indicator catalogue")
    rows = []
    for indicator in payload[1] or []:
        source = indicator.get("source", {})
        rows.append(
            {
                "indicator_id": indicator.get("id"),
                "name": indicator.get("name"),
                "unit": indicator.get("unit"),
                "source": source.get("value"),
                "source_note": indicator.get("sourceNote"),
            }
        )
    return pd.DataFrame(rows)


def fetch_fred_data(
    series_id: str,
    api_key: Optional[str] = None,
    *,
    start_period: Optional[str] = None,
    end_period: Optional[str] = None,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """Retrieve a FRED series, with or without an API key.

    Without a key, observations are downloaded from the public FRED chart CSV
    endpoint for a known ``series_id``. With a key (or ``FRED_API_KEY``), the
    documented FRED API observations endpoint is used.
    """
    key = api_key or os.getenv("FRED_API_KEY")
    if key:
        params: dict[str, Any] = {
            "series_id": series_id,
            "api_key": key,
            "file_type": "json",
        }
        if start_period is not None:
            params["observation_start"] = start_period
        if end_period is not None:
            params["observation_end"] = end_period
        payload = get_json(FRED_API_URL, params=params, session=session, timeout=timeout)
        frame = pd.DataFrame(payload.get("observations", []))
        if frame.empty:
            return pd.DataFrame(columns=["time_period", "value"])
        return observations_frame(frame[["date", "value"]])

    params = {"id": series_id}
    if start_period is not None:
        params["cosd"] = start_period
    if end_period is not None:
        params["coed"] = end_period
    response = get_response(FRED_DOWNLOAD_URL, params=params, session=session, timeout=timeout)
    frame = csv_frame(response)
    if frame.empty:
        return pd.DataFrame(columns=["time_period", "value"])
    if "observation_date" not in frame or series_id not in frame:
        raise DataSourceError(f"FRED did not return downloadable observations for {series_id!r}")
    return observations_frame(
        frame[["observation_date", series_id]].rename(
            columns={"observation_date": "time_period", series_id: "value"}
        )
    )


def search_fred_series(
    search_text: str,
    api_key: Optional[str] = None,
    *,
    limit: int = 100,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """Search available FRED series using the official key-protected API."""
    key = api_key or os.getenv("FRED_API_KEY")
    if not key:
        raise ValueError("FRED series search requires api_key or FRED_API_KEY")
    payload = get_json(
        "https://api.stlouisfed.org/fred/series/search",
        params={
            "search_text": search_text,
            "api_key": key,
            "file_type": "json",
            "limit": limit,
        },
        session=session,
        timeout=timeout,
    )
    fields = ["id", "title", "frequency", "units", "observation_start", "observation_end"]
    return pd.DataFrame(payload.get("seriess", []), columns=fields).rename(
        columns={"id": "series_id"}
    )


def list_inps_datasets(
    *,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """List INPS open-data dataset identifiers."""
    params = {key: value for key, value in {"limit": limit, "offset": offset}.items() if value is not None}
    payload = get_json(f"{INPS_URL}/package_list", params=params, session=session, timeout=timeout)
    return pd.DataFrame({"dataset_id": payload.get("result", [])})


def get_inps_dataset_metadata(
    dataset_id: str,
    *,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> Mapping[str, Any]:
    """Return INPS metadata and downloadable resources for a dataset."""
    payload = get_json(
        f"{INPS_URL}/package_show",
        params={"id": dataset_id},
        session=session,
        timeout=timeout,
    )
    result = payload.get("result")
    if not isinstance(result, dict):
        raise DataSourceError(f"INPS dataset {dataset_id!r} was not returned")
    return result


def get_inps_dataset(
    dataset_id: str,
    *,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> Mapping[str, Any]:
    """Compatibility alias for :func:`get_inps_dataset_metadata`."""
    return get_inps_dataset_metadata(dataset_id, session=session, timeout=timeout)


def fetch_inps_data(
    dataset_id: str,
    *,
    resource_index: int = 0,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
    **read_kwargs: Any,
) -> pd.DataFrame:
    """Download a tabular INPS resource as a DataFrame."""
    metadata = get_inps_dataset_metadata(dataset_id, session=session, timeout=timeout)
    resources = metadata.get("resources", [])
    tabular = [
        resource for resource in resources
        if str(resource.get("format", "")).lower() in {"csv", "xlsx", "xls"}
    ]
    if not tabular:
        raise DataSourceError(f"INPS dataset {dataset_id!r} has no CSV or Excel resource")
    try:
        resource = tabular[resource_index]
    except IndexError as exc:
        raise DataSourceError("INPS resource_index is outside the tabular resources") from exc
    response = get_response(resource["url"], session=session, timeout=timeout)
    file_format = str(resource.get("format", "")).lower()
    if file_format in {"xlsx", "xls"}:
        return pd.read_excel(BytesIO(response.content), **read_kwargs)
    options = {"sep": None, "engine": "python"}
    options.update(read_kwargs)
    return pd.read_csv(BytesIO(response.content), **options)


def list_pnrr_resources(*, session: Any = None, timeout: int = DEFAULT_TIMEOUT) -> pd.DataFrame:
    """Return the public resource URLs exposed by OpenPNRR."""
    payload = get_json(f"{PNRR_URL}/", session=session, timeout=timeout)
    if not isinstance(payload, dict):
        raise DataSourceError("OpenPNRR resource catalogue is invalid")
    return pd.DataFrame(
        [{"resource": resource, "url": url} for resource, url in payload.items()]
    )


def fetch_pnrr_data(
    resource: str,
    *,
    params: Optional[Mapping[str, Any]] = None,
    fetch_all_pages: bool = False,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """Retrieve one OpenPNRR resource, optionally following paginated results."""
    if "/" in resource:
        raise ValueError("resource must be a resource name, such as 'missioni'")
    url: Optional[str] = f"{PNRR_URL}/{quote(resource, safe='_-')}"
    rows: list[Any] = []
    query = dict(params or {})
    while url:
        payload = get_json(url, params=query, session=session, timeout=timeout)
        if isinstance(payload, dict) and "results" in payload:
            rows.extend(payload["results"])
            url = payload.get("next") if fetch_all_pages else None
            query = {}
        elif isinstance(payload, list):
            rows.extend(payload)
            url = None
        else:
            rows.append(payload)
            url = None
    return pd.json_normalize(rows)
