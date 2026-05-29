"""Retrieval functions for Italian and international open-data providers."""

from __future__ import annotations

import os
from io import BytesIO, StringIO
from typing import Any, Mapping, Optional
from urllib.parse import quote
from xml.etree import ElementTree

import pandas as pd
import requests

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
AMECO_URL = "https://ec.europa.eu/economy_finance/ameco/wq/series"
AMECO_VARIABLES_URL = (
    "https://economy-finance.ec.europa.eu/document/download/"
    "5cb507d5-10ac-4470-ad93-66c91a89bd72_en"
)
IMF_DATAMAPPER_URL = "https://www.imf.org/external/datamapper/api/v1"
UN_POPULATION_URL = "https://population.un.org/dataportalapi/api/v1"
BIS_URL = "https://stats.bis.org/api/v1"
WORLD_BANK_URL = "https://api.worldbank.org/v2"
FRED_API_URL = "https://api.stlouisfed.org/fred/series/observations"
FRED_DOWNLOAD_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"
INPS_URL = "https://serviziweb2.inps.it/odapi"
PNRR_URL = "https://openpnrr.it/api/v1"
OPENCOESIONE_URL = "https://opencoesione.gov.it/it/api"
BANKITALIA_EXCHANGE_URL = (
    "https://tassidicambio.bancaditalia.it/terzevalute-wf-web/rest/v1.0"
)
BANKITALIA_BDS_URL = "https://a2a.bancaditalia.it/infostat/dataservices"
DATI_GOV_IT_CKAN_URL = "https://www.dati.gov.it/opendata/api/3/action"
BDAP_CKAN_URL = "https://bdap-opendata.rgs.mef.gov.it/SpodCkanApi/api/3/action"
LOMBARDY_SOCRATA_DOMAIN = "https://www.dati.lombardia.it"


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


def _ckan_action_url(base_url: str, action: str) -> str:
    base = base_url.rstrip("/")
    if base.endswith("/api/3/action"):
        return f"{base}/{action}"
    return f"{base}/api/3/action/{action}"


def _ckan_result(payload: Mapping[str, Any], source: str = "CKAN") -> Any:
    if not payload.get("success", False):
        error = payload.get("error") or payload
        raise DataSourceError(f"{source} returned an unsuccessful response: {error}")
    return payload.get("result")


def _ckan_dataset_frame(datasets: list[Mapping[str, Any]]) -> pd.DataFrame:
    rows = []
    for dataset in datasets:
        organization = dataset.get("organization") or {}
        rows.append(
            {
                "dataset_id": dataset.get("name") or dataset.get("id"),
                "id": dataset.get("id"),
                "title": dataset.get("title"),
                "organization": organization.get("title") or organization.get("name"),
                "license_id": dataset.get("license_id"),
                "license_title": dataset.get("license_title"),
                "resources_count": len(dataset.get("resources") or []),
                "metadata_modified": dataset.get("metadata_modified"),
            }
        )
    return pd.DataFrame(rows)


def _resource_format(resource: Mapping[str, Any], resource_url: Optional[str] = None) -> str:
    file_format = str(resource.get("format") or "").strip().lower()
    if file_format:
        return file_format
    url = str(resource_url or resource.get("url") or "").lower()
    for suffix, inferred in {
        ".csv": "csv",
        ".tsv": "tsv",
        ".json": "json",
        ".geojson": "geojson",
        ".xlsx": "xlsx",
        ".xls": "xls",
    }.items():
        if url.split("?", 1)[0].endswith(suffix):
            return inferred
    return ""


def _normalise_json_table(payload: Any) -> pd.DataFrame:
    if isinstance(payload, list):
        return pd.json_normalize(payload)
    if isinstance(payload, dict):
        for key in ("records", "results", "data"):
            value = payload.get(key)
            if isinstance(value, list):
                return pd.json_normalize(value)
        result = payload.get("result")
        if isinstance(result, dict):
            for key in ("records", "results"):
                value = result.get(key)
                if isinstance(value, list):
                    return pd.json_normalize(value)
        return pd.json_normalize([payload])
    raise DataSourceError("JSON response is not tabular")


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, str):
        if "," in value:
            return [item.strip() for item in value.split(",") if item.strip()]
        if ";" in value:
            return [item.strip() for item in value.split(";") if item.strip()]
        return [value]
    if isinstance(value, (list, tuple, set)):
        return list(value)
    return [value]


def _normalise_portal_rows(payload: Mapping[str, Any]) -> pd.DataFrame:
    rows = payload.get("data", [])
    return pd.json_normalize(rows)


def _year_columns(frame: pd.DataFrame) -> list[str]:
    return [column for column in frame.columns if str(column).isdigit()]


def _select_ckan_resource(
    resources: list[Mapping[str, Any]],
    resource_index: int,
    *,
    formats: Optional[set[str]] = None,
) -> Mapping[str, Any]:
    selected_formats = formats or {"csv", "tsv", "json", "geojson", "xlsx", "xls"}
    tabular = [
        resource
        for resource in resources
        if _resource_format(resource) in selected_formats and resource.get("url")
    ]
    if not tabular:
        raise DataSourceError("CKAN dataset has no supported tabular resource")
    try:
        return tabular[resource_index]
    except IndexError as exc:
        raise DataSourceError("resource_index is outside the tabular resources") from exc


def _socrata_url(domain: str, path: str) -> str:
    return f"{domain.rstrip('/')}/{path.lstrip('/')}"


def _bankitalia_rates_frame(payload: Mapping[str, Any]) -> pd.DataFrame:
    rows = payload.get("rates") or payload.get("latestRates") or []
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    frame = frame.rename(
        columns={
            "referenceDate": "time_period",
            "avgRate": "value",
            "eurRate": "eur_rate",
            "usdRate": "usd_rate",
            "isoCode": "currency_code",
            "uicCode": "uic_code",
        }
    )
    for column in ("value", "eur_rate", "usd_rate"):
        if column in frame:
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame


def _bankitalia_bds_home_url(service: str, calltype: Optional[str] = None) -> str:
    query = f"service={quote(service, safe='')}"
    if calltype:
        query = f"calltype={quote(calltype, safe='')}&{query}"
    return f"{BANKITALIA_BDS_URL}/home?{query}"


def _bankitalia_bds_post(
    service: str,
    *,
    data: Optional[Mapping[str, Any]] = None,
    calltype: Optional[str] = None,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> Any:
    client = session or requests
    try:
        response = client.post(
            _bankitalia_bds_home_url(service, calltype=calltype),
            data=dict(data or {}),
            headers={
                "Accept": "application/json, text/plain, */*",
                "X-Requested-With": "XMLHttpRequest",
            },
            timeout=timeout,
        )
        response.raise_for_status()
        return response.json()
    except requests.RequestException as exc:
        raise DataSourceError(f"Bank of Italy BDS request failed: {exc}") from exc
    except ValueError as exc:
        raise DataSourceError("Bank of Italy BDS returned invalid JSON") from exc


def _bankitalia_bds_subtree_payload(node: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in node.items()
        if key not in {"attributes", "cubes"} and value is not None
    }


def _bankitalia_bds_node_row(
    node: Mapping[str, Any],
    *,
    path: tuple[str, ...],
    depth: int,
) -> dict[str, Any]:
    attributes = node.get("attributes") or {}
    name = node.get("name") or node.get("localId") or node.get("id")
    path_parts = path + ((str(name),) if name else ())
    return {
        "cube_id": node.get("id"),
        "local_id": node.get("localId"),
        "name": name,
        "node_type": node.get("nodeType"),
        "cube_stat_type": node.get("cubeStatType"),
        "children_number": node.get("childrenNumber"),
        "depth": depth,
        "path": " > ".join(path_parts),
        "node_path": node.get("nodePath"),
        "parent_path": node.get("parentAbsPath"),
        "survey_id": attributes.get("SURVEY_ID") or node.get("taxoSurveyId"),
        "first_date": attributes.get("FIRST_CUBE_DATE"),
        "last_date": attributes.get("LAST_CUBE_DATE"),
        "last_update": attributes.get("LAST_UPD"),
        "next_publication": attributes.get("NEXT_PUB"),
    }


def _bankitalia_bds_matches(row: Mapping[str, Any], query: Optional[str]) -> bool:
    if not query:
        return True
    needle = query.lower()
    haystack = " ".join(
        str(row.get(column) or "").lower()
        for column in ("cube_id", "local_id", "name", "path", "survey_id")
    )
    return needle in haystack


def _bankitalia_bds_catalogue_rows(
    *,
    max_depth: Optional[int],
    query: Optional[str],
    limit: Optional[int],
    include_roots: bool,
    node_type: Optional[str],
    session: Any,
    timeout: int,
) -> list[dict[str, Any]]:
    roots = _bankitalia_bds_post(
        "GETTAXOROOTS",
        session=session,
        timeout=timeout,
    )
    if not isinstance(roots, list):
        raise DataSourceError("Bank of Italy BDS taxonomy roots were not returned")

    rows: list[dict[str, Any]] = []
    queue: list[tuple[Mapping[str, Any], tuple[str, ...], int]] = [
        (root, (), 0) for root in roots
    ]
    seen: set[tuple[Optional[str], Optional[str]]] = set()

    while queue:
        node, path, depth = queue.pop(0)
        marker = (node.get("id"), node.get("nodePath") or node.get("parentAbsPath"))
        if marker in seen:
            continue
        seen.add(marker)

        row = _bankitalia_bds_node_row(node, path=path, depth=depth)
        should_include = include_roots or depth > 0
        if node_type is not None and row.get("node_type") != node_type:
            should_include = False
        if should_include and _bankitalia_bds_matches(row, query):
            rows.append(row)
            if limit is not None and len(rows) >= limit:
                break

        name = row.get("name")
        child_path = path + ((str(name),) if name else ())
        children = node.get("cubes") or []
        if max_depth is not None and depth >= max_depth:
            continue
        if not children and int(node.get("childrenNumber") or 0) > 0:
            children = _bankitalia_bds_post(
                "SUBTREENODES",
                data=_bankitalia_bds_subtree_payload(node),
                calltype="asin",
                session=session,
                timeout=timeout,
            )
        if isinstance(children, list):
            queue.extend((child, child_path, depth + 1) for child in children)

    return rows


def list_ckan_datasets(
    base_url: str,
    *,
    query: Optional[str] = None,
    rows: int = 100,
    start: int = 0,
    params: Optional[Mapping[str, Any]] = None,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """Search a CKAN catalogue and return dataset summaries.

    ``base_url`` may be either a portal root that exposes ``/api/3/action`` or
    the full CKAN action URL itself.
    """
    query_params = dict(params or {})
    query_params.update({"rows": rows, "start": start})
    if query is not None:
        query_params["q"] = query
    payload = get_json(
        _ckan_action_url(base_url, "package_search"),
        params=query_params,
        session=session,
        timeout=timeout,
    )
    result = _ckan_result(payload)
    return _ckan_dataset_frame(result.get("results", []))


def get_ckan_dataset_metadata(
    base_url: str,
    dataset_id: str,
    *,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> Mapping[str, Any]:
    """Return CKAN metadata and resources for one dataset."""
    payload = get_json(
        _ckan_action_url(base_url, "package_show"),
        params={"id": dataset_id},
        session=session,
        timeout=timeout,
    )
    result = _ckan_result(payload)
    if not isinstance(result, dict):
        raise DataSourceError(f"CKAN dataset {dataset_id!r} was not returned")
    return result


def get_ckan_resource_metadata(
    base_url: str,
    resource_id: str,
    *,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> Mapping[str, Any]:
    """Return CKAN metadata for one resource."""
    payload = get_json(
        _ckan_action_url(base_url, "resource_show"),
        params={"id": resource_id},
        session=session,
        timeout=timeout,
    )
    result = _ckan_result(payload)
    if not isinstance(result, dict):
        raise DataSourceError(f"CKAN resource {resource_id!r} was not returned")
    return result


def fetch_ckan_resource(
    base_url: str,
    *,
    dataset_id: Optional[str] = None,
    resource_id: Optional[str] = None,
    resource_index: int = 0,
    resource_url: Optional[str] = None,
    resource_format: Optional[str] = None,
    datastore: bool = False,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    params: Optional[Mapping[str, Any]] = None,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
    **read_kwargs: Any,
) -> pd.DataFrame:
    """Download a CKAN resource into a DataFrame.

    Pass ``dataset_id`` to select a supported CSV/JSON/Excel resource by
    ``resource_index``. Pass ``resource_id`` with ``datastore=True`` to use
    CKAN's DataStore API instead of downloading the resource URL.
    """
    query = dict(params or {})
    if datastore:
        if resource_id is None:
            raise ValueError("resource_id is required when datastore=True")
        query["resource_id"] = resource_id
        if limit is not None:
            query["limit"] = limit
        if offset is not None:
            query["offset"] = offset
        payload = get_json(
            _ckan_action_url(base_url, "datastore_search"),
            params=query,
            session=session,
            timeout=timeout,
        )
        result = _ckan_result(payload)
        records = result.get("records", []) if isinstance(result, dict) else []
        return pd.json_normalize(records)

    resource: Mapping[str, Any] = {}
    if resource_url is None:
        if dataset_id is not None:
            metadata = get_ckan_dataset_metadata(
                base_url,
                dataset_id,
                session=session,
                timeout=timeout,
            )
            resources = metadata.get("resources", [])
            resource = _select_ckan_resource(resources, resource_index)
        elif resource_id is not None:
            resource = get_ckan_resource_metadata(base_url, resource_id, session=session, timeout=timeout)
        else:
            raise ValueError("dataset_id, resource_id, or resource_url is required")
        resource_url = resource.get("url")
    if not resource_url:
        raise DataSourceError("CKAN resource does not expose a download URL")

    file_format = (resource_format or _resource_format(resource, resource_url)).lower()
    response = get_response(resource_url, params=query or None, session=session, timeout=timeout)
    if file_format in {"xlsx", "xls"}:
        return pd.read_excel(BytesIO(response.content), **read_kwargs)
    if file_format in {"json", "geojson"}:
        try:
            return _normalise_json_table(response.json())
        except ValueError as exc:
            raise DataSourceError(f"Invalid JSON returned by {resource_url}") from exc
    options = {"sep": "\t" if file_format == "tsv" else None, "engine": "python"}
    options.update(read_kwargs)
    return pd.read_csv(BytesIO(response.content), **options)


def list_socrata_datasets(
    domain: str,
    *,
    limit: int = 100,
    offset: int = 0,
    params: Optional[Mapping[str, Any]] = None,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """List datasets from a Socrata/SODA portal."""
    query = dict(params or {})
    query.update({"limit": limit, "offset": offset})
    payload = get_json(
        _socrata_url(domain, "/api/views.json"),
        params=query,
        session=session,
        timeout=timeout,
    )
    rows = []
    for item in payload:
        rows.append(
            {
                "dataset_id": item.get("id"),
                "name": item.get("name"),
                "asset_type": item.get("assetType"),
                "category": item.get("category"),
                "description": item.get("description"),
                "created_at": item.get("createdAt"),
                "updated_at": item.get("rowsUpdatedAt") or item.get("updatedAt"),
            }
        )
    return pd.DataFrame(rows)


def get_socrata_dataset_metadata(
    domain: str,
    dataset_id: str,
    *,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> Mapping[str, Any]:
    """Return metadata for one Socrata/SODA dataset."""
    payload = get_json(
        _socrata_url(domain, f"/api/views/{quote(dataset_id, safe='_-')}.json"),
        session=session,
        timeout=timeout,
    )
    if not isinstance(payload, dict):
        raise DataSourceError(f"Socrata dataset {dataset_id!r} was not returned")
    return payload


def fetch_socrata_data(
    domain: str,
    dataset_id: str,
    *,
    limit: Optional[int] = 1000,
    offset: Optional[int] = None,
    params: Optional[Mapping[str, Any]] = None,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """Retrieve rows from a Socrata/SODA dataset resource."""
    query = dict(params or {})
    if limit is not None:
        query.setdefault("$limit", limit)
    if offset is not None:
        query.setdefault("$offset", offset)
    payload = get_json(
        _socrata_url(domain, f"/resource/{quote(dataset_id, safe='_-')}.json"),
        params=query,
        session=session,
        timeout=timeout,
    )
    return _normalise_json_table(payload)


def list_italian_open_data_datasets(
    *,
    query: Optional[str] = None,
    rows: int = 100,
    start: int = 0,
    params: Optional[Mapping[str, Any]] = None,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """Search the national dati.gov.it CKAN catalogue."""
    return list_ckan_datasets(
        DATI_GOV_IT_CKAN_URL,
        query=query,
        rows=rows,
        start=start,
        params=params,
        session=session,
        timeout=timeout,
    )


def get_italian_open_data_dataset_metadata(
    dataset_id: str,
    *,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> Mapping[str, Any]:
    """Return metadata for one dataset from the national dati.gov.it catalogue."""
    return get_ckan_dataset_metadata(
        DATI_GOV_IT_CKAN_URL, dataset_id, session=session, timeout=timeout
    )


def fetch_italian_open_data_resource(
    *,
    dataset_id: Optional[str] = None,
    resource_id: Optional[str] = None,
    resource_index: int = 0,
    resource_url: Optional[str] = None,
    resource_format: Optional[str] = None,
    params: Optional[Mapping[str, Any]] = None,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
    **read_kwargs: Any,
) -> pd.DataFrame:
    """Download a tabular resource advertised by dati.gov.it."""
    return fetch_ckan_resource(
        DATI_GOV_IT_CKAN_URL,
        dataset_id=dataset_id,
        resource_id=resource_id,
        resource_index=resource_index,
        resource_url=resource_url,
        resource_format=resource_format,
        params=params,
        session=session,
        timeout=timeout,
        **read_kwargs,
    )


def list_bdap_datasets(
    *,
    query: Optional[str] = None,
    rows: int = 100,
    start: int = 0,
    params: Optional[Mapping[str, Any]] = None,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """Search OpenBDAP public-finance datasets from RGS/MEF."""
    return list_ckan_datasets(
        BDAP_CKAN_URL,
        query=query,
        rows=rows,
        start=start,
        params=params,
        session=session,
        timeout=timeout,
    )


def get_bdap_dataset_metadata(
    dataset_id: str,
    *,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> Mapping[str, Any]:
    """Return metadata for one OpenBDAP dataset."""
    return get_ckan_dataset_metadata(
        BDAP_CKAN_URL, dataset_id, session=session, timeout=timeout
    )


def fetch_bdap_data(
    *,
    dataset_id: Optional[str] = None,
    resource_id: Optional[str] = None,
    resource_index: int = 0,
    resource_url: Optional[str] = None,
    resource_format: Optional[str] = None,
    datastore: bool = False,
    limit: Optional[int] = None,
    offset: Optional[int] = None,
    params: Optional[Mapping[str, Any]] = None,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
    **read_kwargs: Any,
) -> pd.DataFrame:
    """Retrieve a tabular OpenBDAP resource."""
    return fetch_ckan_resource(
        BDAP_CKAN_URL,
        dataset_id=dataset_id,
        resource_id=resource_id,
        resource_index=resource_index,
        resource_url=resource_url,
        resource_format=resource_format,
        datastore=datastore,
        limit=limit,
        offset=offset,
        params=params,
        session=session,
        timeout=timeout,
        **read_kwargs,
    )


def list_lombardy_datasets(
    *,
    limit: int = 100,
    offset: int = 0,
    params: Optional[Mapping[str, Any]] = None,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """List datasets from Regione Lombardia's Socrata portal."""
    return list_socrata_datasets(
        LOMBARDY_SOCRATA_DOMAIN,
        limit=limit,
        offset=offset,
        params=params,
        session=session,
        timeout=timeout,
    )


def get_lombardy_dataset_metadata(
    dataset_id: str,
    *,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> Mapping[str, Any]:
    """Return metadata for one Regione Lombardia Socrata dataset."""
    return get_socrata_dataset_metadata(
        LOMBARDY_SOCRATA_DOMAIN, dataset_id, session=session, timeout=timeout
    )


def fetch_lombardy_data(
    dataset_id: str,
    *,
    limit: Optional[int] = 1000,
    offset: Optional[int] = None,
    params: Optional[Mapping[str, Any]] = None,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """Retrieve rows from a Regione Lombardia Socrata dataset."""
    return fetch_socrata_data(
        LOMBARDY_SOCRATA_DOMAIN,
        dataset_id,
        limit=limit,
        offset=offset,
        params=params,
        session=session,
        timeout=timeout,
    )


def list_opencoesione_resources(
    *,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """Return the public API resources exposed by OpenCoesione."""
    payload = get_json(f"{OPENCOESIONE_URL}/", session=session, timeout=timeout)
    if not isinstance(payload, dict):
        raise DataSourceError("OpenCoesione resource catalogue is invalid")
    return pd.DataFrame(
        [{"resource": resource, "url": url} for resource, url in payload.items()]
    )


def fetch_opencoesione_data(
    resource: str,
    *,
    params: Optional[Mapping[str, Any]] = None,
    fetch_all_pages: bool = False,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """Retrieve one OpenCoesione API resource."""
    if "/" in resource:
        raise ValueError("resource must be a resource name, such as 'temi'")
    url: Optional[str] = f"{OPENCOESIONE_URL}/{quote(resource, safe='_-')}/"
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


def list_bankitalia_currencies(
    *,
    lang: str = "en",
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """List currencies available from the Bank of Italy exchange-rate API."""
    payload = get_json(
        f"{BANKITALIA_EXCHANGE_URL}/currencies",
        params={"lang": lang},
        headers={"Accept": "application/json"},
        session=session,
        timeout=timeout,
    )
    rows = []
    for currency in payload.get("currencies", []):
        countries = currency.get("countries") or [{}]
        for country in countries:
            rows.append(
                {
                    "currency_code": currency.get("isoCode"),
                    "currency": currency.get("name"),
                    "graph": currency.get("graph"),
                    "country": country.get("country"),
                    "country_iso": country.get("countryISO"),
                    "validity_start_date": country.get("validityStartDate"),
                    "validity_end_date": country.get("validityEndDate"),
                }
            )
    return pd.DataFrame(rows)


def fetch_bankitalia_exchange_rates(
    *,
    reference_date: Optional[str] = None,
    base_currency: str = "EUR",
    target_currency: Optional[str] = None,
    latest: bool = False,
    lang: str = "en",
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """Retrieve Bank of Italy exchange rates.

    Without ``reference_date`` the latest daily rates are returned. With a
    ``reference_date``, the API returns daily average rates against
    ``base_currency``; pass ``target_currency`` to narrow the result to one
    currency when the source supports the pair.
    """
    if latest or reference_date is None:
        payload = get_json(
            f"{BANKITALIA_EXCHANGE_URL}/latestRates",
            params={"lang": lang},
            headers={"Accept": "application/json"},
            session=session,
            timeout=timeout,
        )
        frame = _bankitalia_rates_frame(payload)
        if target_currency and "currency_code" in frame:
            frame = frame[frame["currency_code"] == target_currency.upper()].reset_index(
                drop=True
            )
        return frame

    query = {
        "referenceDate": reference_date,
        "currencyIsoCode": base_currency.upper(),
        "lang": lang,
    }
    if target_currency is not None:
        query["baseCurrencyIsoCode"] = target_currency.upper()
    payload = get_json(
        f"{BANKITALIA_EXCHANGE_URL}/dailyRates",
        params=query,
        headers={"Accept": "application/json"},
        session=session,
        timeout=timeout,
    )
    return _bankitalia_rates_frame(payload)


def list_bankitalia_bds_catalogue(
    *,
    max_depth: Optional[int] = 2,
    query: Optional[str] = None,
    limit: Optional[int] = None,
    include_roots: bool = True,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """Traverse the Bank of Italy Statistical Database taxonomy.

    The BDS portal contains a broad catalogue of statistical cubes: interest
    rates, money and banking, public finance, balance of payments, surveys, and
    other Bank of Italy series. ``max_depth`` controls how far the taxonomy is
    expanded from the roots; pass ``None`` to walk until the remote service has
    no more child nodes. Use ``limit`` when exploring interactively.
    """
    rows = _bankitalia_bds_catalogue_rows(
        max_depth=max_depth,
        query=query,
        limit=limit,
        include_roots=include_roots,
        node_type=None,
        session=session,
        timeout=timeout,
    )
    return pd.DataFrame(rows)


def list_bankitalia_bds_cubes(
    *,
    max_depth: Optional[int] = None,
    query: Optional[str] = None,
    limit: Optional[int] = None,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """List statistical cubes available from the Bank of Italy BDS catalogue."""
    rows = _bankitalia_bds_catalogue_rows(
        max_depth=max_depth,
        query=query,
        limit=limit,
        include_roots=False,
        node_type="CUBE",
        session=session,
        timeout=timeout,
    )
    frame = pd.DataFrame(rows)
    if frame.empty or "node_type" not in frame:
        return frame
    return frame.reset_index(drop=True)


def list_ameco_variables(
    *,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """List AMECO variable codes from the Commission's official workbook."""
    response = get_response(
        AMECO_VARIABLES_URL,
        params={"filename": "Ameco Online list of variables 20250121.xlsx"},
        session=session,
        timeout=timeout,
    )
    frame = pd.read_excel(BytesIO(response.content))
    frame = frame.rename(
        columns={
            "Unnamed: 0": "chapter_id",
            "CHAPTER": "chapter",
            "Unnamed: 2": "subchapter_id",
            "SUB-CHAPTER": "subchapter",
            "AMECO\nCODE": "variable",
            "DESCRIPTION": "description",
        }
    )
    frame = frame.dropna(subset=["variable"]).reset_index(drop=True)
    frame["variable"] = frame["variable"].astype(str).str.strip()
    frame["full_variable"] = "1.0.0.0." + frame["variable"]
    columns = [
        "full_variable",
        "variable",
        "description",
        "chapter_id",
        "chapter",
        "subchapter_id",
        "subchapter",
    ]
    return frame[[column for column in columns if column in frame.columns]]


def fetch_ameco_data(
    full_variable: str,
    *,
    countries: Any = "ITA",
    years: Any = None,
    last_year: bool = False,
    year_order: str = "ASC",
    params: Optional[Mapping[str, Any]] = None,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """Retrieve one AMECO series from the legacy online series endpoint.

    ``full_variable`` is the AMECO full indicator code, for example
    ``1.0.0.0.NPTD``. Use :func:`list_ameco_variables` to discover variable
    codes, then pass one or more country codes through ``countries``.
    """
    query = dict(params or {})
    query.update(
        {
            "fullVariable": full_variable,
            "countries": ",".join(str(item) for item in _as_list(countries)),
            "Lastyear": int(last_year),
            "Yearorder": year_order,
        }
    )
    if years is not None:
        query["years"] = ",".join(str(item) for item in _as_list(years))
    response = get_response(AMECO_URL, params=query, session=session, timeout=timeout)
    try:
        tables = pd.read_html(StringIO(response.text))
    except ValueError as exc:
        raise DataSourceError("AMECO did not return a readable table") from exc
    if not tables:
        raise DataSourceError("AMECO did not return any data table")
    frame = tables[0]
    years_found = _year_columns(frame)
    if not years_found:
        return frame
    id_columns = [column for column in frame.columns if column not in years_found]
    melted = frame.melt(
        id_vars=id_columns,
        value_vars=years_found,
        var_name="time_period",
        value_name="value",
    )
    melted = melted.rename(
        columns={
            "Country": "country",
            "Label": "indicator",
            "Unit": "unit",
        }
    )
    melted["full_variable"] = full_variable
    melted["value"] = pd.to_numeric(melted["value"], errors="coerce")
    columns = [
        "full_variable",
        "country",
        "indicator",
        "unit",
        "time_period",
        "value",
    ]
    return observations_frame(melted[[column for column in columns if column in melted.columns]])


def list_imf_indicators(
    *,
    dataset: Optional[str] = None,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """List IMF DataMapper indicators and metadata."""
    payload = get_json(
        f"{IMF_DATAMAPPER_URL}/indicators",
        session=session,
        timeout=timeout,
    )
    indicators = payload.get("indicators", {})
    rows = []
    for indicator_id, metadata in indicators.items():
        row = {
            "indicator_id": indicator_id,
            "name": metadata.get("label"),
            "description": metadata.get("description"),
            "source": metadata.get("source"),
            "unit": metadata.get("unit"),
            "dataset": metadata.get("dataset"),
            "last_modified": metadata.get("last-modified"),
        }
        if dataset is None or str(row["dataset"]).lower() == dataset.lower():
            rows.append(row)
    return pd.DataFrame(rows)


def list_imf_countries(
    *,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """List IMF DataMapper country and area codes."""
    payload = get_json(
        f"{IMF_DATAMAPPER_URL}/countries",
        session=session,
        timeout=timeout,
    )
    countries = payload.get("countries", {})
    rows = []
    for country_id, metadata in countries.items():
        rows.append(
            {
                "country_id": country_id,
                "name": metadata.get("label") or metadata.get("name"),
                "iso2": metadata.get("iso2"),
                "region": metadata.get("region"),
            }
        )
    return pd.DataFrame(rows)


def fetch_imf_data(
    indicator: str,
    *,
    countries: Any = "ITA",
    periods: Any = None,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """Retrieve one IMF DataMapper indicator for selected countries."""
    requested_countries = [str(country).upper() for country in _as_list(countries)]
    requested_periods = [str(period) for period in _as_list(periods)]
    country_path = ",".join(requested_countries) if requested_countries else "all"
    query = {"periods": ",".join(requested_periods)} if requested_periods else None
    payload = get_json(
        f"{IMF_DATAMAPPER_URL}/{quote(indicator, safe='._-')}/{quote(country_path, safe=',._-')}",
        params=query,
        session=session,
        timeout=timeout,
    )
    values = payload.get("values", {}).get(indicator, {})
    metadata = payload.get("indicator", {}) or payload.get("indicators", {}).get(indicator, {})
    rows = []
    for country_id, observations in values.items():
        if requested_countries and country_id.upper() not in requested_countries:
            continue
        for period, value in observations.items():
            if requested_periods and str(period) not in requested_periods:
                continue
            rows.append(
                {
                    "country_id": country_id,
                    "indicator_id": indicator,
                    "indicator": metadata.get("label") or indicator,
                    "unit": metadata.get("unit"),
                    "dataset": metadata.get("dataset"),
                    "source": metadata.get("source"),
                    "time_period": str(period),
                    "value": value,
                }
            )
    return observations_frame(pd.DataFrame(rows))


def list_un_population_indicators(
    *,
    page_size: int = 100,
    page_number: int = 1,
    fetch_all_pages: bool = False,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """List indicators from the UN Population Data Portal."""
    rows = []
    current_page = page_number
    while True:
        payload = get_json(
            f"{UN_POPULATION_URL}/indicators",
            params={"pageSize": page_size, "pageNumber": current_page},
            session=session,
            timeout=timeout,
        )
        rows.extend(payload.get("data", []))
        pages = int(payload.get("pages") or current_page)
        if not fetch_all_pages or current_page >= pages:
            break
        current_page += 1
    frame = pd.json_normalize(rows)
    if frame.empty:
        return frame
    return frame.rename(
        columns={
            "id": "indicator_id",
            "name": "name",
            "shortName": "short_name",
            "sourceName": "source",
            "sourceYear": "source_year",
            "unitLongLabel": "unit",
            "topicName": "topic",
        }
    )


def list_un_population_locations(
    *,
    page_size: int = 100,
    page_number: int = 1,
    fetch_all_pages: bool = False,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """List locations from the UN Population Data Portal."""
    rows = []
    current_page = page_number
    while True:
        payload = get_json(
            f"{UN_POPULATION_URL}/locations",
            params={"pageSize": page_size, "pageNumber": current_page},
            session=session,
            timeout=timeout,
        )
        rows.extend(payload.get("data", []))
        pages = int(payload.get("pages") or current_page)
        if not fetch_all_pages or current_page >= pages:
            break
        current_page += 1
    frame = pd.json_normalize(rows)
    if frame.empty:
        return frame
    return frame.rename(columns={"id": "location_id", "name": "location"})


def fetch_un_population_data(
    indicator_id: int,
    *,
    location_id: int = 380,
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
    auth_token: Optional[str] = None,
    params: Optional[Mapping[str, Any]] = None,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """Retrieve UN Population Data Portal values.

    The public catalogue is open. The data endpoint can require a bearer token;
    pass ``auth_token`` or set ``UN_POPULATION_TOKEN`` when the portal enforces
    authentication.
    """
    token = auth_token or os.getenv("UN_POPULATION_TOKEN")
    if not token:
        raise ValueError("auth_token or UN_POPULATION_TOKEN is required for UN population data")
    start = start_year if start_year is not None else 1950
    end = end_year if end_year is not None else 2100
    query = dict(params or {})
    headers = {"Authorization": f"Bearer {token}"}
    payload = get_json(
        f"{UN_POPULATION_URL}/data/indicators/{indicator_id}/locations/{location_id}"
        f"/start/{start}/end/{end}",
        params=query or None,
        headers=headers,
        session=session,
        timeout=timeout,
    )
    frame = _normalise_portal_rows(payload)
    if frame.empty:
        return frame
    frame = frame.rename(
        columns={
            "location.id": "location_id",
            "location.name": "location",
            "indicator.id": "indicator_id",
            "indicator.name": "indicator",
            "timeLabel": "time_period",
            "time": "time_period",
        }
    )
    return observations_frame(frame)


def list_bis_dataflows(
    provider: str = "BIS",
    *,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """List BIS SDMX dataflows."""
    payload = get_json(
        f"{BIS_URL}/dataflow/{quote(provider, safe='._-')}/all/latest",
        params={"references": "none"},
        headers={"Accept": "application/vnd.sdmx.structure+json;version=1.0.0"},
        session=session,
        timeout=timeout,
    )
    frame = _sdmx_json_dataflows(payload)
    if not frame.empty:
        frame["dataflow"] = (
            frame["agency_id"].fillna(provider)
            + ","
            + frame["dataflow_id"].fillna("")
            + ","
            + frame["version"].fillna("latest")
        )
        first = ["dataflow"]
        frame = frame[first + [column for column in frame.columns if column not in first]]
    return frame


def fetch_bis_data(
    dataflow: str,
    key: str = "",
    *,
    start_period: Optional[str] = None,
    end_period: Optional[str] = None,
    params: Optional[Mapping[str, Any]] = None,
    session: Any = None,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """Retrieve observations from the BIS SDMX API."""
    url = f"{BIS_URL}/data/{quote(dataflow, safe=',._-')}"
    if key:
        url = f"{url}/{quote(key, safe='.+_-')}"
    query = dict(params or {})
    query["format"] = "csv"
    if start_period is not None:
        query["startPeriod"] = start_period
    if end_period is not None:
        query["endPeriod"] = end_period
    response = get_response(url, params=query, session=session, timeout=timeout)
    return observations_frame(csv_frame(response))


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
