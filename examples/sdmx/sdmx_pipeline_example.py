"""DataFrame transformation example for an ISTAT SDMX result."""

import pandas as pd

from italian_our_world_data import fetch_istat_data


def fetch_data(dataflow_id, data_source="ISTAT", key="", params=None):
    if data_source.upper() != "ISTAT":
        raise ValueError("Use the provider-specific library function for non-ISTAT data")
    return fetch_istat_data(dataflow_id, key, params=params)


def transform_df(frame, dataflow_id, data_source):
    observations = frame.to_dict(orient="records")
    return pd.DataFrame(
        {
            "date": [pd.Timestamp.now()] * len(observations),
            "dataflow_id": [dataflow_id] * len(observations),
            "data_source": [data_source] * len(observations),
            "data": observations,
        }
    )
