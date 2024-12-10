import json
import logging
import datetime
from typing import Optional, Dict
import sdmx
import pandas as pd

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def fetch_data(
    dataflow_id: str,
    data_source: str,
    key: str = "",
    params: Optional[Dict[str, str]] = None,
) -> pd.Series:
    """Fetch ISTAT data as a pandas DataFrame."""
    client = sdmx.Client(data_source)
    data_msg = client.get("data", resource_id=dataflow_id, key=key, params=params)
    return sdmx.to_pandas(data_msg)


def transform_df(series: pd.Series, dataflow_id: str, data_source: str) -> pd.DataFrame:
    df = series.reset_index()
    df.columns = [*df.columns[:-1], "data"]
    df["date"] = pd.Timestamp.now()
    df["dataflow_id"] = dataflow_id
    df["data_source"] = data_source

    excluded = ["date", "dataflow_id", "data_source"]
    df["data"] = df.apply(
        lambda row: {col: row[col] for col in df.columns if col not in excluded}, axis=1
    )
    return df[["date", "dataflow_id", "data_source", "data"]]


def main() -> None:
    dataflow_id = "150_915"
    data_source = "ISTAT"
    params = {}
    df = fetch_data(dataflow_id, data_source, params=params)
    df = transform_df(df, dataflow_id, data_source)


if __name__ == "__main__":
    main()
