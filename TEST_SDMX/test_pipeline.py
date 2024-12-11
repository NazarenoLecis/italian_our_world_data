import json
import logging
import datetime
from typing import Optional, Dict
import sdmx
import pandas as pd

# Configure logging settings to provide information about the program's execution.
# The format includes timestamp, log level, and message.
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Define a function to fetch data from an ISTAT source using the SDMX library.
def fetch_data(
    dataflow_id: str,  # The ID of the dataflow to retrieve.
    data_source: str,  # The source of the data (e.g., ISTAT).
    key: str = "",  # Optional key to filter the data query.
    params: Optional[Dict[str, str]] = None,  # Optional query parameters for the API call.
) -> pd.Series:  # Returns the data as a pandas Series.
    """Fetch ISTAT data as a pandas DataFrame."""
    # Initialize an SDMX client for the specified data source.
    client = sdmx.Client(data_source)
    
    # Fetch the data message from the SDMX API using the specified dataflow ID and parameters.
    data_msg = client.get("data", resource_id=dataflow_id, key=key, params=params)
    
    # Convert the SDMX data message into a pandas Series for easier data manipulation.
    return sdmx.to_pandas(data_msg)

# Define a function to transform a pandas Series into a structured DataFrame.
def transform_df(series: pd.Series, dataflow_id: str, data_source: str) -> pd.DataFrame:
    # Reset the index of the pandas Series to prepare it for transformation into a DataFrame.
    df = series.reset_index()
    
    # Rename the last column to "data" to store the actual data values.
    df.columns = [*df.columns[:-1], "data"]
    
    # Add a new column for the current timestamp to track when the data was processed.
    df["date"] = pd.Timestamp.now()
    
    # Add columns to identify the dataflow ID and data source for contextual information.
    df["dataflow_id"] = dataflow_id
    df["data_source"] = data_source

    # Define a list of columns to exclude when creating a dictionary representation of each row.
    excluded = ["date", "dataflow_id", "data_source"]
    
    # Apply a lambda function to each row to create a dictionary of non-excluded columns.
    df["data"] = df.apply(
        lambda row: {col: row[col] for col in df.columns if col not in excluded}, axis=1
    )
    
    # Return a DataFrame with only the relevant columns for output.
    return df[["date", "dataflow_id", "data_source", "data"]]

# Define the main function that orchestrates the data fetching and transformation process.
def main() -> None:
    # Define the dataflow ID and source to fetch data from.
    dataflow_id = "150_915"  # Example dataflow ID.
    data_source = "ISTAT"  # Example data source.
    
    # Initialize query parameters for the data fetching function.
    params = {}
    
    # Fetch the data using the fetch_data function.
    df = fetch_data(dataflow_id, data_source, params=params)
    
    # Transform the fetched data into a structured format using the transform_df function.
    df = transform_df(df, dataflow_id, data_source)

# Check if the script is executed as the main program.
if __name__ == "__main__":
    # Call the main function to execute the program logic.
    main()
