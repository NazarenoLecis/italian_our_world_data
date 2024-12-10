#NOT WORKING YET

import requests
import pandas as pd

def fetch_eurostat_data(dataset, start_date=None, end_date=None, filters=None):
    """
    Fetches data from the Eurostat API and returns it as a Pandas DataFrame.

    Args:
        dataset (str): The Eurostat dataset ID (e.g., "nama_10_gdp" for GDP data).
        start_date (str, optional): Start date in "YYYY-MM-DD" format.
        end_date (str, optional): End date in "YYYY-MM-DD" format.
        filters (dict, optional): Filters as a dictionary (e.g., {"geo": "EU27_2020", "unit": "CP_MEUR"}).

    Returns:
        pd.DataFrame: DataFrame containing the requested data.
    """
    # Base URL for the Eurostat API
    BASE_URL = f"https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/{dataset}"
    
    # Construct filter part of the URL
    filter_string = "."
    if filters:
        filter_parts = [f"{key}.{value}" for key, value in filters.items()]
        filter_string = f".{'+'.join(filter_parts)}"

    # Build the full URL
    url = f"{BASE_URL}{filter_string}"

    # Query parameters
    params = {
        "startPeriod": start_date,
        "endPeriod": end_date,
    }

    # Headers for the request
    headers = {"Accept": "application/vnd.sdmx.data+json;version=1.0.0-wd"}

    # Send the GET request
    response = requests.get(url, params=params, headers=headers)
    
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
        observations = data.get("dataSets", [])[0].get("observations", {})

        # Extract metadata
        structure = data.get("structure", {})
        dimensions = structure.get("dimensions", {}).get("observation", [])
        dim_keys = [dim["name"] for dim in dimensions]

        # Extract time periods
        time_periods = [entry["id"] for entry in dimensions[-1].get("values", [])]

        # Prepare the DataFrame
        rows = []
        for obs_key, obs_value in observations.items():
            keys = obs_key.split(":")
            obs = {dim_keys[i]: dimensions[i]["values"][int(keys[i])]["id"] for i in range(len(keys) - 1)}
            obs["time"] = time_periods[int(keys[-1])]
            obs["value"] = obs_value[0]
            rows.append(obs)

        df = pd.DataFrame(rows)
        df["time"] = pd.to_datetime(df["time"], format="%Y-%m", errors="coerce")
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        return df
    else:
        raise Exception(f"Failed to fetch data: {response.status_code}, {response.text}")


# Example usage
if __name__ == "__main__":
    DATASET = "nama_10_gdp"  # Example dataset: GDP data
    START_DATE = "2015"
    END_DATE = "2023"
    FILTERS = {
        "geo": "EU27_2020",  # European Union
        "unit": "CP_MEUR",   # Current prices, million euro
        "na_item": "B1GQ"    # Gross domestic product at market prices
    }

    try:
        # Fetch data as DataFrame
        df = fetch_eurostat_data(DATASET, start_date=START_DATE, end_date=END_DATE, filters=FILTERS)
        print(df)
    except Exception as e:
        print(f"An error occurred: {e}")
