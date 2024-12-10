#NOT WORKING YET

import requests
import pandas as pd

def fetch_oecd_data(dataset, filter_conditions=None, start_date=None, end_date=None):
    """
    Fetches data from the OECD API and returns it as a Pandas DataFrame.

    Args:
        dataset (str): The OECD dataset ID (e.g., "MEI" for Main Economic Indicators).
        filter_conditions (str, optional): Filter conditions in the format "LOCATION+SUBJECT".
        start_date (str, optional): Start date for the data in "YYYY-MM" format.
        end_date (str, optional): End date for the data in "YYYY-MM" format.

    Returns:
        pd.DataFrame: DataFrame containing the requested data.
    """
    # Base URL for the OECD API
    BASE_URL = f"https://stats.oecd.org/SDMX-JSON/data/{dataset}"
    
    # Add filters to the URL if specified
    if filter_conditions:
        BASE_URL += f"/{filter_conditions}"
    BASE_URL += "/all"  # Fetch all available dimensions for the specified dataset
    
    # Query parameters
    params = {
        "startTime": start_date,
        "endTime": end_date,
        "dimensionAtObservation": "allDimensions",
    }
    
    # Send the GET request
    response = requests.get(BASE_URL, params=params)
    
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
        observations = data.get('dataSets', [])[0].get('observations', {})
        
        # Extract dimensions metadata
        structure = data.get('structure', {})
        dimensions = structure.get('dimensions', {}).get('observation', [])
        dim_keys = [dim["name"] for dim in dimensions]
        
        # Extract time periods
        time_periods = [entry["id"] for entry in dimensions[-1].get("values", [])]
        
        # Prepare the DataFrame
        rows = []
        for obs_key, obs_value in observations.items():
            keys = obs_key.split(":")
            obs = {dim_keys[i]: dimensions[i]["values"][int(keys[i])]["name"] for i in range(len(keys) - 1)}
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
    DATASET = "MEI"  # Example dataset: Main Economic Indicators
    FILTER = "USA+IR"  # Filters for specific countries or subjects (e.g., USA, Interest Rates)
    START_DATE = "2020-01"
    END_DATE = "2023-12"
    
    try:
        # Fetch data as DataFrame
        df = fetch_oecd_data(DATASET, filter_conditions=FILTER, start_date=START_DATE, end_date=END_DATE)
        print(df)
    except Exception as e:
        print(f"An error occurred: {e}")
