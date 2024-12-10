import requests
import pandas as pd

def fetch_fred_data(series_id, api_key, start_date=None, end_date=None):
    """
    Fetches data from the FRED API and returns it as a Pandas DataFrame.

    Args:
        series_id (str): The FRED series ID (e.g., "GDP" for GDP data).
        api_key (str): Your FRED API key.
        start_date (str, optional): Start date in "YYYY-MM-DD" format.
        end_date (str, optional): End date in "YYYY-MM-DD" format.

    Returns:
        pd.DataFrame: DataFrame containing the date and value of the series.
    """
    # Base URL for the FRED API
    BASE_URL = "https://api.stlouisfed.org/fred/series/observations"
    
    # Query parameters
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": "json",  # Specify JSON as the output format
    }
    
    if start_date:
        params["observation_start"] = start_date
    if end_date:
        params["observation_end"] = end_date

    # Send the GET request
    response = requests.get(BASE_URL, params=params)
    
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
        
        # Extract observations
        observations = data.get("observations", [])
        
        # Convert to a DataFrame
        df = pd.DataFrame(observations)
        if not df.empty:
            df = df[["date", "value"]]
            df["date"] = pd.to_datetime(df["date"])
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
            df.sort_values("date", inplace=True)
            return df
        else:
            return pd.DataFrame(columns=["date", "value"])
    else:
        raise Exception(f"Failed to fetch data: {response.status_code}, {response.text}")

# Example usage
if __name__ == "__main__":
    # Replace with your FRED API key
    API_KEY = "your_api_key_here"
    SERIES_ID = "GDP"  # Example: GDP data
    START_DATE = "2020-01-01"
    END_DATE = "2023-12-31"
    
    try:
        # Fetch data as DataFrame
        df = fetch_fred_data(SERIES_ID, API_KEY, START_DATE, END_DATE)
        print(df)
    except Exception as e:
        print(f"An error occurred: {e}")
