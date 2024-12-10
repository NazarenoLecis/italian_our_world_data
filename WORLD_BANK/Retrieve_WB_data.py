import requests
import pandas as pd

def fetch_world_bank_data(indicator, country="all", start_year=None, end_year=None):
    """
    Fetches data from the World Bank API and returns it as a Pandas DataFrame.

    Args:
        indicator (str): The World Bank indicator (e.g., "NY.GDP.MKTP.CD" for GDP).
        country (str): Country code or "all" for all countries (default is "all").
        start_year (int, optional): Start year for the data.
        end_year (int, optional): End year for the data.

    Returns:
        pd.DataFrame: DataFrame containing the date, country, and value of the series.
    """
    # Base URL for the World Bank API
    BASE_URL = f"https://api.worldbank.org/v2/country/{country}/indicator/{indicator}"
    
    # Query parameters
    params = {
        "format": "json",  # Specify JSON as the output format
        "date": f"{start_year}:{end_year}" if start_year and end_year else None,
        "per_page": 1000  # Max number of records per page
    }
    
    # Send the GET request
    response = requests.get(BASE_URL, params=params)
    
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
        
        # Check if data is available
        if isinstance(data, list) and len(data) > 1:
            observations = data[1]  # The data is in the second element of the response list
            
            # Convert to a DataFrame
            df = pd.DataFrame(observations)
            if not df.empty:
                df = df[["date", "country", "value"]]
                df["country"] = df["country"].apply(lambda x: x["value"] if isinstance(x, dict) else x)
                df["date"] = pd.to_datetime(df["date"], format="%Y", errors="coerce")
                df["value"] = pd.to_numeric(df["value"], errors="coerce")
                df.sort_values(["country", "date"], inplace=True)
                return df
            else:
                return pd.DataFrame(columns=["date", "country", "value"])
        else:
            raise ValueError("No data available for the given parameters.")
    else:
        raise Exception(f"Failed to fetch data: {response.status_code}, {response.text}")

# Example usage
if __name__ == "__main__":
    INDICATOR = "NY.GDP.MKTP.CD"  # Example: GDP (current US$)
    START_YEAR = 2020
    END_YEAR = 2023
    COUNTRY = "all"  # Fetch for all countries
    
    try:
        # Fetch data as DataFrame
        df = fetch_world_bank_data(INDICATOR, country=COUNTRY, start_year=START_YEAR, end_year=END_YEAR)
        print(df)
    except Exception as e:
        print(f"An error occurred: {e}")
