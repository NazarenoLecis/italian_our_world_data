import requests
import pandas as pd

def fetch_ecb_data(dataset, resource_id, start_period, end_period):
    """
    Fetches data from the ECB API and returns it as a Pandas DataFrame.

    Args:
        dataset (str): The dataset name (e.g., "EXR" for exchange rates).
        resource_id (str): The specific resource identifier.
        start_period (str): Start date in "YYYY-MM-DD" format.
        end_period (str): End date in "YYYY-MM-DD" format.

    Returns:
        pd.DataFrame: DataFrame containing the extracted data.
    """
    # Base URL for the ECB API
    BASE_URL = "https://sdw-wsrest.ecb.europa.eu/service/data"

    # Build the full URL
    url = f"{BASE_URL}/{dataset}/{resource_id}"
    
    # Query parameters
    params = {
        "startPeriod": start_period,
        "endPeriod": end_period
    }

    # Headers for the request
    headers = {"Accept": "application/vnd.sdmx.data+json;version=1.0.0-wd"}
    
    # Send the GET request
    response = requests.get(url, params=params, headers=headers)
    
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
        
        # Extract observations
        dataset = data.get('dataSets', [])[0]
        series = dataset.get('series', {})
        
        time_dimension = data.get('structure', {}).get('dimensions', {}).get('observation', [])[0]
        time_periods = [entry['id'] for entry in time_dimension.get('values', [])]

        time_series = []
        for key, value in series.items():
            for time_index, obs in value['observations'].items():
                date = time_periods[int(time_index)]  # Map time index to the actual date
                rate = obs[0]  # Get the exchange rate
                time_series.append({"date": date, "rate": rate})
        
        # Convert to a Pandas DataFrame
        df = pd.DataFrame(time_series)
        df['date'] = pd.to_datetime(df['date'])  # Ensure date is in datetime format
        df.sort_values('date', inplace=True)  # Sort by date
        return df
    
    else:
        raise Exception(f"Failed to fetch data: {response.status_code}, {response.text}")


# Example usage
if __name__ == "__main__":
    dataset = "EXR"  # Exchange Rates dataset
    resource_id = "D.USD.EUR.SP00.A"  # Daily exchange rate for USD to EUR
    start_period = "2023-01-01"
    end_period = "2023-12-31"
    
    try:
        # Fetch data as DataFrame
        df = fetch_ecb_data(dataset, resource_id, start_period, end_period)
        print(df)
    except Exception as e:
        print(f"An error occurred: {e}")
