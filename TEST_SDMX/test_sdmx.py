# Import necessary libraries
import pandasdmx  # Library for handling SDMX data
import logging  # Library for logging messages

# Configure logging to track the execution process
logging.basicConfig(
    level=logging.INFO,  # Set logging level to INFO
    format='%(asctime)s - %(levelname)s - %(message)s'  # Format for log messages
)

def fetch_istat_data(dataflow_id, key='', params=None):
    """
    Fetch data from ISTAT using the SDMX protocol.

    Parameters:
        dataflow_id (str): The ID of the dataflow to retrieve.
        key (str): A string representing the selection key for dimensions (optional).
                   Example: 'A.U2' to filter specific data.
        params (dict): Additional parameters for the SDMX query (optional).
                       Example: {'startPeriod': '2020', 'endPeriod': '2023'}

    Returns:
        pandas.DataFrame: The retrieved dataset as a pandas DataFrame.

    Raises:
        Exception: If any error occurs during data retrieval.
    """
    try:
        # Step 1: Initialize the ISTAT SDMX client
        logging.info("Connecting to ISTAT SDMX service...")
        client = pandasdmx.Request('ISTAT')

        # Step 2: Retrieve the structure (metadata) of the specified dataflow
        logging.info(f"Fetching structure for dataflow ID: {dataflow_id}...")
        dataflow = client.dataflow(dataflow_id)  # This call retrieves metadata about the dataset
        structure = dataflow.dataflow[dataflow_id].structure

        # Log available dimensions for the dataflow
        logging.info("Available dimensions in the dataflow:")
        for dim in structure.dimensions:
            logging.info(f"Dimension: {dim.id}, Name: {dim.name}")

        # Step 3: Retrieve the actual data for the dataflow
        logging.info(f"Fetching data for dataflow ID: {dataflow_id} with key: '{key}'...")
        response = client.data(resource_id=dataflow_id, key=key, params=params)

        # Step 4: Convert the response to a pandas DataFrame
        logging.info("Converting data to a pandas DataFrame...")
        data = response.to_pandas()

        logging.info("Data successfully retrieved.")
        return data  # Return the resulting DataFrame

    except Exception as e:
        # Handle and log any errors during data retrieval
        logging.error(f"Error fetching data from ISTAT: {e}")
        raise

if __name__ == "__main__":
    """
    Main block to execute the script. Allows for testing and saving data locally.
    """

    # Step 5: Specify the dataflow ID to fetch data
    # Replace  with a valid ISTAT dataflow ID from the list of available dataflows
    dataflow_id = '150_915'  # Example: Gross Domestic Product and main components

    # Step 6: (Optional) Specify filtering parameters for the data query
    # Key allows for filtering by specific dimension values
    key = ''  # Leave empty for all data; specify keys for dimension filtering if needed

    # Add additional query parameters, such as a date range
    params = {'startPeriod': '2000', 'endPeriod': '2024'}  # Filter data from 2000 to 2024

    # Step 7: Fetch the data using the fetch_istat_data function
    try:
        data = fetch_istat_data(dataflow_id, key=key, params=params)

        # Step 8: Display the first few rows of the retrieved data
        print("Sample of the retrieved data:")
        print(data.head())

        # Step 9: Save the data to a CSV file for further analysis
        output_file = f"{dataflow_id}_istat_data.csv"
        data.to_csv(output_file, index=False)  # Save DataFrame as a CSV file without the index column
        logging.info(f"Data successfully saved to {output_file}")

    except Exception as e:
        logging.error(f"Failed to fetch or save data: {e}")