# Import necessary modules and libraries
import logging  # Used for logging messages, useful for debugging and monitoring execution
from istatapi import discovery, retrieval  # ISTAT API modules for dataset discovery and data retrieval
from config import API_KEY  # API key for authentication with the ISTAT API
from utils import (  # Custom utility functions used throughout the script
    get_dataset_description,  # Retrieves metadata or a description of the dataset
    merge_dimension_descriptions,  # Merges descriptive metadata of dataset dimensions with the data
    load_to_database  # Loads processed data into a database
)

def main(dataset_id):
    """
    Main function to process a dataset using its ID:
    - Retrieves dataset metadata and dimensions
    - Fetches data from the ISTAT API
    - Processes and cleans the data
    - Loads the data into a SQLite Cloud database
    """
    # Log the start of the process for a specific dataset ID
    logging.info(f"Starting processing for dataset ID: {dataset_id}")

    try:
        # Step 1: Retrieve dataset description
        # Fetches a high-level description or metadata about the dataset
        dataset_description = get_dataset_description(dataset_id)

        # Step 2: Initialize the dataset
        # Create a DataSet object using the ISTAT API discovery module
        logging.info(f"Initializing dataset with ID: {dataset_id}")
        ds = discovery.DataSet(dataflow_identifier=dataset_id)

        # Step 3: Retrieve dimensions information
        # Dimensions define the structure of the dataset (e.g., time, region)
        logging.info("Retrieving dimensions information...")
        dimensions_info = ds.dimensions_info()

        # Step 4: Retrieve the dataset
        # Fetch the actual data for the specified dataset ID
        logging.info("Retrieving dataset...")
        data = retrieval.get_data(ds)

        # Step 5: Process 'itter107' column if it exists
        # Check if the dataset contains a specific column ('itter107'), which likely represents territorial codes
        # Rename it to a more descriptive name ('territorio')
        if 'itter107' in data.columns:
            logging.info("Column 'itter107' found. Renaming it to 'territorio'...")
            data.rename(columns={'itter107': 'territorio'}, inplace=True)

        # Step 6: Merge dimension descriptions
        # Combine the dataset with its dimension descriptions for better readability and analysis
        logging.info("Merging data with dimension descriptions...")
        data = merge_dimension_descriptions(data, ds, dimensions_info)

        # Step 7: Process and clean the DataFrame
        # Standardize column names to lowercase
        logging.info("Processing DataFrame...")
        data.columns = data.columns.str.lower()
        
        # Sort the data by the 'time_period' column if it exists, in descending order
        if 'time_period' in data.columns:
            data = data.sort_values(by='time_period', ascending=False)
        
        # Drop columns with all missing values
        data.dropna(axis=1, how='all', inplace=True)
        
        # Remove the 'dataflow' column if it exists, as it might not be necessary for analysis
        if 'dataflow' in data.columns:
            data.drop(columns=['dataflow'], inplace=True)

        # Step 8: Load the processed data into a database
        # Save the cleaned and enriched data into a SQLite Cloud database
        load_to_database(data, dataset_description, API_KEY)

    except Exception as e:
        # Log any errors encountered during the process
        logging.error(f"An error occurred during processing: {e}")

# Ensures that the main function is executed only when the script is run directly
# Prevents the code from executing if it is imported as a module
if __name__ == "__main__":
    main('150_915')
