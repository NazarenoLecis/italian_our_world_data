import logging
from istatapi import discovery, retrieval
from config import API_KEY
from utils import (
    get_dataset_description,
    merge_dimension_descriptions,
    load_to_database
)

def main(dataset_id):
    logging.info(f"Starting processing for dataset ID: {dataset_id}")

    try:
        # Retrieve dataset description
        dataset_description = get_dataset_description(dataset_id)

        # Initialize the dataset
        logging.info(f"Initializing dataset with ID: {dataset_id}")
        ds = discovery.DataSet(dataflow_identifier=dataset_id)

        # Retrieve dimensions information
        logging.info("Retrieving dimensions information...")
        dimensions_info = ds.dimensions_info()

        # Retrieve the data
        logging.info("Retrieving dataset...")
        data = retrieval.get_data(ds)

        # Check if 'itter107' exists and rename it to 'territorio'
        if 'itter107' in data.columns:
            logging.info("Column 'itter107' found. Renaming it to 'territorio'...")
            data.rename(columns={'itter107': 'territorio'}, inplace=True)

        # Merge data with dimension descriptions
        logging.info("Merging data with dimension descriptions...")
        data = merge_dimension_descriptions(data, ds, dimensions_info)

        # Process DataFrame: Convert column names to lowercase, sort, and clean
        logging.info("Processing DataFrame...")
        data.columns = data.columns.str.lower()
        if 'time_period' in data.columns:
            data = data.sort_values(by='time_period', ascending=False)
        data.dropna(axis=1, how='all', inplace=True)
        if 'dataflow' in data.columns:
            data.drop(columns=['dataflow'], inplace=True)

        # Load data into the SQLite Cloud database
        load_to_database(data, dataset_description, API_KEY)

    except Exception as e:
        logging.error(f"An error occurred during processing: {e}")

if __name__ == "__main__":
    main('150_915')
