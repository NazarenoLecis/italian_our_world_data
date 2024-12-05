import re
import logging
from istatapi import discovery, retrieval
import pandas as pd
import sqlitecloud
from config import API_KEY  # Ensure this file contains your API key

# Constants
DATASET_ID = "150_915"

# Configure logging
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)

def get_dataset_description(dataset_id):
    """Retrieve and format the dataset description for the given dataset ID."""
    logging.info(f"Retrieving all available datasets...")
    available_datasets = discovery.all_available()
    dataset_info = available_datasets[available_datasets['df_id'] == dataset_id]
    if not dataset_info.empty:
        description = re.sub(r'\s+', '_', dataset_info.iloc[0]['df_description'])
        logging.info(f"Dataset description found: {description}")
        return description
    else:
        logging.error(f"No dataset found with ID {dataset_id}")
        raise ValueError(f"No dataset found with ID {dataset_id}")

def merge_dimension_descriptions(data, ds, dimensions_info):
    """Merge data with dimension descriptions."""
    for dimension in dimensions_info['dimension']:
        logging.info(f"Processing dimension: {dimension}")
        values_df = ds.get_dimension_values(dimension)
        data[dimension] = data[dimension].astype(str)
        values_df['values_ids'] = values_df['values_ids'].astype(str)
        data = data.merge(values_df, how='left', left_on=dimension, right_on='values_ids')
        data.drop(columns=[dimension, 'values_ids'], inplace=True)
        data.rename(columns={'values_description': f'{dimension}_description'}, inplace=True)
    return data

def prepare_column_definitions(data):
    """Prepare column definitions for SQL table creation."""
    column_definitions = []
    for column_name, dtype in data.dtypes.items():
        if pd.api.types.is_integer_dtype(dtype):
            column_type = "INTEGER"
        elif pd.api.types.is_float_dtype(dtype):
            column_type = "REAL"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            logging.info(f"Converting datetime column '{column_name}' to string format...")
            data[column_name] = data[column_name].dt.strftime('%Y-%m-%d %H:%M:%S')
            column_type = "TEXT"
        else:
            column_type = "TEXT"
        column_definitions.append(f"{column_name} {column_type}")
    return column_definitions

def main():
    logging.info(f"Starting processing for dataset ID: {DATASET_ID}")

    # Retrieve dataset description
    dataset_description = get_dataset_description(DATASET_ID)

    # Initialize the dataset
    logging.info(f"Initializing dataset with ID: {DATASET_ID}")
    ds = discovery.DataSet(dataflow_identifier=DATASET_ID)

    # Retrieve dimensions information
    logging.info("Retrieving dimensions information...")
    dimensions_info = ds.dimensions_info()

    # Retrieve the data
    logging.info("Retrieving dataset...")
    data = retrieval.get_data(ds)

    # Merge data with dimension descriptions
    logging.info("Merging data with dimension descriptions...")
    data = merge_dimension_descriptions(data, ds, dimensions_info)

    # Convert all column names to lowercase
    logging.info("Converting all column names to lowercase...")
    data.columns = data.columns.str.lower()

    # Sort the DataFrame by 'time_period' in descending order
    if 'time_period' in data.columns:
        logging.info("Sorting data by 'time_period' in descending order...")
        data = data.sort_values(by='time_period', ascending=False)

    # Remove columns that are all NaN
    logging.info("Removing columns that are entirely NaN...")
    data.dropna(axis=1, how='all', inplace=True)

    # Drop the 'dataflow' column if it exists
    if 'dataflow' in data.columns:
        logging.info("Dropping 'dataflow' column...")
        data.drop(columns=['dataflow'], inplace=True)
    data=data.head(500)
    # Prepare column definitions for SQL table creation
    logging.info("Preparing column definitions for SQL table creation...")
    column_definitions = prepare_column_definitions(data)

    # Connect to the SQLite Cloud database
    logging.info("Connecting to the SQLite Cloud database...")
    conn = sqlitecloud.connect(f"sqlitecloud://cfqv0pfvhz.sqlite.cloud:8860/IOWID?apikey={API_KEY}")

    # Create the table
    table_name = dataset_description
    logging.info(f"Creating table '{table_name}' if it doesn't exist...")
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_definitions)})"
    conn.execute(create_table_query)

    # Insert the data into the table
    logging.info(f"Inserting data into table '{table_name}'...")
    data.to_sql(table_name, conn, if_exists='append', index=False)

    # Commit the transaction
    logging.info("Committing the transaction...")
    conn.commit()

    # Close the connection
    logging.info("Closing the database connection...")
    conn.close()

    logging.info(f"Data processing and insertion into table '{table_name}' completed successfully.")

if __name__ == "__main__":
    main()
