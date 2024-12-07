import re
import logging
from istatapi import discovery, retrieval
import pandas as pd
import sqlitecloud
from config import API_KEY

# Configure logging
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)

def get_dataset_description(dataset_id):
    """Retrieve and format the dataset description for the given dataset ID."""
    try:
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
    except Exception as e:
        logging.error(f"Error retrieving dataset description: {e}")
        raise

def merge_dimension_descriptions(data, ds, dimensions_info):
    """Merge data with dimension descriptions."""
    try:
        for dimension in dimensions_info['dimension']:
            logging.info(f"Processing dimension: {dimension}")
            values_df = ds.get_dimension_values(dimension)
            data[dimension] = data[dimension].astype(str)
            values_df['values_ids'] = values_df['values_ids'].astype(str)
            data = data.merge(values_df, how='left', left_on=dimension, right_on='values_ids')
            data.drop(columns=[dimension, 'values_ids'], inplace=True)
            data.rename(columns={'values_description': f'{dimension}'}, inplace=True)
        return data
    except Exception as e:
        logging.error(f"Error merging dimension descriptions: {e}")
        raise

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

def batch_insert_data(conn, data, table_name, batch_size=1000):
    """Insert data into the database in batches."""
    total_rows = len(data)
    logging.info(f"Inserting {total_rows} rows into table '{table_name}' in batches of {batch_size} rows...")
    for start in range(0, total_rows, batch_size):
        end = start + batch_size
        batch = data.iloc[start:end]
        try:
            # Use method='multi' for faster multi-row insertion
            batch.to_sql(
                table_name,
                conn,
                if_exists='append',
                index=False,
                method='multi'
            )
            logging.info(f"Inserted rows {start} to {end}...")
        except Exception as e:
            logging.error(f"Failed to insert rows {start} to {end}: {e}")
            raise

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

        # Prepare column definitions for SQL table creation
        column_definitions = prepare_column_definitions(data)

        # Connect to the SQLite Cloud database
        logging.info("Connecting to the SQLite Cloud database...")
        conn = sqlitecloud.connect(f"sqlitecloud://cfqv0pfvhz.sqlite.cloud:8860/IOWID?apikey={API_KEY}")
        conn.execute("PRAGMA synchronous = OFF;")  # Performance optimization
        conn.execute("PRAGMA journal_mode = WAL;")

        # Create the table
        table_name = dataset_description
        logging.info(f"Creating table '{table_name}' if it doesn't exist...")
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_definitions)})"
        conn.execute(create_table_query)

        # Insert the data into the table in batches
        batch_insert_data(conn, data, table_name)

        # Commit the transaction
        logging.info("Committing the transaction...")
        conn.commit()

    except Exception as e:
        logging.error(f"An error occurred during processing: {e}")
    finally:
        # Close the connection
        logging.info("Closing the database connection...")
        conn.close()

    logging.info(f"Data processing and insertion into table '{table_name}' completed successfully.")

if __name__ == "__main__":
    main('150_915')
