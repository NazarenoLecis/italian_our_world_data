# Import necessary libraries
import re  # Regular expressions for text processing
import logging  # Logging for monitoring execution and debugging
import pandas as pd  # Pandas for data manipulation
from istatapi import discovery, retrieval  # ISTAT API modules for discovering and retrieving datasets
import sqlitecloud  # SQLite Cloud library for database operations
from concurrent.futures import ThreadPoolExecutor, as_completed  # Threading for parallel processing

# Configure the logging system
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',  # Log format including timestamp, level, and message
    level=logging.INFO,  # Set logging level to INFO
    datefmt='%Y-%m-%d %H:%M:%S'  # Date and time format for log messages
)

def get_dataset_description(dataset_id):
    """
    Retrieve the description of a dataset based on its ID.
    - Fetches all available datasets.
    - Filters to find the dataset matching the given ID.
    - Formats the dataset description by replacing spaces with underscores.
    """
    try:
        logging.info(f"Retrieving all available datasets...")
        available_datasets = discovery.all_available()  # Retrieve all datasets
        dataset_info = available_datasets[available_datasets['df_id'] == dataset_id]  # Filter for the given ID
        if not dataset_info.empty:
            description = re.sub(r'\s+', '_', dataset_info.iloc[0]['df_description'])  # Format description
            logging.info(f"Dataset description found: {description}")
            return description
        else:
            logging.error(f"No dataset found with ID {dataset_id}")
            raise ValueError(f"No dataset found with ID {dataset_id}")
    except Exception as e:
        logging.error(f"Error retrieving dataset description: {e}")
        raise

def merge_dimension_descriptions(data, ds, dimensions_info):
    """
    Merge dataset with descriptions of its dimensions.
    - Retrieves values for each dimension.
    - Joins these descriptions with the dataset for better interpretability.
    """
    try:
        for dimension in dimensions_info['dimension']:
            logging.info(f"Processing dimension: {dimension}")
            values_df = ds.get_dimension_values(dimension)  # Retrieve dimension values
            data[dimension] = data[dimension].astype(str)  # Ensure data type consistency
            values_df['values_ids'] = values_df['values_ids'].astype(str)
            # Merge data with dimension values
            data = data.merge(values_df, how='left', left_on=dimension, right_on='values_ids')
            data.drop(columns=[dimension, 'values_ids'], inplace=True)  # Drop redundant columns
            data.rename(columns={'values_description': f'{dimension}'}, inplace=True)  # Rename column
        return data
    except Exception as e:
        logging.error(f"Error merging dimension descriptions: {e}")
        raise

def prepare_column_definitions(data):
    """
    Prepare SQL column definitions for table creation based on the DataFrame's data types.
    - Converts column data types to SQL-compatible types.
    """
    column_definitions = []
    for column_name, dtype in data.dtypes.items():
        if pd.api.types.is_integer_dtype(dtype):
            column_type = "INTEGER"
        elif pd.api.types.is_float_dtype(dtype):
            column_type = "REAL"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            # Convert datetime columns to string format for compatibility
            logging.info(f"Converting datetime column '{column_name}' to string format...")
            data[column_name] = data[column_name].dt.strftime('%Y-%m-%d %H:%M:%S')
            column_type = "TEXT"
        else:
            column_type = "TEXT"
        column_definitions.append(f"{column_name} {column_type}")
    return column_definitions

def load_to_database(data, table_name, api_key, batch_size=1000):
    """
    Load processed data into a SQLite Cloud database.
    - Connects to the database.
    - Creates the table if it does not already exist.
    - Inserts data in batches to improve performance.
    """
    try:
        logging.info("Connecting to the SQLite Cloud database...")
        conn = sqlitecloud.connect(f"sqlitecloud://cfqv0pfvhz.sqlite.cloud:8860/IOWID?apikey={api_key}")
        conn.execute("PRAGMA synchronous = OFF;")  # Optimize for performance
        conn.execute("PRAGMA journal_mode = WAL;")  # Enable Write-Ahead Logging

        # Generate SQL column definitions
        column_definitions = prepare_column_definitions(data)

        # Create the table if it doesn't exist
        logging.info(f"Creating table '{table_name}' if it doesn't exist...")
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_definitions)})"
        conn.execute(create_table_query)

        # Insert data in batches
        total_rows = len(data)
        logging.info(f"Inserting {total_rows} rows into table '{table_name}' in batches of {batch_size} rows...")
        for start in range(0, total_rows, batch_size):
            end = start + batch_size
            batch = data.iloc[start:end]
            try:
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

        logging.info("Committing the transaction...")
        conn.commit()

    except Exception as e:
        logging.error(f"Error during database operation: {e}")
        raise
    finally:
        logging.info("Closing the database connection...")
        conn.close()

def fetch_dataset(df_id, df_description):
    """
    Fetch data for a specific dataset.
    - Retrieves data using its ID and description.
    - Returns a dictionary summarizing the success or failure.
    """
    try:
        logging.info(f"Fetching dataset {df_id}: {df_description}")
        ds = discovery.DataSet(dataflow_identifier=df_id)
        retrieval.get_data(ds)
        return {'df_id': df_id, 'df_description': df_description, 'check': 'ok', 'error': ''}
    except Exception as e:
        logging.error(f"Failed to fetch dataset {df_id}: {e}")
        return {'df_id': df_id, 'df_description': df_description, 'check': 'na', 'error': str(e)}

def process_datasets_in_parallel(datasets, num_threads=10):
    """
    Process multiple datasets in parallel using threading.
    - Distributes dataset fetching tasks across threads.
    - Collects results from all threads.
    """
    results = []
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        # Map each dataset to a thread
        future_to_dataset = {
            executor.submit(fetch_dataset, row['df_id'], row['df_description']): (row['df_id'], row['df_description'])
            for _, row in datasets.iterrows()
        }
        total_datasets = len(future_to_dataset)
        completed_datasets = 0
        for future in as_completed(future_to_dataset):
            dataset_info = future_to_dataset[future]
            completed_datasets += 1
            logging.info(f"Processed dataset {completed_datasets}/{total_datasets}: {dataset_info[0]}")
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                df_id, df_description = dataset_info
                results.append({'df_id': df_id, 'df_description': df_description, 'check': 'na', 'error': str(e)})
    return results

def save_results_to_excel(results, file_name):
    """
    Save the processed results to an Excel file.
    - Converts results into a DataFrame.
    - Writes the DataFrame to an Excel file.
    """
    try:
        logging.info(f"Saving results to {file_name}...")
        results_df = pd.DataFrame(results)
        results_df.to_excel(file_name, index=False)
        logging.info(f"Results saved to {file_name}.")
    except Exception as e:
        logging.error(f"Error saving results to Excel: {e}")
        raise
