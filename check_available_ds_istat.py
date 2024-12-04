import warnings
from istatapi import discovery, retrieval
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

# Suppress all warnings
warnings.filterwarnings("ignore")

def fetch_dataset(df_id, df_description):
    try:
        # Initialize the dataset
        ds = discovery.DataSet(dataflow_identifier=df_id)
        # Retrieve data
        data = retrieval.get_data(ds)
        # If retrieval is successful, mark as 'ok'
        return {'df_id': df_id, 'df_description': df_description, 'check': 'ok', 'error': ''}
    except Exception as e:
        # If retrieval fails, mark as 'na' and record the error message
        return {'df_id': df_id, 'df_description': df_description, 'check': 'na', 'error': str(e)}

# Fetch the list of all available datasets
datasets = discovery.all_available()

# Initialize a list to store results
results = []

# Define the number of threads to use
num_threads = 10  # Adjust this number based on your system's capabilities

# Use ThreadPoolExecutor to fetch datasets in parallel
with ThreadPoolExecutor(max_workers=num_threads) as executor:
    # Create a dictionary to map futures to dataset info
    future_to_dataset = {
        executor.submit(fetch_dataset, row['df_id'], row['df_description']): (row['df_id'], row['df_description'])
        for index, row in datasets.iterrows()
    }
    total_datasets = len(future_to_dataset)
    completed_datasets = 0
    for future in as_completed(future_to_dataset):
        dataset_info = future_to_dataset[future]
        completed_datasets += 1
        print(f"Processing dataset {completed_datasets}/{total_datasets}: {dataset_info[0]}")
        try:
            result = future.result()
            results.append(result)
        except Exception as e:
            # Handle exceptions raised during retrieval
            df_id, df_description = dataset_info
            results.append({'df_id': df_id, 'df_description': df_description, 'check': 'na', 'error': str(e)})

# Convert the list of results to a DataFrame
results_df = pd.DataFrame(results)
results_df.to_excel('available_ds_istat.xlsx',index=False)
