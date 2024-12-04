from istatapi import discovery, retrieval
import pandas as pd

# Fetch the list of all available datasets
datasets = discovery.all_available()

# Initialize a list to store results
results = []

# Iterate through each dataset and attempt retrieval
for index, row in datasets.iterrows():
    df_id = row['df_id']
    df_description = row['df_description']
    print(f"Processing dataset {df_id}: {df_description}")

    # Attempt to retrieve data
    try:
        # Initialize the dataset
        ds = discovery.DataSet(dataflow_identifier=df_id)
        data = retrieval.get_data(ds)
        # If retrieval is successful, mark as 'ok' with no error
        check_status = 'ok'
        error_message = ''
    except Exception as e:
        # If retrieval fails, mark as 'na' and record the error message
        check_status = 'na'
        error_message = str(e)
        print(f"Failed to retrieve dataset {df_id}: {error_message}")

    # Append the result to the list
    results.append({
        'df_id': df_id,
        'df_description': df_description,
        'check': check_status,
        'error': error_message
    })

# Convert the list of results to a DataFrame
results_df = pd.DataFrame(results)

# Convert the list of results to a DataFrame
results_df = pd.DataFrame(results)
results_df.to_excel('available_ds_istat.xlsx',index=False)
