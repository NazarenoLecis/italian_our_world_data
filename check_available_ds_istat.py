from istatapi import discovery, retrieval
import pandas as pd

# Create a list to store the results
results = []


for index, row in datasets.iterrows():
    df_id = row['df_id']
    df_description = row['df_description']
    print(f"Processing dataset {df_id}: {df_description}")

    # Attempt to retrieve data
    try:
        # Initialize the dataset
        ds = discovery.DataSet(dataflow_identifier=df_id)
        data = retrieval.get_data(ds)
        # If retrieval is successful, mark as 'ok'
        check_status = 'ok'
    except Exception as e:
        # If retrieval fails, mark as 'na'
        check_status = 'na'
        print(f"Failed to retrieve dataset {df_id}: {e}")

    # Append the result to the list
    results.append({
        'df_id': df_id,
        'df_description': df_description,
        'check': check_status
    })

# Convert the list of results to a DataFrame
results_df = pd.DataFrame(results)
results_df.to_excel('available_ds_istat.xlsx',index=False)
