import pandas as pd
from istatapi import discovery
from utils import process_datasets_in_parallel

def main():
    # Fetch the list of all available datasets
    datasets = discovery.all_available()

    # Define the number of threads to use
    num_threads = 10  # Adjust this number based on your system's capabilities

    # Process datasets in parallel
    results = process_datasets_in_parallel(datasets, num_threads)

    # Convert the list of results to a DataFrame
    results_df = pd.DataFrame(results)

    # Save results to an Excel file
    results_df.to_excel('available_ds_istat.xlsx', index=False)

if __name__ == "__main__":
    main()
