# Import necessary libraries
import pandas as pd  # Pandas is used for data manipulation and analysis
from istatapi import discovery  # 'discovery' is assumed to be a module for interacting with the ISTAT API
from utils import process_datasets_in_parallel  # Custom utility function for parallel processing

def main():
    """
    Main function to fetch available datasets from the ISTAT API, process them in parallel, 
    and save the results to an Excel file.
    """
    # Fetch the list of all available datasets using the ISTAT API
    # 'discovery.all_available()' is assumed to return a list of datasets
    datasets = discovery.all_available()

    # Specify the number of threads to use for parallel processing
    # This number can be adjusted depending on the system's processing power and workload
    num_threads = 10

    # Process the datasets using parallel processing
    # The function 'process_datasets_in_parallel' takes the datasets and number of threads as input
    # It returns the processed results as a list
    results = process_datasets_in_parallel(datasets, num_threads)

    # Convert the processed results (a list of dictionaries or other iterable objects) into a Pandas DataFrame
    # This structure allows for easy manipulation and export to various formats
    results_df = pd.DataFrame(results)

    # Save the DataFrame to an Excel file
    # The Excel file will contain the processed datasets for further analysis
    # 'index=False' ensures the DataFrame index is not included in the Excel file
    results_df.to_excel('available_ds_istat.xlsx', index=False)

# Ensures that the main function runs only when the script is executed directly
# Prevents the script from running if it is imported as a module in another script
if __name__ == "__main__":
    main()
