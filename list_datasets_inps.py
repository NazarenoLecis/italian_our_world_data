import requests
import pandas as pd

def fetch_all_datasets_and_export_to_excel(output_file):
    # Base URL for the endpoint
    base_url = "https://serviziweb2.inps.it/odapi/current_package_list_with_resources"
    limit = 50  # Number of results per page
    offset = 0  # Start with the first dataset
    all_datasets = []

    while True:
        # Construct URL with limit and offset for pagination
        url = f"{base_url}?limit={limit}&offset={offset}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            datasets = response.json().get("result", [])
            
            if not datasets:  # Break the loop if no more datasets are returned
                break
            
            # Append datasets to the list
            for dataset in datasets:
                all_datasets.append({
                    "ID": dataset.get("id"),
                    "Title": dataset.get("title", "No title available"),
                    "Description": dataset.get("notes", "No description available")
                })
            
            # Increment offset for the next batch
            offset += limit
        except Exception as e:
            print(f"Error: {e}")
            break

    # Create a DataFrame and export to Excel
    if all_datasets:
        df = pd.DataFrame(all_datasets)
        df.to_excel(output_file, index=False, sheet_name="Datasets")
        print(f"Excel file saved as '{output_file}'.")
    else:
        print("No datasets found.")

if __name__ == "__main__":
    # Define the output Excel file name
    output_filename = "datasets_INPS_full_list.xlsx"
    fetch_all_datasets_and_export_to_excel(output_filename)
