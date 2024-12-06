import requests
import pandas as pd

def fetch_dataset(dataset_id):
    """
    Fetch metadata and resources for a specific INPS dataset.

    Parameters:
    - dataset_id: The ID of the dataset to fetch.

    Returns:
    - A dictionary containing dataset metadata and resources.
    """
    url = f"https://serviziweb2.inps.it/odapi/package_show?id={dataset_id}"
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching dataset {dataset_id}: {e}")
        return None

def download_resource(resource_url, output_path):
    """
    Download a resource file from a given URL.

    Parameters:
    - resource_url: The URL of the resource to download.
    - output_path: The local file path to save the downloaded resource.
    """
    try:
        response = requests.get(resource_url, timeout=15)
        response.raise_for_status()
        with open(output_path, 'wb') as file:
            file.write(response.content)
        print(f"Resource downloaded successfully and saved to '{output_path}'.")
    except requests.exceptions.RequestException as e:
        print(f"Error downloading resource from {resource_url}: {e}")

def load_and_save_as_excel(file_path, output_file="cleaned_dataset_54.xlsx"):
    """
    Load the dataset and save it as an Excel file.

    Parameters:
    - file_path: Path to the input dataset file.
    - output_file: Path to save the Excel file (default: cleaned_dataset_{id}.xlsx).
    """
    try:
        # Load the dataset using the correct delimiter
        data = pd.read_csv(file_path, delimiter=';', encoding='utf-8', on_bad_lines='skip')

        # Clean the numeric columns by removing spaces and converting them
        for column in data.columns:
            # Remove spaces from numeric fields and handle errors gracefully
            data[column] = (
                data[column]
                .astype(str)  # Ensure all values are strings for cleaning
                .str.replace(" ", "", regex=False)  # Remove spaces
                .str.replace(",", ".", regex=False)  # Replace commas with dots for decimal conversion
            )
            # Convert cleaned strings to numeric where possible
            data[column] = pd.to_numeric(data[column], errors='ignore')

        # Save the cleaned dataset to an Excel file
        data.to_excel(output_file, index=False, sheet_name="Dataset_54")
        print(f"Cleaned dataset successfully saved to Excel file: '{output_file}'.")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    # Step 1: Fetch dataset metadata
    dataset_id = "54"
    dataset_info = fetch_dataset(dataset_id)
    
    if dataset_info:
        # Step 2: Extract resources and download
        resources = dataset_info.get('result', {}).get('resources', [])
        for resource in resources:
            resource_url = resource.get('url')
            format = resource.get('format')
            if resource_url and format and format.lower() == "csv":
                # Step 3: Download the resource file
                downloaded_file = f"dataset_{dataset_id}.csv"
                download_resource(resource_url, downloaded_file)

                # Step 4: Save the dataset directly as an Excel file
                cleaned_file = f"cleaned_dataset_{dataset_id}.xlsx"
                load_and_save_as_excel(downloaded_file, output_file=cleaned_file)
