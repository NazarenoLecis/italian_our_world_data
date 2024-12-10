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
        print(f"Fetching dataset metadata for ID {dataset_id}...")
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        print("Metadata fetched successfully!")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching dataset {dataset_id}: {e}")
        return None

def download_resource(dataset_id, resource_url):
    """
    Download a resource file from a given URL.

    Parameters:
    - dataset_id: The ID of the dataset.
    - resource_url: The URL of the resource to download.

    Returns:
    - Path to the downloaded file.
    """
    output_path = f"dataset_{dataset_id}.csv"
    try:
        print(f"Downloading resource from {resource_url} for dataset ID {dataset_id}...")
        response = requests.get(resource_url, timeout=15)
        response.raise_for_status()
        with open(output_path, 'wb') as file:
            file.write(response.content)
        print(f"Resource downloaded successfully and saved to '{output_path}'.")
        return output_path
    except requests.exceptions.RequestException as e:
        print(f"Error downloading resource from {resource_url}: {e}")
        return None

def load_and_save_as_excel(dataset_id, file_path):
    """
    Load the dataset and save it as an Excel file.

    Parameters:
    - dataset_id: The ID of the dataset.
    - file_path: Path to the input dataset file.

    Returns:
    - Path to the cleaned Excel file.
    """
    output_file = f"cleaned_dataset_{dataset_id}.xlsx"
    try:
        print(f"Loading dataset from {file_path}...")
        data = pd.read_csv(file_path, delimiter=';', encoding='utf-8', on_bad_lines='skip')

        print("Cleaning data...")
        for column in data.columns:
            data[column] = (
                data[column]
                .astype(str)
                .str.replace(" ", "", regex=False)
                .str.replace(",", ".", regex=False)
            )
            data[column] = pd.to_numeric(data[column], errors='ignore')

        print(f"Saving cleaned data to Excel file: {output_file}...")
        data.to_excel(output_file, index=False, sheet_name=f"Dataset_{dataset_id}")
        print(f"Cleaned dataset successfully saved to '{output_file}'.")
        return output_file
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def process_dataset(dataset_id):
    """
    Fetch, download, clean, and save a dataset as an Excel file.

    Parameters:
    - dataset_id: The ID of the dataset to process.
    """
    dataset_info = fetch_dataset(dataset_id)
    
    if dataset_info:
        print("Analyzing dataset resources...")
        resources = dataset_info.get('result', {}).get('resources', [])
        if not resources:
            print(f"No resources found for dataset {dataset_id}.")
            return
        for resource in resources:
            resource_url = resource.get('url')
            format = resource.get('format')
            print(f"Found resource: {resource_url} (Format: {format})")
            if resource_url and format and format.lower() == "csv":
                # Step 3: Download the resource file
                downloaded_file = download_resource(dataset_id, resource_url)

                # Step 4: Save the dataset directly as an Excel file
                if downloaded_file:
                    cleaned_file = load_and_save_as_excel(dataset_id, downloaded_file)
                    if cleaned_file:
                        print(f"Dataset {dataset_id} processed successfully. Cleaned file: {cleaned_file}")
                    break
    else:
        print(f"Dataset {dataset_id} could not be fetched.")

if __name__ == "__main__":
    dataset_id = "82"
    process_dataset(dataset_id)
