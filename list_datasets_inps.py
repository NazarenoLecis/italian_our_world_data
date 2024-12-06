import requests
import re
import pandas as pd
from html import unescape
from concurrent.futures import ThreadPoolExecutor

def fetch_bulk_download(limit=50):
    """
    Fetch data from the bulk_download endpoint.

    Parameters:
    - limit: Number of records to fetch (default is 50).

    Returns:
    - RDF data as a string.
    """
    url = f"https://serviziweb2.inps.it/odapi/bulk_download?limit={limit}"
    try:
        print(f"Fetching metadata for up to {limit} datasets from the bulk_download endpoint...")
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        print("Metadata successfully fetched!")
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Error fetching bulk download: {e}")
        return None

def fetch_dataset_status(dataset_id):
    """
    Fetch the status of a specific dataset.

    Parameters:
    - dataset_id: The ID of the dataset to check.

    Returns:
    - The status of the dataset (e.g., active, dismissed).
    """
    url = f"https://serviziweb2.inps.it/odapi/package_show?id={dataset_id}"
    try:
        print(f"Fetching status for dataset ID {dataset_id}...")
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        dataset_info = response.json()
        result = dataset_info.get("result", {})
        state = result.get("state", "unknown")
        print(f"Status for dataset ID {dataset_id}: {state}")
        return {"ID": dataset_id, "Status": state}
    except requests.exceptions.RequestException as e:
        print(f"Error fetching status for dataset ID {dataset_id}: {e}")
        return {"ID": dataset_id, "Status": "error"}

def fetch_all_statuses(dataset_ids):
    """
    Fetch statuses for all datasets using threading for parallel requests.

    Parameters:
    - dataset_ids: List of dataset IDs.

    Returns:
    - A list of dictionaries containing dataset IDs and statuses.
    """
    print(f"Starting parallel status checks for {len(dataset_ids)} datasets...")
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(fetch_dataset_status, dataset_ids))
    print("All dataset statuses successfully fetched!")
    return results

def parse_and_save_rdf_to_csv(rdf_data, output_file="datasets_with_status.csv"):
    """
    Parse RDF-like data, fetch dataset statuses, and save to a CSV file.

    Parameters:
    - rdf_data: String containing RDF data.
    - output_file: Name of the CSV file to save (default: datasets_with_status.csv).
    """
    print("Parsing metadata from RDF data...")
    entries = rdf_data.split("\n\n")
    datasets = []

    id_pattern = re.compile(r"<http://www.inps.it/dominioINPS.owl#ID_(\d+)>")
    title_pattern = re.compile(r'dcterms:title\s+"([^"]+)"@it')
    description_pattern = re.compile(r'dcterms:description\s+"([^"]+)"@it')
    
    for entry in entries:
        dataset_id = id_pattern.search(entry)
        title = title_pattern.search(entry)
        description = description_pattern.search(entry)
        if dataset_id and title and description:
            datasets.append({
                "ID": dataset_id.group(1),
                "Title": unescape(title.group(1)),
                "Description": unescape(description.group(1))
            })

    print(f"Parsed metadata for {len(datasets)} datasets. Fetching their statuses...")
    
    dataset_ids = [dataset["ID"] for dataset in datasets]
    statuses = fetch_all_statuses(dataset_ids)

    # Merge statuses with dataset information
    print("Merging statuses with dataset metadata...")
    status_dict = {status["ID"]: status["Status"] for status in statuses}
    for dataset in datasets:
        dataset["Status"] = status_dict.get(dataset["ID"], "unknown")

    print(f"Saving all dataset information and statuses to '{output_file}'...")
    df = pd.DataFrame(datasets)
    df.to_csv(output_file, index=False)
    print(f"Dataset information and statuses successfully saved to '{output_file}'.")

if __name__ == "__main__":
    print("Starting the dataset processing workflow...")
    rdf_data = fetch_bulk_download(limit=50)
    if rdf_data:
        parse_and_save_rdf_to_csv(rdf_data, output_file="datasets_with_status.csv")
    print("Workflow completed!")
