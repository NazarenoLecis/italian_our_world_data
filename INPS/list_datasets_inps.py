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

def fetch_dataset_metadata(dataset_id):
    """
    Fetch metadata for a specific dataset, including status and last modification date.

    Parameters:
    - dataset_id: The ID of the dataset to check.

    Returns:
    - A dictionary with dataset ID, status, and last modification date.
    """
    url = f"https://serviziweb2.inps.it/odapi/package_show?id={dataset_id}"
    try:
        print(f"Fetching metadata for dataset ID {dataset_id}...")
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        dataset_info = response.json().get("result", {})
        return {
            "ID": dataset_id,
            "Status": dataset_info.get("state", "unknown"),
            "Last Modified": dataset_info.get("metadata_modified", "unknown")
        }
    except requests.exceptions.RequestException as e:
        print(f"Error fetching metadata for dataset ID {dataset_id}: {e}")
        return {"ID": dataset_id, "Status": "error", "Last Modified": "unknown"}

def fetch_all_metadata(dataset_ids):
    """
    Fetch metadata for all datasets using threading for parallel requests.

    Parameters:
    - dataset_ids: List of dataset IDs.

    Returns:
    - A list of dictionaries containing metadata for all datasets.
    """
    print(f"Starting parallel metadata fetch for {len(dataset_ids)} datasets...")
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(fetch_dataset_metadata, dataset_ids))
    print("All dataset metadata successfully fetched!")
    return results

def parse_and_save_rdf_to_csv(rdf_data, output_file="datasets_with_metadata.csv"):
    """
    Parse RDF-like data, fetch dataset metadata, and save to a CSV file.

    Parameters:
    - rdf_data: String containing RDF data.
    - output_file: Name of the CSV file to save (default: datasets_with_metadata.csv).
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

    print(f"Parsed metadata for {len(datasets)} datasets. Fetching detailed metadata...")
    
    dataset_ids = [dataset["ID"] for dataset in datasets]
    metadata = fetch_all_metadata(dataset_ids)

    # Merge metadata with dataset information
    print("Merging detailed metadata with dataset information...")
    metadata_dict = {item["ID"]: item for item in metadata}
    for dataset in datasets:
        dataset_metadata = metadata_dict.get(dataset["ID"], {})
        dataset["Status"] = dataset_metadata.get("Status", "unknown")
        dataset["Last Modified"] = dataset_metadata.get("Last Modified", "unknown")

    print(f"Saving all dataset information to '{output_file}'...")
    df = pd.DataFrame(datasets)
    df.to_csv(output_file, index=False)
    print(f"Dataset information successfully saved to '{output_file}'.")

if __name__ == "__main__":
    print("Starting the dataset processing workflow...")
    rdf_data = fetch_bulk_download(limit=50)
    if rdf_data:
        parse_and_save_rdf_to_csv(rdf_data, output_file="datasets_with_metadata.csv")
    print("Workflow completed!")
