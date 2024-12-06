import requests
import re
import pandas as pd
from html import unescape

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
        # Make a GET request to the bulk_download endpoint
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        
        print(f"Data successfully fetched from the endpoint.")
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Error fetching bulk download: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error: {e}")
        return None

def parse_and_save_rdf_to_csv(rdf_data, output_file="datasets.csv"):
    """
    Parse RDF-like data, decode text fields, and save to a CSV file.

    Parameters:
    - rdf_data: String containing RDF data.
    - output_file: Name of the CSV file to save (default: datasets.csv).
    """
    # Split RDF data into individual entries based on dataset blocks
    entries = rdf_data.split("\n\n")
    
    # List to hold parsed data
    datasets = []

    # Regular expressions to extract key fields
    id_pattern = re.compile(r"<http://www.inps.it/dominioINPS.owl#ID_(\d+)>")
    title_pattern = re.compile(r'dcterms:title\s+"([^"]+)"@it')
    description_pattern = re.compile(r'dcterms:description\s+"([^"]+)"@it')
    
    # Iterate through entries to parse data
    for entry in entries:
        dataset_id = id_pattern.search(entry)
        title = title_pattern.search(entry)
        description = description_pattern.search(entry)

        # Decode the description and title if available
        if dataset_id and title and description:
            datasets.append({
                "ID": dataset_id.group(1),
                "Title": unescape(title.group(1)),
                "Description": unescape(description.group(1))
            })

    # Create a DataFrame and save it to a CSV file
    df = pd.DataFrame(datasets)
    df.to_csv(output_file, index=False)
    print(f"Data successfully parsed and saved to '{output_file}'.")

if __name__ == "__main__":
    # Step 1: Fetch data from the bulk_download endpoint
    rdf_data = fetch_bulk_download(limit=50)
    
    # Step 2: Parse and save the data to a CSV file if the request was successful
    if rdf_data:
        parse_and_save_rdf_to_csv(rdf_data, output_file="datasets_inps.csv")
