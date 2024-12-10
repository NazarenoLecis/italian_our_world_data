import sdmx
import logging
import csv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def list_and_save_dataflows_to_csv(source_name, csv_file):
    """
    List all available dataflows for a given predefined SDMX source and save them to a CSV file.

    Parameters:
        source_name (str): Predefined source name (e.g., 'ISTAT', 'ECB').
        csv_file (str): Path to the CSV file where the dataflows will be saved.
    """
    try:
        client = sdmx.Client(source_name)
        dataflows = client.dataflow()

        if not dataflows.dataflow:
            print(f"{source_name} has no available datasets.")
            return

        with open(csv_file, mode='a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            for flow_id, flow_obj in dataflows.dataflow.items():
                writer.writerow([source_name, flow_id, flow_obj.name])
        print(f"Dataflows for {source_name} saved to {csv_file}.")

    except Exception as e:
        print(f"Error fetching dataflows from {source_name}: {e}")

if __name__ == "__main__":
    # Predefined SDMX sources supported by the `sdmx` library
    sources = ['ISTAT', 'ECB', 'OECD', 'ESTAT', 'WB', 'IMF']
    output_csv = "dataflows_list.csv"

    # Clear the CSV file and add headers
    with open(output_csv, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Source', 'Dataflow ID', 'Name'])

    # List dataflows for each source and save to CSV
    for source in sources:
        print(f"\nListing dataflows for {source}...")
        list_and_save_dataflows_to_csv(source, output_csv)
