import requests
import json
import csv

# Define the main API URL
base_url = "https://openpnrr.it/api/v1/"

# Send a GET request to fetch the main data
response = requests.get(base_url)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    # Parse the JSON response
    data = response.json()

    # Inspect the structure of the data
    print("Main API Data Structure:")
    print(json.dumps(data, indent=4))  # Pretty print the data to check its structure

    # Loop through the dictionary and fetch data from each endpoint
    for key, endpoint_url in data.items():
        print(f"\nFetching data from {key}: {endpoint_url}")
        
        # Send a GET request to the individual endpoint
        endpoint_response = requests.get(endpoint_url)
        
        if endpoint_response.status_code == 200:
            # Parse the JSON response from the endpoint
            endpoint_data = endpoint_response.json()

            # Check if the data contains a 'results' key for further extraction
            if isinstance(endpoint_data, dict) and 'results' in endpoint_data:
                data_to_save = endpoint_data['results']
            else:
                data_to_save = endpoint_data

            # Handle the 'componenti' field if it exists
            if 'componenti' in data_to_save[0]:
                for item in data_to_save:
                    if 'componenti' in item:
                        # Extract the 'componenti' list and explode it
                        componenti_list = item['componenti']
                        if componenti_list:
                            for component in componenti_list:
                                # Create a CSV file for each component
                                component_file_name = f"{key}_componenti_{item.get('id', 'unknown')}.csv"

                                with open(component_file_name, mode="w", newline="", encoding="utf-8") as csv_file:
                                    # Write each component's data to the CSV
                                    writer = csv.DictWriter(csv_file, fieldnames=component.keys())
                                    writer.writeheader()
                                    writer.writerow(component)

                                print(f"Component data saved to '{component_file_name}'")
            else:
                print(f"No 'componenti' field found in the data for {key}.")
            
            # Convert the main data (without exploding components) to CSV
            if data_to_save:
                # Define the CSV file name for main data
                file_name = f"{key}_data.csv"
                
                # Open a CSV file for writing
                with open(file_name, mode="w", newline="", encoding="utf-8") as csv_file:
                    # Create a CSV writer object
                    writer = csv.DictWriter(csv_file, fieldnames=data_to_save[0].keys())

                    # Write the header (column names)
                    writer.writeheader()

                    # Write the data (rows)
                    writer.writerows(data_to_save)

                print(f"Data from {key} saved to '{file_name}'")
            else:
                print(f"No data to save for {key}.")
        else:
            print(f"Failed to fetch data from {key}. Status code: {endpoint_response.status_code}")
else:
    print(f"Failed to retrieve data from the main API. Status code: {response.status_code}")
