import pandas as pd
import sqlitecloud
from config import API_KEY  # Import the API key from config.py

# Define the path to your Excel file
excel_file_path = "datasets.xlsx"  # Replace with the actual path to your Excel file

# Read the Excel file into a DataFrame
df = pd.read_excel(excel_file_path)

# Connect to the SQLite Cloud database
conn = sqlitecloud.connect(f"sqlitecloud://cfqv0pfvhz.sqlite.cloud:8860/IOWID?apikey={API_KEY}")

# Ensure the correct database context is set
conn.execute("USE DATABASE IOWID")

# Dynamically construct the CREATE TABLE statement based on DataFrame columns and data types
column_definitions = []
for column_name, dtype in df.dtypes.items():
    if pd.api.types.is_integer_dtype(dtype):
        column_type = "INTEGER"
    elif pd.api.types.is_float_dtype(dtype):
        column_type = "REAL"
    else:
        column_type = "TEXT"
    column_definitions.append(f"{column_name} {column_type}")

# Create the table
table_name = "uploaded_data"
create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_definitions)})"
conn.execute(create_table_query)

# Insert the data into the table
data = df.values.tolist()  # Convert DataFrame to a list of lists
columns = ", ".join(df.columns)  # Get column names from the DataFrame
placeholders = ", ".join(["?"] * len(df.columns))  # Create placeholders for query

insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
conn.executemany(insert_query, data)

# Query the table to verify the data
cursor = conn.execute(f"SELECT * FROM {table_name}")

# Print out the data retrieved
for row in cursor:
    print(row)

# Optionally, close the connection
conn.close()
