import json
import sqlite3
import csv

def extract_table_attributes(sql_file):
    # Connect to the SQLite database
    conn = sqlite3.connect(':memory:')
    cursor = conn.cursor()

    # Read and execute the SQL script
    with open(sql_file, 'r') as f:
        script = f.read()
        cursor.executescript(script)

    # Extract table names and attributes
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()

    table_attributes = []
    for table in tables:
        table_name = table[0]
        cursor.execute(f"PRAGMA table_info({table_name})")
        attributes = cursor.fetchall()

        for attr in attributes:
            attribute_name = attr[1]
            is_nullable = 'NULL' if attr[3] == 0 else 'NOT NULL'
            table_attributes.append((table_name, attribute_name, is_nullable))
    # delete all rows with table name 'sqlite_sequence'
    table_attributes = [x for x in table_attributes if x[0] != 'sqlite_sequence']        

    # Close the database connection
    cursor.close()
    conn.close()

    return table_attributes

def write_to_csv(table_attributes, path):
    counter = 0
    # Create the CSV file for each table[0] which is the table name
    # skip tables which are already created
    tables=[]
    for table in table_attributes:
        table_name = table[0]
        if table_name in tables:
            continue
        tables.append(table_name)
        with open(f'{path}{counter}-0-{table_name}.csv', mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['Table', 'Attribute', 'NotNull', 'field_name'])
            for row in table_attributes:
                if row[0] == table_name:
                    # extend row with None value for field_name
                    row = list(row)
                    row.append(None)
                    if row[2] == 'NULL':
                        row[2] = None
                    writer.writerow(row)
        # Increment the counter
        counter += 1

    print(f"Total {counter} CSV files created.")

# Main function
# Read Config file to specify the input and output file paths
config_file = 'config.json'
with open(config_file, 'r') as f:
    config = json.load(f)
    sql_file = config['db_schema']   # Input SQL file
    table_path = config['mapping_path']   # Output CSV file

# Extract table attributes
table_attributes = extract_table_attributes(sql_file)

# check if there are already csv files in the folder
import os
files = os.listdir(table_path)
if len(files) > 0:
    print(f"Files already exist in the folder: {table_path}. Are you sure you want to overwrite the existing ones? Please delete the files and run the script again.")
    exit()

# Write the table attributes to CSV
write_to_csv(table_attributes, table_path)
