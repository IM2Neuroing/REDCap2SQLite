import os
import datetime
import json
import pandas as pd
import logging

# Configure logger
workflow_logger = logging.getLogger('workflow_logger')

def add_timestamp_to_filename(filename):
    """
    Adds a timestamp for versioning to the filename.

    Args:
    filename (str): The filename to be versioned.

    Returns:
    str: The versioned filename.
    """
    # Get current timestamp
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    # Split filename into name and extension
    file_name, file_extension = os.path.splitext(filename)
    # Create new filename
    new_filename = f"{file_name}_{timestamp}{file_extension}"
    return new_filename


def read_config_file(file_path):
    """
    This function checks if the configuration file exists and loads it.

    Args:
    file_path (str): The path to the configuration file.

    Returns:
    dict: The configuration data.
    """
    try:
        with open(file_path, 'r') as file:
            config_data = json.load(file)
        workflow_logger.debug("Configuration file loaded")
    except FileNotFoundError:
        workflow_logger.error(f"Configuration file not found at {file_path}, search path: {os.getcwd()}")
        raise
    return config_data

def csvs_reader(folder_path):
    # Get a list of all CSV files in the folder
    csv_files = [file for file in os.listdir(folder_path) if file.endswith('.csv')]
    # Sort the CSV files by filename
    csv_files = sorted(csv_files)
    workflow_logger.debug("Found mapping CSVs %s", str(csv_files))
    
    # Iterate through each CSV file
    for csv_file in csv_files:
        # Construct the full path to the CSV file
        csv_path = os.path.join(folder_path, csv_file)
        
        # Read the CSV file into a Pandas DataFrame
        df = pd.read_csv(csv_path)
        workflow_logger.debug("Read in: %s", str(csv_path))
        
        # Yield the DataFrame to the caller
        yield df