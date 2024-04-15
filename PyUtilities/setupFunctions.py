import os
import datetime
import json
import logging

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
        logging.info("Configuration file loaded")
    except FileNotFoundError:
        logging.error("Configuration file not found")
        raise
    return config_data