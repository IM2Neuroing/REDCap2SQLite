from PyUtilities.setupFunctions import read_config_file
from redcap import Project
import pandas as pd
import logging


CONFIG_FILE_PATH = 'config.json'

def extract_data():
  # load configuration file
  config = read_config_file(CONFIG_FILE_PATH)

  ## DATA EXTRACTION REDCAP to CSV
  # Check if CSV file needs to be downloaded from REDCap
  if config["extract_redcap"] is True:
    # Run function to download data from REDCap
    extract_redcap_data(config)

  ## DATA EXTRACTION from CSV
  # Logic to read data from CSV using pandas
  data = pd.read_csv(config['extraction_path'])
  return data

def extract_redcap_data(config):
  """
  Function to extract data from REDCap.
  """
  # Check if REDCap API token is provided
  if config['redcap_api_token'] is None:
    logging.error("No REDCap API token was specified in the config file")
    exit()
  # Check if REDCap URL is provided
  if config['redcap_api_address'] is None:
    logging.error("No REDCap URL was specified in the config file")
    exit()
  # Check if extraction path is provided
  if config['extraction_path'] is None:
    logging.error("No extraction path was specified in the config file")
    exit()
  
  ## LOGIC to extract data from REDCap
  api_url = config['redcap_api_address']
  api_key = config['redcap_api_token']
  project = Project(api_url, api_key)

  # Download data from REDCap
  data = project.export_records(raw_or_label="label")
  # Save data to a file using pandas
  df = pd.DataFrame(data)
  df.to_csv(config['extraction_path'], index=False)
  logging.info(f"Data extracted from REDCap: {df}")
  return
