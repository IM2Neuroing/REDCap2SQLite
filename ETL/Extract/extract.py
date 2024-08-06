import sys
import os

# Get the current script directory
current_script_directory = os.path.dirname(os.path.realpath(__file__))

# Get the root directory of your repository
root_directory = os.path.abspath(os.path.join(current_script_directory, os.pardir, os.pardir))

# Add the root directory to sys.path
sys.path.append(root_directory)

from PyUtilities.setupFunctions import read_config_file

from redcap import Project
import pandas as pd
import logging

# Configure logger
workflow_logger = logging.getLogger('workflow_logger')

CONFIG_FILE_PATH = 'config.json'
CONFIG = read_config_file(CONFIG_FILE_PATH)

def extract_data():
  """
  Function to extract data from the source.
  """ 
  # Check if extraction path is provided
  if CONFIG['extraction_path'] is None:
    workflow_logger.error("No extraction path was specified in the config file, no data will be extracted")
    exit()

  ## DATA EXTRACTION REDCAP to CSV
  # Check if CSV file needs to be downloaded from REDCap
  if CONFIG["extract_redcap"] is True:
    # Run function to download data from REDCap
    workflow_logger.info("Extracting data from REDCap")
    data = extract_redcap_data()

  ## DATA EXTRACTION from CSV
  else:
    # Check if CSV file exists
    try:
      with open(CONFIG['extraction_path'], 'r') as file:
        # Logic to read data from CSV using pandas
        data = pd.read_csv(CONFIG['extraction_path'],dtype=str, index_col='index', encoding='utf-8', na_filter=False)
    except FileNotFoundError:
      workflow_logger.error(f"File not found at the specified extraction path: {CONFIG['extraction_path']}")
      exit()
  
    # check if data is empty
  if data.empty:
    workflow_logger.error("Extracted data is empty, no data will be processed")
    exit()

  workflow_logger.info(f"Data extracted:\n{data}")
  return data

def extract_redcap_data():
  """
  Function to extract data from REDCap.
  """
  # Check if REDCap API token is provided
  if CONFIG['redcap_api_token'] is None:
    workflow_logger.error("No REDCap API token was specified in the config file")
    exit()
  # Check if REDCap URL is provided
  if CONFIG['redcap_api_address'] is None:
    workflow_logger.error("No REDCap URL was specified in the config file")
    exit()
  
  ## LOGIC to extract data from REDCap
  api_url = CONFIG['redcap_api_address']
  api_key = CONFIG['redcap_api_token']
  project = Project(api_url, api_key)
  workflow_logger.debug("Project variables defined")
  # Download data from REDCap
  data = project.export_records(format_type='json',
                                records=None,
                                fields=None, 
                                forms=None, 
                                events=None, 
                                raw_or_label='label', 
                                raw_or_label_headers='raw', 
                                event_name='label', 
                                record_type='eav', 
                                export_survey_fields=False, 
                                export_data_access_groups=False, 
                                export_checkbox_labels=True, 
                                filter_logic=None, 
                                date_begin=None, 
                                date_end=None, 
                                decimal_character=None, 
                                export_blank_for_gray_form_status=None, 
                                df_kwargs=None) 
  workflow_logger.debug("Data acquired from REDCap API")
  # Save data to a file using pandas
  df = pd.DataFrame(data)
  df.to_csv(CONFIG['extraction_path'],  index=True, index_label='index', encoding='utf-8')
  workflow_logger.info("Data saved to file: %s", CONFIG['extraction_path'])
  return df

# Extract program
if __name__ == "__main__":
    """
    @TODO: Add docstring
    """
    extracted_data = extract_data()
    print(extracted_data)
    workflow_logger.info("Data extracted successfully.")
