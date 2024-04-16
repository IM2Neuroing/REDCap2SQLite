from PyUtilities.setupFunctions import read_config_file
from redcap import Project
import pandas as pd
import logging


CONFIG_FILE_PATH = 'config.json'

def extract_data():
  """
  Function to extract data from the source.
  """
  
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
  # check if data is empty
  if data.empty:
    logging.error("No data was extracted from the source")
    exit()
  logging.debug(f"Data extracted: {data}")

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
  # Save data to a file using pandas
  df = pd.DataFrame(data)
  df.to_csv(config['extraction_path'], index=False)
  logging.info(f"Data extracted from REDCap: {df}")
  return
