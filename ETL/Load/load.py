from PyUtilities.setupFunctions import read_config_file
from PyUtilities.databaseFunctions import create_database

import logging

CONFIG_FILE_PATH = 'config.json'

def load_data():
  """
  Function to load data into the destination database.
  """
  # load configuration file
  config = read_config_file(CONFIG_FILE_PATH)

  ## DATABASE CREATION
  database_setup(config)

  ## DATA LOADING

  # Implement your logic to write data to the destination (e.g., CSV file)
  logging.info(f"Writing data: {None}")

# Database setup Function
def database_setup(config):
  """
  Function to create a new SQLite database if the config file specifies it.
  """
  # Check if a new database should be created
  if config['db_creation'] is True:
    # Check if db_path config from file is valid
    if config['db_path'] is None:
      logging.error("No database path was specified in the config file")
      exit()
    # Check if db_schema config from file is valid
    if config['db_schema'] is None:
      logging.error("No database schema (SQL file) was specified in the config file")
      exit()
    
    # Create database
    db_filename = config['db_path']
    df_schema = config['db_schema']
    #db_filename = add_timestamp_to_filename(db_filename)
    create_database(db_filename, df_schema, wipe=True)
    logging.info("Empty SQLite Database initialized")