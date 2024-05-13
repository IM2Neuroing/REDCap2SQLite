from PyUtilities.setupFunctions import read_config_file
from PyUtilities.databaseFunctions import create_database, execute_sql_script, data_check

import logging
import os

CONFIG_FILE_PATH = 'config.json'
CONFIG = read_config_file(CONFIG_FILE_PATH)
# Configure logger
workflow_logger = logging.getLogger('workflow_logger')

def load_data():
    """
    Function to load data into the destination database.
    """
    ## DATABASE CREATION
    workflow_logger.info("Database setup started.")
    database_setup()
    workflow_logger.info("Database setup completed.")

    ## DATA LOADING
    workflow_logger.info("Data loading started.")
    load_data_into_database()
    workflow_logger.info("Data loaded into the database.")

    ## CHECK IF DATA LOADED
    data_check(CONFIG['db_path'])

# Database setup Function
def database_setup():
    """
    Function to create a new SQLite database if the config file specifies it.
    """
    # Check if a new database should be created
    if CONFIG['db_creation'] is True:
      # Check if db_path CONFIG from file is valid
      if CONFIG['db_path'] is None:
        workflow_logger.error("No database path was specified in the config file")
        exit()
      # Check if db_schema CONFIG from file is valid
      if CONFIG['db_schema'] is None:
        workflow_logger.error("No database schema (SQL file) was specified in the config file")
        exit()
      # Check if the database already exists
      if os.path.exists(CONFIG['db_path']):
        workflow_logger.warning("Database already exists: %s", CONFIG['db_path'])
        # Check if the database should be wiped
        if CONFIG['db_wipe'] is True:
          create_database(CONFIG['db_path'], CONFIG['db_schema'], wipe=True)
          workflow_logger.info("Database wiped: %s", CONFIG['db_path'])
        else:
          workflow_logger.info("Database have not been wiped: %s", CONFIG['db_path'])
      else:
        create_database(CONFIG['db_path'], CONFIG['db_schema'])
        workflow_logger.info("Database created: %s", CONFIG['db_path'])

# Load data into database Function
def load_data_into_database():
    """
    Function to load the data into the destination database.
    Check if the sqlite database is created.
    Check if sql files are provided, in the Patients folder.
    Execute the sql files to load the data into the database.
    """

    # Check if the data should be loaded into the database
    if CONFIG['db_load_data'] is True:
      # Check if data_path CONFIG from file is valid
      if CONFIG['data_path'] is None:
        workflow_logger.error("No data path was specified in the config file")
        exit()
      # Check if there is a Patients folder in the data folder CONFIG["data_path"]}/Patients
      if not os.path.exists(f"{CONFIG['data_path']}/Patients"):
        workflow_logger.error("No Patients folder was found in the data path")
        exit()
      # Check if there are any patient folder with sql files in the Patients folder
      if not os.listdir(f"{CONFIG['data_path']}/Patients"):
        workflow_logger.error("No patient folders were found in the Patients folder")
        exit()
      # Check if the database path is valid
      if CONFIG['db_path'] is None:
        workflow_logger.error("No database path was specified in the config file")
        exit()
      
      # List all SQL files in the Patients folder and subfolders
      sql_files = list_sql_files()
    
      # Execute the SQL files
      for sql_file in sql_files:
        with open(sql_file, 'r') as file:
          sql = file.read()
          # Execute the SQL
          result = execute_sql_script(sql,CONFIG['db_path'])
          workflow_logger.info("SQL file %s executed %s", sql_file, result)

      workflow_logger.debug("Data loaded into SQLite Database")

def list_sql_files():
    """
    Function to list all SQL files in the Patients folder and subfolders.
    """
    sql_files = []
    for root, dirs, files in os.walk(f"{CONFIG['data_path']}/Patients"):
      for file in files:
        if file.endswith(".sql"):
          sql_files.append(os.path.join(root, file))
    return sql_files