from ETL.Extract.extract import extract_data
from ETL.Transform.transform import transform_data
from ETL.Load.load import load_data

import logging

# Configure logger
workflow_logger = logging.getLogger('workflow_logger')
workflow_logger.setLevel(logging.INFO)
file_handler1 = logging.FileHandler('Workflow-debug.log')
formatter = logging.Formatter('%(asctime)-20s - %(levelname)-10s - %(filename)-25s - %(funcName)-25s %(message)-50s')
file_handler1.setFormatter(formatter)
workflow_logger.addHandler(file_handler1)

# Main Workflow
def main_workflow():
  """
  This function is the main workflow of the ETL process.
  It calls the extract_data, transform_data, and load_data functions.
  """
  # Log the start of the workflow
  workflow_logger.info("Workflow started.")
  # Extract data
  extracted_data = extract_data()
  workflow_logger.info("Data extracted successfully.")
  
  # Transform data
  transform_data(extracted_data)
  workflow_logger.info("Data transformed successfully.")

  # Load data
  load_data()
  workflow_logger.info("Workflow finished successfully.")

# Main program
if __name__ == "__main__":
  """
  This is the main program that will be executed when the script is run.
  It calls the main_workflow function to execute the ETL process.
  1) Extract data from the source REDCap database.
  2) Transform the data, according to the mapping rules.
  3) (Optional) Create the SQLite Database
  4) Load the transformed data into the destination database. SQLite in this case.
  """
  main_workflow()
