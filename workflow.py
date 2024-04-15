from ETL.Extract.extract import extract_data
from ETL.Transform.transform import transform_data
from ETL.Load.load import load_data

import logging

# Main Workflow
def main_workflow():
  # Define log file name
  logfilename = f"Workflow.log"
  # Configure the logger
  logging.basicConfig(
      filename= logfilename,  # Specify the log file
      level=logging.DEBUG,   # Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
       # format the log messages keeping the timestamp, log level, filename, and the message always keeping the same characters count
      format='%(asctime)-20s - %(levelname)-10s - %(filename)-25s - %(message)-50s'
  )
  # Log the start of the workflow
  logging.info("Workflow started.")
  # Extract data
  extracted_data = extract_data()
  logging.info("Data extracted successfully.")
  
  # Transform data
  transformed_data = transform_data(extracted_data)
  logging.info("Data transformed successfully.")

  # Load data
  load_data(transformed_data)
  logging.info("Data loaded successfully.")

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
