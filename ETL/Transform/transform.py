from ETL.Transform.patient_transform import transform_patient

import pandas as pd
import concurrent.futures
import logging

# Configure logger
workflow_logger = logging.getLogger('workflow_logger')

def transform_data(data):
    """
    This function transforms the data and creates import scripts for the SQLite database.
    It uses a ThreadPoolExecutor to run the transformation of each patient in a separate thread.
    For that, it splits the data into patient specific data and submits the transformation of each patient to the executor.

    Args:
    data (pandas.DataFrame): The data to be transformed.

    Returns:
    None
    """
    ## Prepare import of patients
    # Get the name of the column that contains the patient ID
    id_col_name = data.columns[0]
    # Get number of patients
    list_of_patients = data[id_col_name].unique()
    number_of_patients = len(list_of_patients)
    workflow_logger.info("Number of patients: %s", str(number_of_patients))

    # Number of threads to run concurrently
    max_threads = 8

    # Preliminary data cleaning which could disrupt the transformation
    # replace all occurrences within the data
    # all "'" with "`" and '"' with "`"
    # all "(" with "{" and ")" with "}"
    data = data.replace("'", "`", regex=True)
    data = data.replace('"', "`", regex=True)
    data = data.replace("\(", ".(", regex=True)
    data = data.replace("\)", ").", regex=True)
    
    workflow_logger.info(f"Data cleaned:{data[data.values == '{']}")
 
    ## Create a ThreadPoolExecutor with a maximum of max_threads threads
    with concurrent.futures.ThreadPoolExecutor(max_threads) as executor:
        futures = []

        # Submit tasks to the executor for each patient in the list
        for record in list_of_patients:
            # Get data for each patient
            patient_df = data[data[id_col_name] == record]
            future = executor.submit(transform_patient, patient_df)
            futures.append(future)

        # Wait for all tasks to complete
        for future in concurrent.futures.as_completed(futures):
            future.result()
    return
