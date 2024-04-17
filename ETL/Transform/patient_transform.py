import logging
import pandas as pd
import os
from PyUtilities.setupFunctions import read_config_file, csvs_reader
from PyUtilities.databaseFunctions import generate_insert_statement
from ETL.Transform.transform_utils import drop_rows_with_NULL, getRedCapValueROW, getAllOccurringAttributes

# load configuration file
CONFIG_FILE_PATH = 'config.json'
CONFIG = read_config_file(CONFIG_FILE_PATH)

def transform_patient(patient_df):
    """
    This function creates import-sqls for a patient. [Code to be executed in the thread]
    It creates patient-specific log files and logs the creation of the SQLs.
    Creates a initial SQL file for the patient.
    The CSVs reader is used to read the mapping files one by one and process them. One mapping file corresponds to one entity.
    It calls the create_imports_entity function to create import-sqls for each entity.

    Args:
    patient_df (pandas.DataFrame): The ONE patient data.
    """
    # Get the patient ID
    id_col_name = patient_df.columns[0]
    patient_id = patient_df[id_col_name].values[0]
    logging.info("PATIENT: Prepare Import script Patient %s", patient_id)

    # check if data directory exists or create it
    if not os.path.exists(f'{CONFIG["data_path"]}/Patients/Patient-{patient_id}'):
        os.makedirs(f'{CONFIG["data_path"]}/Patients/Patient-{patient_id}')

    # Create the patient-specific SQL file
    with open(f'{CONFIG["data_path"]}/Patients/Patient-{patient_id}/Patient-{patient_id}.sql', 'w') as f:
        f.write(f'-- Patient: {patient_id}\n')
        
    ## SETUP Patient LOGGING
    # Configure patient logger
    plogger = logging.getLogger('PatientLogger')
    plogger.setLevel(logging.DEBUG)
    file_patient = logging.FileHandler(f'{CONFIG["data_path"]}/Patients/Patient-{patient_id}/Patient-{patient_id}.log')
    formatter = logging.Formatter('%(asctime)-20s - %(levelname)-10s - %(filename)-25s - %(funcName)-25s %(message)-50s')
    file_patient.setFormatter(formatter)
    plogger.addHandler(file_patient)
    plogger.info("PATIENT %s", patient_id)

    # Read CSVs and create a generator
    csv_mapping_reader = csvs_reader(CONFIG['mapping_path'])

    # Use a while loop to read and process each CSV file
    while True:
        try:
            entity_df = next(csv_mapping_reader)
            create_imports_entity(patient_df,entity_df,plogger)
        except StopIteration:
            break


def create_imports_entity(patient_df,mapping,plogger):
    """
    This function creates all import-sqls for an entity.
    It creates a subset of the patient data based on the found attributes in the mapping file.
    It checks if the subset is empty and logs a warning if it is.
    It checks if the subset contains the 'redcap_repeat_instance' column.
    If it does not contain the 'redcap_repeat_instance' column, it calls the build_SQL_for_single_entity function.
    If it contains the 'redcap_repeat_instance' column, it splits the DataFrame based on the 'redcap_repeat_instance' column.
    It then calls the create_imports_repeat function for each repeat.

    Args:
    patient_df (pandas.DataFrame): The patient data.
    mapping (pandas.DataFrame): The mapping data of ONE entity.
    plogger (logging.Logger): The logger for the patient.
    """
    # Start import of entity
    plogger.debug("------------------------------------")
    plogger.debug("ENTITY: Start SQL creation of entity: %s",mapping["Table"][0])
    plogger.debug("------------------------------------")

    # shorten mapping table / Drop those which are not assigned
    mapping = mapping.dropna(subset=['field_name'])
    # clean mapping table
    mapping = mapping[~mapping["field_name"].str.contains('AUTO', case=True)]

    if mapping.empty:
        plogger.warning(f"ENTITY: No field_name in mapping table (after dropna & AUTO remove) for entity: {mapping['Table'][0]}")
        return
    # Create a List with all elements in mapping["field_name"]
    entity_elements_to_filter = getAllOccurringAttributes(list(mapping["field_name"]))
    # Create a subset of the PatientDataFrame With only the elements in entity_elements_to_filter
    patient_subset_df = patient_df[patient_df['field_name'].isin(entity_elements_to_filter)]

    # Check if the patient_subset_df is empty
    if patient_subset_df.empty:
        plogger.warning(f"ENTITY: No subset df for entity: {mapping['Table'][0]}, does not contain any elements from mapping")
        return
    
    # Check if the patient_subset_df contains the 'redcap_repeat_instance' column
    if 'redcap_repeat_instance' not in patient_subset_df.columns:
        plogger.debug("ENTITY: No repeats found")
        build_SQL_for_single_entity(patient_subset_df,patient_df,mapping,plogger)
        return
    
    # Otherwise, the patient_subset_df contains the 'redcap_repeat_instance' column
    # Splitting the DataFrame based on the 'redcap_repeat_instance' column
    repeats = patient_subset_df.groupby('redcap_repeat_instance')
    plogger.debug("ENTITY: How many repeats: %s",len(repeats))
    # each repeat creates a single entity in SQLite
    for num, repeat in repeats:
        create_imports_repeat(num,repeat,patient_df,mapping,plogger)
    plogger.debug("------------------------------------")

def create_imports_repeat(num,entity_repeat,patient_df,mapping,plogger):
    """
    This function creates all import-sqls for a repeat.
    It checks if the mapping file contains a 'MULT' field.
    If it contains a 'MULT' field, it splits the DataFrame based on the 'MULT' field.
    It then calls the build_SQL_for_single_entity function for each split.
    If it does not contain a 'MULT' field, it calls the build_SQL_for_single_entity function for the repeat.
    
    Args:
    num (int): The repeat number.
    entity_repeat (pandas.DataFrame): The repeat data.
    patient_df (pandas.DataFrame): The patient data.
    mapping (pandas.DataFrame): The mapping data of ONE entity.
    plogger (logging.Logger): The logger for the patient.
    """
    plogger.debug("REPEAT: import entity %s \tNumber: %s",mapping["Table"][0],num)
    plogger.debug("REPEAT: \n%s",entity_repeat)

    # check if any row has a "MULT" for multiple values (in this case several entities are created)
    if mapping["field_name"].str.contains('MULT', case=True).any():
        # build and import multiple entities
        plogger.debug("REPEAT: MULT found")
        plogger.debug("REPEAT: MappingTable: \n%s",mapping)
        build_SQL_for_multiple_entity(entity_repeat,patient_df,mapping,plogger)
        
    else:
        # build and import single entity
        plogger.debug("REPEAT: No MULT found")
        plogger.debug("REPEAT: MappingTable: \n%s",mapping)
        build_SQL_for_single_entity(entity_repeat,patient_df,mapping,plogger)

def build_SQL_for_multiple_entity(multi_entity_repeat,patient_df,mapping,plogger):
    """
    This function creates all import-sqls for multiple entities.
    It gets the 'MULT' field from the mapping file.
    It splits the DataFrame based on the 'MULT' field.
    It creates a DataFrame for each row with the 'MULT' field.
    It calls the build_SQL_for_single_entity function for each resulting DataFrame.

    Args:
    multi_entity_repeat (pandas.DataFrame): The repeat data.
    patient_df (pandas.DataFrame): The patient data.
    mapping (pandas.DataFrame): The mapping data of ONE entity.
    plogger (logging.Logger): The logger for the patient.
    """
    plogger.debug("REPEAT: MULT found")
    # get the MULT field | Specify the value in the 'fieldname' column to consider
    uncleanedspecified_value = mapping[mapping["field_name"].str.contains('MULT', case=True)]["field_name"].values[0]
    # clean the MULT value IF(XXX MULT(YYY),XXX,"*") -> YYY
    specified_value = uncleanedspecified_value.split("MULT(")[1].split(")")[0]
    plogger.debug("REPEAT: specified_value %s",specified_value)
    # Find the indices where the specified value appears in the 'fieldname' column
    indices = multi_entity_repeat[multi_entity_repeat['field_name'] == specified_value].index

    # Initialize an empty list to store the resulting DataFrames
    mult_dfs = []

    # Iterate through the indices and create a DataFrame for each row with the specified value
    for idx in indices:
        # Exclude the row with the specified value
        single_entity_repeat_df = multi_entity_repeat.copy()
        
        # Exclude rows with the specified value except for the current index
        single_entity_repeat_df = single_entity_repeat_df.drop(indices.difference([idx]))

        # Reset the index to start from 0
        single_entity_repeat_df.reset_index(drop=True, inplace=True)
        
        # Append the resulting DataFrame to the list
        mult_dfs.append(single_entity_repeat_df)
    plogger.debug("REPEAT: mult_dfs \n%s",mult_dfs)

    # build And Import SingleEntity with the resulting DataFrames
    for i, single_entity_repeat_df in enumerate(mult_dfs, start=1):
        plogger.debug("REPEAT: single_entity_repeat_df \n%s",single_entity_repeat_df)
        build_SQL_for_single_entity(single_entity_repeat_df,patient_df,mapping,plogger)
    
def build_SQL_for_single_entity(single_entity_repeat,patient_df,mapping,plogger):
    """
    This function creates an import-sql for a single entity.
    It gets the patient ID.
    It creates a variable to fill with information.
    It defines the entity name.
    It adds values from REDCap to the entity.
    It checks if all rows with 'NOT NULL' have a value in the 'value' column.
    It drops rows with 'NULL'.
    It drops all rows if any value equals 'DROP'.
    It drops rows with 'NaN'.
    It logs the entity.
    It skips empty entities.
    It extracts two columns as a dictionary.
    It generates the insert statement for the table.
    It writes the insert statement to the SQL file.

    Args:
    single_entity_repeat (pandas.DataFrame): The repeat data.
    patient_df (pandas.DataFrame): The patient data.
    mapping (pandas.DataFrame): The mapping data of ONE entity.
    plogger (logging.Logger): The logger for the patient.
    """
    # Get the patient ID
    id_col_name = patient_df.columns[0]
    patient_id = patient_df[id_col_name].values[0]
    sql_file = f'{CONFIG["data_path"]}/Patients/Patient-{patient_id}/Patient-{patient_id}.sql'

    # create a variable to fill with information
    entity = mapping.copy()
    # define entityname
    entity_name = entity["Table"].values[0]
    # add values from redcap to entity
    entity["value"] =  entity.apply(lambda row: getRedCapValueROW(row,single_entity_repeat,patient_df,plogger), axis=1)

    # check if all rows with NOTNULL have a value in column value
    mask = (entity["NotNull"] == "NOT NULL") & ((entity["value"] == 'NULL')|entity["value"].isnull())
    if mask.any():
        plogger.warning("SINGLE ENTITY: NOTNULL check failed in entity: " + entity_name
        + " \nhas no value provided from redcap: \n" + str(entity[mask]["Attribute"].astype(str))
        + " \n for subject: " + str(patient_df[patient_df["field_name"] == "patient_id"]["value"].values[0]))
        entity["value"] = None


    # drop rows with NULL
    entity = drop_rows_with_NULL(entity, "value")

    # drop all rows it any value equals "DROP"
    # Check if there are DROP values
    if entity["value"].str.contains('DROP', case=True).any():
        entity["value"] = None

    # drop rows with NaN
    entity = entity.dropna(subset=['value'])

    plogger.info('SINGLE ENTITY: ENITIY - {} \n{}'.format(entity_name,entity.to_string()))

    # Skip empty entities
    if not entity.empty:
        # Extract two columns as a dictionary
        entityDict = dict(zip(entity['Attribute'], entity['value']))
        # Generate the insert statement for the table
        insert_statement = generate_insert_statement(entity_name, entityDict)
        plogger.info('SINGLE ENTITY: INSERT STATEMENT - {}'.format(insert_statement))

        # Write the insert statement to the SQL file
        with open(sql_file, 'a') as f:
            f.write(insert_statement + '\n')
        