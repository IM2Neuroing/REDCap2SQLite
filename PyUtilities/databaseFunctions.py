import sqlite3
import logging
import re
import os
import pandas as pd

def create_database(database_name, database_sql ,wipe=False):
    """
    This function creates a new SQLite database using the provided SQL schema.

    Args:
    database_name (str): The name of the database.
    database_sql (str): The path to the SQL schema file.
    wipe (bool): A flag to indicate if the database should be wiped if it already exists.

    Returns:
    None
    """

    # First check if the database already exists
    if os.path.exists(database_name):
        logging.warning("Database already exists: %s", database_name)
        if wipe:
            logging.warning("Wiping database")
            wipe_sqlite_database(database_name)
        # If the database already exists, return
        return None
        
    try:
        # Create a connection to the database
        conn = sqlite3.connect(database_name)

        # Create a cursor object to interact with the database
        cursor = conn.cursor()

        # Open the SQL script file and read the content
        with open(database_sql, 'r') as sql_file:
            script = sql_file.read()
        logging.debug("Database schema loaded")

        # Execute the SQL script to initialize the database
        cursor.executescript(script)
        conn.commit()
        logging.debug("Database created: %s", database_name)

    except sqlite3.Error as e:
        logging.error("Database Creation: An error occurred: SQL-shema", e)
        return None
    
    finally:
        # Close the database connection
        conn.close()


def wipe_sqlite_database(db_name):
    """
    This function wipes all tables in a SQLite database.

    Args:
    db_name (str): The name of the SQLite database.

    Returns:
    None
    """

    try:
        # Connect to the SQLite database
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        # Get all table names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name != 'sqlite_sequence';")
        tables = cursor.fetchall()
        
        # Drop all tables
        for table_name in tables:
            cursor.execute(f"DROP TABLE {table_name[0]};")
            logging.debug(f"Table {table_name[0]} dropped")

        # Commit the changes and close the connection
        conn.commit()
        conn.close()
        logging.debug("Database wiped: %s", db_name)

    except sqlite3.Error as e:
        logging.error("Database Creation (Wiping-step): An error occurred:", e)
        return None

    finally:
        # Close the database connection
        conn.close()

def generate_insert_statement(table_name, data):
    """
    This function generates an insert statement for a given table and data.

    Args:
    table_name (str): The name of the table.
    data (dict): The data to be inserted.

    Returns:
    str: The insert statement.
    """
    columns = ', '.join(data.keys())
    values = ', '.join(['"' + str(value) + '"' for value in data.values()])
    
    insert_statement = f"INSERT INTO {table_name} ({columns}) VALUES ({values})"
    
    return insert_statement

def generate_search_statement(searched_attribute,table,attributes,redcapvalues):
    """
    This function generates an search statement for a given table and data.

    Args:
    searched_attribute (str): The name of the searched attribute.
    table (str): The name of the table.
    attributes (list): The attributes to be searched.
    redcapvalues (list): The values to be searched.

    Returns:
    str: The search statement.
    """
    sql_statement = f"SELECT {searched_attribute} FROM {table} WHERE {attributes[0]} = '{redcapvalues[0]}'"
    for attribute, redcapvalue in zip(attributes[1:], redcapvalues[1:]):
        sql_statement += f" AND {attribute} = '{redcapvalue}'"
    sql_statement += ";"
    return fix_sql_query(sql_statement)

def execute_sql_statement(sql_statement, db_file):
    """
    :param db_file: complete file path to db
    :param sql_statement: insert statement creating the new entry if it doesn't exist yet
    :return: None or the result of the query
    """
    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    try:
        # Execute the SQL statement
        cursor.execute(sql_statement)

        # If the statement is a query, fetch all rows
        if sql_statement.strip().lower().startswith("select"):
            rows = cursor.fetchall()
            return rows

        # If the statement is an update, delete, or insert, commit the changes
        else:
            conn.commit()
            logging.debug(f"Statement:{sql_statement}: ran successfully")
            return "Statement executed successfully."

    except sqlite3.Error as e:
        logging.error(f"Statement:{sql_statement}: failed: {e}")
        return f"Statement:{sql_statement}: failed: {e}"

    finally:
        # Close the database connection
        cursor.close()
        conn.close()


def fix_sql_query(sql_query):
    """
    Fix some errors that occur from automatically constructing the query strings
    1) convert f" 'None'  " to f" None  " otherwise the value is not added as None-type to the db but as 'None'-string
    2)
    :param sql_query:
    :return: fixed_query:
    """

    # Use regular expressions to replace 'NULL' with 'IS NULL' while preserving single quotes
    fixed_query = re.sub(r"= 'NULL'", "IS NULL", sql_query)
    return fixed_query


def db_insert_if_not_exists(db_path, check_query, insertion_query):
    """

    :param db_path: complete file path to db
    :param check_query: select statement used to check if an entry with the same properties already exist
    :param insertion_query: insert statement creating the new entry if it doesn't exist yet
    :return: inserted or already available data is returned as dataframe
    """
    sqlite_connection = None
    df_out = None

    # make some critical string fixes
    check_query = query_string_fixes(check_query)
    insertion_query = query_string_fixes(insertion_query)
    logging.debug(f'db_insert_if_not_exists: check query:\n{check_query}')
    logging.debug(f'db_insert_if_not_exists: insertion query:\n{insertion_query}')

    # open the db connection, handle exceptions and finally close the db connection
    try:
        # establish and configure connection to db
        sqlite_connection = sqlite3.connect(db_path)
        sqlite_connection.row_factory = sqlite3.Row
        cursor = sqlite_connection.cursor()

        # check if data exists
        cursor.execute(check_query)
        cursor_data = cursor.fetchall()

        logging.debug(f'{[i for i in cursor_data]}')

        if len(cursor_data) == 0:
            # data does not exist - insert
            logging.debug("record does not exist in db, insert")
            cursor.execute(insertion_query)
            # df_out = pd.read_sql_query(insertion_query, sqlite_connection)  # to get the inserted row back
        else:
            logging.debug("record already exists in db, no changes to db")

        # TODO: new: get final df_out
        df_out = pd.read_sql_query(check_query, sqlite_connection)

        # commit the insertion to the db
        sqlite_connection.commit()

        if df_out is None:
            exit(f"db_insert_if_not_exists: Failed to update sqlite table, returned df == None")

    except sqlite3.Error as error:
        exit(f"db_insert_if_not_exists: Failed to update sqlite table - {error}")

    finally:
        if sqlite_connection:
            sqlite_connection.close()
            logging.debug("db_insert_if_not_exists: The SQLite connection is closed")
            logging.debug("db_insert_if_not_exists: ran successfully")

    return df_out


def db_select_entries(db_path, query, no_quickfix=False):
    """

    :param db_path: complete file path to db
    :param query: select statement to find entries based
    :return: found data is returned as dataframe
    """
    sqlite_connection = None
    df_out = None

    # make some critical string fixes
    if no_quickfix is False:
        query = query_string_fixes(query)

    # open the db connection, handle exceptions and finally close the db connection
    try:
        # establish and configure connection to db
        sqlite_connection = sqlite3.connect(db_path)
        df_out = pd.read_sql_query(query, sqlite_connection)

        logging.debug("runSqliteSearch: ran successfully")

    except sqlite3.Error as error:
        exit(f"runSqliteSearch: Failed to update sqlite table - {error}")

    finally:
        if sqlite_connection:
            sqlite_connection.close()
            logging.debug("runSqliteSearch: The SQLite connection is closed")

        return df_out


def db_update_entry(db_path, check_query, update_query):
    """

    :param db_path: complete file path to db
    :param check_query: select statement used to check if an entry with the same properties already exist
    :param update_query: statement to update the fields with new values of an existing entry
    :return: dataframe of updated entry
    """

    sqlite_connection = None
    df_out = None

    # make some critical string fixes
    check_query = query_string_fixes(check_query)
    update_query = query_string_fixes(update_query, is_update_query=True)

    # open the db connection, handle exceptions and finally close the db connection
    try:
        # establish and configure connection to db
        sqlite_connection = sqlite3.connect(db_path)
        # TODO: new
        cursor = sqlite_connection.cursor()

        # update the desired entry
        cursor.execute(update_query)

        # get final df_out
        df_out = pd.read_sql_query(check_query, sqlite_connection)

        # commit the insertion to the db
        sqlite_connection.commit()

    except sqlite3.Error as error:
        exit(f"db_update_entry: Failed to update sqlite table - {error}")

    finally:
        if sqlite_connection:
            sqlite_connection.close()
            logging.debug("db_update_entry: The SQLite connection is closed")

            if df_out is None:
                exit(f"db_update_entry: the entry does not exist and can therefore not be updated, query:"
                     f"\n{update_query}")
            elif len(df_out) == 0:
                exit(f"db_update_entry: the entry does not exist and can therefore not be updated, query:"
                     f"\n{update_query}")

            logging.debug("db_update_entry: ran successfully")

        return df_out


def db_insert_or_update_if_exists(db_path, check_query, insertion_query, update_query):
    """

    :param update_query: statement to update the fields with new values of an existing entry
    :param db_path: complete file path to db
    :param check_query: select statement used to check if an entry with the same properties already exist
    :param insertion_query: insert statement creating the new entry if it doesn't exist yet
    :return: inserted or already available data is returned as dataframe
    """

    sqlite_connection = None
    df_out = None

    # make some critical string fixes
    check_query = query_string_fixes(check_query)
    insertion_query = query_string_fixes(insertion_query)
    update_query = query_string_fixes(update_query, is_update_query=True)

    logging.debug(f'db_insert_or_update_if_exists, insert query:\n{insertion_query}')

    # # open the db connection, handle exceptions and finally close the db connection
    try:
        # establish and configure connection to db
        sqlite_connection = sqlite3.connect(db_path)
        sqlite_connection.row_factory = sqlite3.Row

        # TODO: new
        cursor = sqlite_connection.cursor()

        # check if data exists
        cursor.execute(check_query)
        cursor_data = cursor.fetchall()
        # cursor.close()
        # cursor = sqlite_connection.cursor()
        if len(cursor_data) == 0:
            # data does not exist - insert
            logging.debug("record does not exist in db, insert")
            # TODO: new
            cursor.execute(insertion_query, ())

            # cursor.execute(check_query)
            # cursor_data = cursor.fetchall()
            # print(f'cursor data: {cursor_data}')
            # df_out = pd.read_sql_query(insertion_query, sqlite_connection)
        else:
            # data already exists - update
            logging.debug("record already exists in db, update the desired values")
            # TODO: new
            cursor.execute(update_query, ())
            # df_out = pd.read_sql_query(update_query, sqlite_connection)

        # # commit the insertion to the db
        # sqlite_connection.commit()
        #
        # # TODO: new: get final df_out
        # # sqlite_connection.close()
        # # sqlite_connection = sqlite3.connect(db_path)
        df_out = pd.read_sql_query(check_query, sqlite_connection)

        # commit the insertion to the db
        sqlite_connection.commit()

        if df_out is None:
            exit(f"db_insert_or_update_if_exists: Failed to update sqlite table, returned df == None")

        logging.debug("db_insert_if_not_exists: ran successfully")

    except sqlite3.Error as error:
        exit(f"db_insert_or_update_if_exists: Failed to update sqlite table - {error}")

    except Exception as e:
        exit(f"db_insert_or_update_if_exists: General Exception - {e}")

    finally:
        if sqlite_connection:
            sqlite_connection.close()
            logging.debug("db_insert_if_not_exists: The SQLite connection is closed")

    return df_out


def parse_insert_query_from_1row_df(df_sliced_in, db_table_name, additional_fields_dict):
    """
    Builds a db insertion query from the provided table name, with the additional fields & values dict and setting
        the values from the dataframe.
    :param df_sliced_in: dataframe with exactly ONE col -> index ('row' names) should correspond to db entity fields
        (this type of df can be extracted from a multirow-multicolumn df by eg df.oc[0])
    :param db_table_name: the name of the db table/entity to be updated
    :param additional_fields_dict: additional field names and values to add to the db entry
    :return: string with the insertion query
    """

    # add the PKs to the df
    df_sliced = df_sliced_in.copy()
    for field_name, field_value in additional_fields_dict.items():
        df_sliced[field_name] = field_value

    # insert query pre-definitions
    # eg: f"INSERT INTO stimulation_status (stimulation_status) VALUES ('{stimulation_status}')"
    str_start = f"INSERT INTO {db_table_name} ("
    str_middle = f") VALUES ("
    str_end = f")"
    # str_end = f") RETURNING *"

    # combine insert query
    counter = 0
    for field_name, value in df_sliced.items():
        # add the field names and values at the respective positions in correct format (single quotes for values)
        str_start += field_name
        str_middle += f"'{value}'"
        # while not at the last field/value add dividers aka ', '
        if counter < (len(df_sliced) - 1):
            str_start += ", "
            str_middle += ", "
        counter += 1
    # the final query is the combination of the three string parts
    insert_query = str_start + str_middle + str_end
    logging.debug(f'parse_insert_query_from_df: {insert_query}')

    return insert_query


def parse_update_query_from_1col_df(df_sliced_in, db_table_name, unique_field_combo_dict):
    """
    Builds a db update query from the provided table name, with the unique field combo as search fields and setting
        the values from the dataframe.
    :param df_sliced_in: dataframe with exactly ONE col -> index ('row' names) should correspond to db entity fields
        (this type of df can be extracted from a multirow-multicolumn df by eg df.oc[0])
    :param db_table_name: the name of the db table/entity to be updated
    :param unique_field_combo_dict: field names and values to select the row to be updated
    :return: string with the update query
    """

    df_sliced = df_sliced_in.copy()

    # update query pre-definitions
    # eg: f"UPDATE subject SET center_name='{center_name}', patient_id_acr='{patient_id_acr}' " \
    #     f"WHERE redcap_record_id='{redcap_record_id}'"
    str_start = f"UPDATE {db_table_name} SET "
    str_end = f" WHERE "

    # COMBINE UPDATE QUERY
    # add the field values to set
    counter = 0
    for field_name, value in df_sliced.items():
        # add the field names and values at the respective positions in correct format (single quotes for values)
        str_start += f"{field_name}='{value}'"
        # while not at the last field/value add dividers aka ', '
        if counter < (len(df_sliced) - 1):
            str_start += ", "
        counter += 1
    # add the unique identifier field combos in the WHERE clause
    counter = 0
    for field_name, value in unique_field_combo_dict.items():
        # add the field names and values at the respective positions in correct format (single quotes for values)
        str_end += f"{field_name}='{value}'"
        # while not at the last field/value add dividers aka ', '
        if counter < (len(unique_field_combo_dict) - 1):
            str_end += " AND "
        counter += 1

    # the final query is the combination of the three string parts
    update_query = str_start + str_end
    logging.debug(f'parse_update_query_from_df: {update_query}')

    return update_query


def parse_select_query_from_field_value_dict(db_table_name, unique_field_combo_dict):
    """
    Builds a db select query from the provided table name, with the unique field combo as search fields.
    :param db_table_name: string denoting the db entity to select from
    :param unique_field_combo_dict: dictionary containing the field names and values to match in the selection
    :return: string with the selection query
    """

    # check query pre-definitions
    # eg: f"SELECT * FROM stimulation_status WHERE stimulation_status='{stimulation_status}'"
    select_query = f"SELECT * FROM {db_table_name} WHERE "

    # combine check query
    counter = 0
    for field_name, value in unique_field_combo_dict.items():
        # add the field names and values at the respective positions in correct format (single quotes for values)
        select_query += f"{field_name}='{value}'"
        # while not at the last field/value add dividers aka ', '
        if counter < (len(unique_field_combo_dict) - 1):
            select_query += " AND "
        counter += 1
    logging.debug(f'parse_insert_query_from_df: {select_query}')

    return select_query


def parse_db_queries_from_df_and_pks(df_sliced_in, db_table_name, unique_field_combo_dict):
    """
    Builds a db update query from the provided table name, with the unique field combo as search fields and setting
        the values from the dataframe.
    :param df_sliced_in: dataframe with exactly ONE col -> index ('row' names) should correspond to db entity fields
        (this type of df can be extracted from a multirow-multicolumn df by eg df.oc[0])
    :param db_table_name: the name of the db table/entity to be updated
    :param unique_field_combo_dict: field names and values to select the row to be updated
        (THESE FIELDS SHOULD NOT BE CONTAINED IN df_sliced_in)
    :return: 3 strings: insert-, selection-, and update-query
    """
    df_sliced = df_sliced_in.copy()
    insert_query = parse_insert_query_from_1row_df(df_sliced, db_table_name, unique_field_combo_dict)
    select_query = parse_select_query_from_field_value_dict(db_table_name, unique_field_combo_dict)
    update_query = parse_update_query_from_1col_df(df_sliced, db_table_name, unique_field_combo_dict)

    return insert_query, select_query, update_query


def check_if_value_exists_in_db(db_path, table_name, field_name, field_value):
    """
    Builds a selection query checking if the provided field_value exists for the provided table_name & field
    :param db_path: as string
    :param table_name: table from db
    :param field_name: field from db
    :param field_value: value to look for
    :return: df with the found entries
    """

    db_query = f"SELECT {field_name} FROM {table_name} WHERE {field_name}='{field_value}'"
    df_out = db_select_entries(db_path, db_query)

    return df_out


def query_string_fixes(in_str: str, is_update_query=False):
    """
    Fix some errors that occur from automatically constructing the query strings
    1) convert f" 'None'  " to f" None  " otherwise the value is not added as None-type to the db but as 'None'-string
    2)
    :param in_str:
    :return: out_str:
    """

    out_str = in_str.replace("'None'", "NULL")
    if is_update_query is False:
        out_str = out_str.replace("=NULL", " IS NULL")

    return out_str
