import sqlite3
import re
import os
import pandas as pd
import logging

# Configure logger
workflow_logger = logging.getLogger('workflow_logger')

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
        if wipe:
            wipe_sqlite_database(database_name)
            workflow_logger.info("Database wiped: %s", database_name)
        # If the database already exists, return
        else:
            workflow_logger.info("Database have not been wiped: %s", database_name)
            return None
        
    try:
        # Create a connection to the database
        conn = sqlite3.connect(database_name)

        # Create a cursor object to interact with the database
        cursor = conn.cursor()

        # Open the SQL script file and read the content
        with open(database_sql, 'r') as sql_file:
            script = sql_file.read()
        workflow_logger.debug("Database schema loaded")

        # Execute the SQL script to initialize the database
        cursor.executescript(script)
        conn.commit()
        workflow_logger.info("Database created: %s", database_name)

    except sqlite3.Error as e:
        workflow_logger.error("Database Creation: An error occurred: SQL-shema", e)
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
            workflow_logger.debug(f"Table {table_name[0]} dropped")

        # Commit the changes and close the connection
        conn.commit()
        conn.close()
        workflow_logger.debug("Database wiped: %s", db_name)

    except sqlite3.Error as e:
        workflow_logger.error("Database Creation (Wiping-step): An error occurred:", e)
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
    values = ', '.join(["'" + str(value) + "'" for value in data.values()])
    # remove ' before and after numeric values to avoid SQL errors
    values = re.sub(r"'(\d+(\.\d+)?)'", r"\1", values)
    # remove ' before opening brackets to avoid SQL errors
    values = re.sub(r"'(\()", r'\1', values)
    # remove ' after closing brackets to avoid SQL errors
    values = re.sub(r"(\))'", r'\1', values)

    insert_statement = f"INSERT OR IGNORE INTO {table_name} ({columns}) VALUES ({values});"
    
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
    # add ' before and after
    redcapvalues = [f"'{value}'" for value in redcapvalues]
    # remove ' before and after numeric values to avoid SQL errors
    redcapvalues = [re.sub(r"'(\d+(\.\d+)?)'", r'\1', value) for value in redcapvalues]
    # remove ' before opening brackets to avoid SQL errors
    redcapvalues = [re.sub(r"'(\()", r'\1', value) for value in redcapvalues]
    # remove ' after closing brackets to avoid SQL errors
    redcapvalues = [re.sub(r"(\))'", r'\1', value) for value in redcapvalues]

    sql_statement = f"(SELECT {searched_attribute} FROM {table} WHERE {attributes[0]} = {redcapvalues[0]}"
    for attribute, redcapvalue in zip(attributes[1:], redcapvalues[1:]):
        sql_statement += f" AND {attribute} = {redcapvalue}"
    sql_statement += ")"
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
            workflow_logger.debug(f"Statement:{sql_statement}: ran successfully")
            return "Statement executed successfully."

    except sqlite3.Error as e:
        workflow_logger.error(f"Statement:{sql_statement}: failed: {e}")
        return f"Statement:{sql_statement}: failed: {e}"

    finally:
        # Close the database connection
        cursor.close()
        conn.close()

def execute_sql_script(sql_script, db_file):
    """
    This function executes an SQL script on a SQLite database.

    Args:
    sql_script (str): The SQL script to be executed.
    db_file (str): The path to the SQLite database.

    Returns:
    None
    """
    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    try:
        # Execute the SQL script
        cursor.executescript(sql_script)
        conn.commit()
        return "successfully."

    except sqlite3.Error as e:
        workflow_logger.exception("SQL script execution failed:", e)
        return e

    finally:
        # Close the database connection
        cursor.close()
        conn.close()

# Data check Function
def data_check(db_file):
    """
    Function to check if the data was loaded into the database.
    It retrieves the number of rows in each table and logs the result.
    :param db_file: complete file path to db
    :return: None
    """
    # Connect to the SQLite database
    conn = sqlite3.connect(db_file)
    
    # Create a cursor object
    cursor = conn.cursor()
    
    # Execute the SQL query to retrieve all table names
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    
    # Fetch the result of the query
    tables = cursor.fetchall()
    
    # For each table, execute a SQL query to count the number of rows
    for table in tables:
      cursor.execute(f"SELECT COUNT(*) FROM {table[0]}")
      count = cursor.fetchone()[0]
      workflow_logger.info(f"Data check: Table {table[0]} has {count} rows.")
    
    # Close the connection to the database
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