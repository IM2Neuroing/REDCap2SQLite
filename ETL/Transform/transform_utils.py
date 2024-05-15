from PyUtilities.databaseFunctions import generate_search_statement
import pandas as pd

def getAllOccurringAttributes(field_name_list):
    '''
    Extract all different attributes from the field_name_list
    Attributes w, x, y, z are to be extracted from field_names like "SRCH(w, SET(x), y, z)", "__IF(w, SRCH(w, SET(x), y, z), y, RADI(z))" or "SET(x)"
    Attributes are returned as a list

    Args:
    field_name_list (list): List of field names

    Returns:
    list: List of attributes
    '''
    def extract_attributes(field_name):
        '''
        Extract attributes from field_name
        Attributes w, x, y, z are to be extracted from field_names like "SRCH(w, SET(x), y, z)", "__IF(w, SRCH(w, SET(x), y, z), y, RADI(z))" or "SET(x)"
        Attributes are returned as a list

        Args:
        field_name (str): Field name

        Returns:
        list: List of attributes
        '''
        attributes = []
        field_name = field_name.strip()
        # Extract attributes from field_name
        if field_name.startswith("SRCH") or field_name.startswith("__IF"):
            text = field_name[5:-1]
            texts = split_string(text)
            for t in texts:
                attributes.extend(extract_attributes(t.strip()))            
        elif field_name.startswith("SET_") or field_name.startswith("LIST") or field_name.startswith("GLOB") or field_name.startswith("MULT"):
            text = field_name[5:-1]
            attributes.append(text.strip())
        else:
            attributes.append(field_name)
        return attributes

    attributes = []
    for field_name in field_name_list:
        # Check if field_name is a SRCH, IF, SET, or RADI statement
        attributes.extend(extract_attributes(field_name))
    #logging.debug("Found Attributes: %s", attributes)
    return list(set(attributes))

# TODO: Check if this function is still needed
def getRedCapValueROW(row,singleEntityRepeat,patient_df,plogger):
    '''
    Get the RedCap Value for a row in the singleEntityRepeat DataFrame

    Args:
    row (pandas.Series): The row to be replaced.
    singleEntityRepeat (pandas.DataFrame): The DataFrame containing the single repeat data.
    patient_df (pandas.DataFrame): The DataFrame containing the patient data.
    plogger (logging.Logger): The logger object.

    Returns:
    str: patient value.
    '''
    return getRedCapValue(row.field_name,singleEntityRepeat,patient_df,plogger)

def getRedCapValue(value,singleRepeatdf,patient_df,plogger):
    '''
    Get the RedCap Value for a given value

    Args:
    value (str): The key_value to be replaced.
    singleRepeatdf (pandas.DataFrame): The DataFrame containing the single repeat data.
    patient_df (pandas.DataFrame): The DataFrame containing the patient data.
    plogger (logging.Logger): The logger object.

    Returns:
    str: patient value.
    '''
    plogger.debug("UTILS: Get RedCap Value for %s", value)

    if value is None:
        return None
    elif value[:4] == "AUTO":
        return None
    elif value[:4] == "DROP":
        return value
    elif value[:4] == "SET_":
        return replace_SETvalue_with_value(value,plogger)
    elif value[:4] == "SRCH":
        return replace_SRCHvalue_with_value(value,singleRepeatdf,patient_df,plogger)
    elif value[:4] == "__IF":
        return replace_IFvalue_with_value(value,singleRepeatdf,patient_df,plogger)
    elif value[:4] == "LIST":
        return replace_LISTvalue_with_values(value,singleRepeatdf,patient_df,plogger)
    elif value[:4] == "GLOB":
        return get_value_from_df(value[5:-1],patient_df,plogger)
    elif value[:4] == "MULT":
        return get_value_from_df(value[5:-1],singleRepeatdf,plogger)
    else:
        return get_value_from_df(value,singleRepeatdf,plogger)

def replace_SETvalue_with_value(value,plogger):
    '''
    Replace "SET(xyz)" with "xyz" in the specified column

    Args:
    value (str): The value to be replaced.

    Returns:
    str: The cutted value.
    '''
    value = value[5:-1]
    plogger.debug("UTILS: Replace SET value with value: %s", value)
    return value

def get_value_from_df(value, df,plogger):
    '''
    Get the value from the DataFrame for the given value
    
    Args:
    value (str): The value to be replaced, corresponding to the field_name in the DataFrame.
    df (pandas.DataFrame): The DataFrame containing the patient data.
    plogger (logging.Logger): The logger object.
    
    Returns:
    str: The patient value.
    '''

    plogger.debug("UTILS: Get value from PatienDF for %s", value)
    try:
        value = df[df["field_name"] == value]["value"].values[0]
        plogger.debug("UTILS: Found Value in PatienDF: %s", value)
    except:
        plogger.warning("UTILS: Value not found in PatienDF: %s", value)
        value = "NULL"    
    return value
      
def replace_LISTvalue_with_values(value,df,plogger):
    '''
    Replace "LIST(x)" with [x1,x2,x3]
    
    Args:
    value (str): The value to be replaced.
    df (pandas.DataFrame): The DataFrame containing the single entity repeat data.
    plogger (logging.Logger): The logger object.
    
    Returns:
    list: The replaced value.
    '''
    plogger.debug("Replace LIST value with values for %s", value)
    value = value[5:-1]
    try:
        x_values = df.loc[df["field_name"] == value, "value"].values
        plogger.debug("UTILS: LIST: Found following values for %s in df: %s",value,x_values)
        return list(x_values)
    except:
        x_values = "WARN_"+value
        plogger.warning("UTILS: LIST: No values found in df: %s", value)
        return x_values

def replace_SRCHvalue_with_value(value,df,patient_df,plogger):
    '''
    Replace "SRCH(searchedattribute,entity,(attribute,redcapattribute)*x)" with SQL statement to get the value, (SELECT searchedattribute FROM entity WHERE (attribute = redcapattribute)*x
    Example: SRCH(id,demographics,race,race,ethnicity,ethnicity, gender,gender,dob,dob) -> SELECT id FROM demographics WHERE race = race AND ethnicity ... AND dob = dob
    
    Args:
    value (str): The SRCH statement.
    df (pandas.DataFrame): The DataFrame containing the single entity repeat data.
    patient_df (pandas.DataFrame): The DataFrame containing the whole patient data.
    plogger (logging.Logger): The logger object.

    Returns:
    str: Select-statement, like SELECT searchedattribute FROM entity WHERE (attribute = redcapattribute)*x
    '''

    plogger.debug("UTILS: Replace SRCH-statement [%s] with select statement.", value)
    # split the string into entity, entity_name and redcapattribute
    string = str(value)
    string = string[5:-1]
    # Split the string using function split_string
    strings = split_string(string)
    # Strip whitespace from the results
    strings = [item.strip() for item in strings]
    plogger.debug("Strings: %s", strings)

    # Check by length if the SRCH statement is valid
    if len(strings) < 4 or len(strings) % 2 != 0:
        plogger.error("UTILS: Unvalid SRCH statetment: %s", value)
        return None
    plogger.debug("UTILS: Valid SRCH statetment: %s", value)

    # Extract the searched_attribute, table, attributes and redcapattributes
    searched_attribute = strings[0].strip()
    table = strings[1].strip()
    plogger.debug("UTILS: Searched Attribute: %s", searched_attribute)
    plogger.debug("UTILS: Table: %s", table)
    # Extract the attributes and redcapattributes with a loop
    attributes = []
    redcapattributes = []
    counter = 2
    while counter < len(strings):
        attribute = strings[counter].strip()
        redcapattribute = strings[counter + 1].strip()
        attributes.append(attribute)
        redcapattributes.append(redcapattribute)
        counter += 2
        plogger.debug("UTILS: Attribute: %s", attribute)
        plogger.debug("UTILS: Redcap Attribute: %s", redcapattribute)

    # create a redcapvalues with the redcapattributes from the patient_df
    redcapvalues = []
    for attribute, redcapattribute in zip(attributes, redcapattributes):
        # Recursively call getRedCapValue for each redcapattribute
        redcapvalue = getRedCapValue(redcapattribute,df,patient_df,plogger)
        if redcapvalue is None:
            redcapvalue = "NULL"
        redcapvalues.append(redcapvalue)
        plogger.debug("UTILS: Redcap Value: %s\tRedcap Attribute: %s", redcapvalue,redcapattribute)

    # create search statement
    sql_statement = generate_search_statement(searched_attribute,table,attributes,redcapvalues)
    plogger.debug("UTILS: SQL Statement: %s", sql_statement)
    return sql_statement

def replace_IFvalue_with_value(value,df,patient_df,plogger):
    '''
    Replace "__IF(x,y,a,b)" with a if y in x.list else b in the specified column
    Check which values x stands for, if it is a field_name, get the values from the df and replace it with x
    Check IF Statement __IF(x,y,a,b): if y in x.list then a else b

    Args:
    value (str): The IF statement.
    df (pandas.DataFrame): The DataFrame containing the single entity repeat data.
    patient_df (pandas.DataFrame): The DataFrame containing the whole patient data.
    plogger (logging.Logger): The logger object.

    Returns:
    str: The replaced value.
    '''
  
    # split the string into entity, entity_name and redcapattribute
    plogger.debug("UTILS: Replace IF value %s", value)
    string = str(value)
    string = string[5:-1]

    # Split the string using function split_string
    strings = split_string(string)
    # Strip whitespace from the results
    strings = [item.strip() for item in strings]
    # Check by length if the IF statement is valid
    if len(strings) != 4:
        plogger.error("Unvalid IF statetment: %s", value)
        return None
    plogger.debug("Valid IF statetment: %s", value)

    # Extract the x, y, a and b values
    x = strings[0].strip()
    y = strings[1].strip()
    a = strings[2].strip()
    b = strings[3].strip()
    plogger.debug("x: %s", x)
    plogger.debug("y: %s", y)
    plogger.debug("a: %s", a)
    plogger.debug("b: %s", b)

    # Recursively call getRedCapValue for each redcapattribute
    x = getRedCapValue(x,df,patient_df,plogger)
    y = getRedCapValue(y,df,patient_df,plogger)
    a = getRedCapValue(a,df,patient_df,plogger)
    b = getRedCapValue(b,df,patient_df,plogger)

    def eval_if_statement(x, y, a, b):
        plogger.debug("CHECK IF: y (equal)|(in) x then a else b  %s", value)
        plogger.debug("x: %s", x)
        plogger.debug("y: %s", y)
        plogger.debug("a: %s", a)
        plogger.debug("b: %s", b)
        if type(x) is str and type(y) is str:
            return a if y == x else b
        elif type(x) is list and type(y) is str:
            return a if y in x else b
        elif type(x) is str and type(y) is list:
            return a if x in y else b
        elif type(x) is list and type(y) is list:
            return a if any([item in x for item in y]) else b
        else:
            plogger.error("Unvalid IF statetment: x is a %s", type(x))
            return None

    value = eval_if_statement(x, y, a, b)

    plogger.debug("Final IF Value: %s", value)
    return value

def split_string(input_string):
    '''
    Split a string at commas, but ignore commas inside parentheses
    Format: "SRCH(w, SET(x), y, z)" -> ["SRCH(w", "SET(x)", "y", "z"]
    
    Args:
    input_string (str): The string to be split.
    plogger (logging.Logger): The logger object.
    
    Returns:
    list: List of sub strings.
    '''
    sub_strings = []
    temp_string = ""
    open_parenthesis = 0

    for char in input_string:
        if char == "(":
            open_parenthesis += 1
            temp_string += char
        elif char == ")":
            open_parenthesis -= 1
            temp_string += char
        elif char == "," and open_parenthesis == 0:
            sub_strings.append(temp_string.strip())
            temp_string = ""
        else:
            temp_string += char

    if temp_string:
        sub_strings.append(temp_string.strip())

    return sub_strings

def drop_rows_with_NULL(df, column_name):
    '''
    Drop rows equals the keyword 'NULL' in the 'columname' column 

    Args:
    df (pandas.DataFrame): The DataFrame containing the data.
    column_name (str): The column name to be checked.

    Returns:
    pandas.DataFrame: The filtered DataFrame.
    '''
    return df[df[column_name] != 'NULL']

