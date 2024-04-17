# Mapping Functions

## Mapping Function SRCH (Search)

The SRCH function is used to search for a specific value in a list of values. The function returns the index of the first occurrence of the value in the list. If the value is not found, the function returns -1.

    if value is None:
        return None
    elif value[:4] == "AUTO":
        return None
    elif value[:4] == "DROP":
        return value
    elif value[:4] == "SET_":
        return replace_SETvalue_with_value(value)
    elif value[:4] == "SRCH":
        return replace_SRCHvalue_with_value(value,singleRepeatdf,patient_df,database_file,api_address,project_name)
    elif value[:4] == "__IF":
        return replace_IFvalue_with_value(value,singleRepeatdf,patient_df,database_file,api_address,project_name)
    elif value[:4] == "LIST":
        return replace_LISTvalue_with_values(value,singleRepeatdf,patient_df,database_file,api_address,project_name)
    elif value[:4] == "GLOB":
        return getValueFromPatientDF(value,patient_df)
    elif value[:4] == "MULT":
        return getValueFromRepeatDF(value[5:-1],singleRepeatdf)
    else:

    DONT USE SRCH FUNCTION WITHIN IF FUNCTION