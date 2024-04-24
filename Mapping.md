# Mapping

Mapping is used to map values from one source entity (REDCap CSV in our case) to another destination entity (SQLite Database). The mapping functions are defined per destination enitity (DB Table) in csv files in the `mappingtables` directory. The mapping functions are used to map values from REDCAP fieldname to another SQLite Entity Attribute.

## CSV to SQLite Mapping

Starting point is the REDCap CSV file (Downloaded manually or over API). The CSV file contains the following columns, and could look something like this:

| **record** | **redcap_event_name** | **redcap_repeat_instrument** | **redcap_repeat_instance** | **field_name**        | **value**                          |
|------------|-----------------------|------------------------------|----------------------------|-----------------------|------------------------------------|
| 1          | general_data_arm_1    |                              |                            | study_id              | 4632                               |
| 1          | general_data_arm_1    |                              |                            | date_enrolled         | 2024-04-11                         |
| 1          | general_data_arm_1    |                              |                            | first_name            | Jon                                |
| 1          | general_data_arm_1    |                              |                            | last_name             | Dow                                |
| 1          | general_data_arm_1    |                              |                            | address               | Mainstreet 42, Zurich, Zurich,3000 |
| 1          | general_data_arm_1    |                              |                            | demographics_complete | Complete                           |
| 1          | general_data_arm_1    |                              |                            | email                 | examplemail@mail.ch                |
| 1          | general_data_arm_1    |                              |                            | dob                   | 1984-04-05                         |
| 1          | general_data_arm_1    |                              |                            | age                   | 40                                 |
| 1          | general_data_arm_1    |                              |                            | ethnicity             | Hispanic or Latino                 |
| 1          | general_data_arm_1    |                              |                            | gender                | Male                               |
| 1          | general_data_arm_1    |                              |                            | specify_mood          | 91                                 |
| 1          | general_data_arm_1    |                              |                            | height                | 183                                |
| 1          | general_data_arm_1    |                              |                            | weight                | 88                                 |

the mapping file (E-C-entityname.csv | E = Entity Order -> to resolve dependecies first. C = Counter if more than one possibility to add this entity) could look like this: 1-0-patients.csv

| Table    | Attribute       | NotNull  | field_name                                                                 |
|----------|-----------------|----------|----------------------------------------------------------------------------|
| patients | id              | NOT NULL | study_id                                                                   |
| patients | sign_date       | NOT NULL | date_enrolled                                                              |
| patients | first_name      | NOT NULL | first_name                                                                 |
| patients | last_name       | NOT NULL | last_name                                                                  |
| patients | email           |          | email                                                                      |
| patients | demographics_id | NOT NULL | SRCH(id,demographics,race,race,ethnicity,ethnicity, gender,gender,dob,dob) |

The code separates the REDCap CSV file into the different records. For each record, it iterates over the mapping tables and maps the values from the REDCap CSV to the attributes of the SQLite entity. The mapping is done by using the mapping functions defined in the mapping tables.

The entity dataframe is filled with the values from the REDCap CSV file and would look like this:

| Table    | Attribute       | NotNull  | field_name                                                                 | value                              |
|----------|-----------------|----------|----------------------------------------------------------------------------|------------------------------------|
| patients | id              | NOT NULL | study_id                                                                   | 4632                               |
| patients | sign_date       | NOT NULL | date_enrolled                                                              | 2024-04-11                         |
| patients | first_name      | NOT NULL | first_name                                                                 | Jon                                |
| patients | last_name       | NOT NULL | last_name                                                                  | Dow                                |
| patients | email           |          | email                                                                      | examplemail@mail.com               |
| patients | demographics_id | NOT NULL | SRCH(id,demographics,race,race,ethnicity,ethnicity, gender,gender,dob,dob) | (SELECT id FROM demographics WHERE race = 'White' AND ethnicity = 'Hispanic or Latino' AND gender = 'Male' AND dob = '1984-04-05') |

Then the code generates a SQL statement like this:

```sql
INSERT OR IGNORE INTO patients (id, sign_date, first_name, last_name, email, demographics_id) VALUES (1, '2024-04-11', 'Jon', 'Dow', 'examplemail@mail.ch', (SELECT id FROM demographics WHERE race = 'White' AND ethnicity = 'Hispanic or Latino' AND gender = 'Male' AND dob = '1984-04-05'));
```

## Mapping Functions

Possible mapping functions are: AUTO, DROP, SET_, SRCH, __IF, LIST, GLOB, MULT

And they can be used in the following ways:

### Mapping Function AUTO

The AUTO function is used to tell the code to drop the attribute (maybe, because it will be automatically generated by the database -> id). The function does not take any arguments.

### Mapping Function DROP

The DROP function is used to tell the code to drop the whole entity. The function does not take any arguments.

```text
USE CASE:
    
        This function makes more sense when you use it in combination with other functions. For example, you could use it to drop the whole entity if a certain condition is met (check the __IF function).
```

### Mapping Function SET_(value)

This function is used to set the value of a variable in a entity attribute. The function takes one arguments: the value you want to set. SET(2) will set the value of the field of this entity to 2.

```text
If we for example want to set the value of the field `first_name` to `Jonny`, we would use the following mapping function within 1-0-patients.csv:
    | patients | first_name      | NOT NULL | SET_("Jonny")                                                                 |

    But will set all first_name attributes in the creating SQLite DB to "Jonny"

USE CASE:

    You are collecting data from 2 different sources REDCap sources and you want to set the value of the field 'center' to 'Hospital of Zurich' for all records from the REDCap source and to 'Hospital of Bern' for all records from the other source.
```

### Mapping Function SRCH( searched attribute, table, known attribute , value , known attribute , value , ...)

This function is used to search for a value in another table and return the id of the found record. The function takes at least 4 arguments: the attribute you want to search for, the table you want to search in, the attribute you know, and the value of this attribute. You can add more pairs of known attributes and values.

```text
If we for example want to search for the id of the record in the table `demographics` with race, ethnicity and dob, we would use the following mapping function:
    SRCH(id,demographics,race,race,ethnicity,ethnicity,dob,dob)

USE CASE:
    
        For foreign keys. You have a table `patients` and a table `demographics`. The table `patients` has a foreign key `demographics_id` that references the table `demographics`.
```

### Mapping Function __IF(attribute, equals attribute, true attribute, false atrribute)

This Function is used to check for a condition and return the value of the true attribute if the condition is met and the first two attributes are equal and the value of the false attribute if the condition is not met, so if the first two attributes are not equal. The function takes 4 arguments: the attribute you want to check, the attribute you want to compare it to, the value of the true attribute, and the value of the false attribute.

```text
If we for example only want data from the REDCap source which has the value 'Complete' in the field 'demographics_complete', we would use the following mapping function:
    __IF(demographics_complete, SET_("Complete"), AUTO ,DROP)

This will insert only records with the value 'Complete' in the field 'demographics_complete' into the SQLite database.
```

### Mapping Function LIST(attribute)

This function is used to create a list of values. The function takes one argument: the attribute you want to list all values of within this repeat instance.
(Only for repeatable fields) 

Still under construction, not stable yet.

### Mapping Function GLOB(attribute)

This function is like a normal attribute, but it used if a attribute is not findable in a repeat instance and you need to search for it in the whole record.


### Mapping Function MULT(attribute)

This function is used to tell the code that the entity will be created multiple times. The function takes one argument: the argument with differen values within the different entities you want to create.

Still under construction, not stable yet.