# RedCap2SQLite (REDCap to SQLite Data Transfer)

## Introduction

This project is a Python application that fetches data from a REDCap server and stores it in a SQLite database.

## Usage

1. Ensure Python is installed on your system.
1. Clone this repository.
1. Navigate to the root directory and run python main.py to start the script.

## Functionality

The script uses REDCap APIs to access and retrieve necessary data from the REDCap server, then it transfers this data to a SQLite database. This project can be useful for users needing to transfer large amounts of data from REDCap to SQLite reliably.

## Requirements

- Python 3.x
- SQLite version x.x.x
- REDCap API access

## Contributing

Contributions, issues, and feature requests are welcome!

***Enjoy transferring data from REDCap to SQLite efficiently and reliably!***

## **ETL Workflow Overview**

This workflow performs Extract, Transform, and Load (ETL) operations on data.

- **Extract:** Reads data from a source (e.g., CSV file).
- **Transform:** Processes the extracted data (e.g., cleaning, filtering).
- **Load:** Writes the transformed data to a destination (e.g., database).

**Files:**

- workflow.py: Defines the main workflow logic.
- extract.py: Contains the logic to extract data.
- transform.py: Contains the logic to transform data.
- load.py: Contains the logic to load data.

**Execution:**

Run the `workflow.py` script to execute the ETL process.