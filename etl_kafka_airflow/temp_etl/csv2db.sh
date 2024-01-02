# This script
# Extracts data from /etc/passwd file into a CSV file.

# The csv data file contains the user name, user id and 
# home directory of each user account defined in /etc/passwd

# Transforms the text delimiter from ":" to ",".
# Loads the data from the CSV file into a table in PostgreSQL database.

# The script is executed by the Airflow DAG: etl_kafka_airflow/dags/csv2db.py

# Extract phase

echo "Extracting data";

# Extract columns 1 (user name), 2 (user id) and 6 (home directory) from /etc/passwd

cut -d":" -f1,3,6 /etc/passwd > extracted_data.txt;

# Transform phase

echo "Transforming data";

# read extracted data and replace colons with commas

tr ":" "," < extracted_data.txt > transformed-data.csv;

# Load phase

echo   "Loading data";
# Send instructions to connect to template1 and
# copy the file to the table 'users'  through command pipeline

echo "\c template1;\COPY users FROM 'transformed-data.csv' DELIMITERS ',' CSV;" | psql --username=postgres --host=localhost;