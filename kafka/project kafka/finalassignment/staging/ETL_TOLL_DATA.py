#import relevant libraries for creating dags
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'root',
    'start_date': days_ago(0),
    'email': ['root@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
# Dag definition
dag = DAG(
    'ETL_TOLL_DATA',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

unzip_data = BashOperator(
    task_id='unzip_data',
    #unzip .tgz file to current directory
    bash_command='tar -xvzf ../tolldata.tgz -C .',
    dag=dag,
)

# Extract data from csv file from 1st to 4th field
extract_csv = BashOperator(
    task_id='extract_csv',
    # Rowid, Timestamp, Anonymized Vehicle number, and Vehicle type
    bash_command='cut -d"," -f1-4 vehicle-data.csv > csv_data.csv',
    dag=dag,
)

# Extract data from tsv file
extract_tsv = BashOperator(
    task_id='extract_tsv',
    # Columns Number of axles, Tollplaza id, and Tollplaza code
    bash_command="cat tollplaza-data.tsv | tr '\t' ',' | cut -d',' -f5-7 | tr -s ' ' > tsv_data.csv",
    dag=dag,
)

#Extract data from a fixed width file
extract_fixed_width = BashOperator(
    task_id='extract_fixed_width',
    # Columns Type of Payment code, and Vehicle Code
    bash_command='awk \'{print substr($0, 59, 3) "," substr($0, 63)}\' payment-data.txt | tr -s \' \'  > fixed_width_data.csv',
    dag=dag,
)

# Consolidate all data into one file
consolidate_data = BashOperator(
    task_id='consolidate_data',
    # Columns in order of Rowid, Timestamp, Anonymized Vehicle number, Vehicle type, Number of axles, 
    # Tollplaza id, Tollplaza code, Type of Payment code, and Vehicle Code
    bash_command="paste  csv_data.csv tsv_data.csv fixed_width_data.csv > consolidated_data.csv",
    dag=dag,
)


# Transform data This task should transform the vehicle_type field in extracted_data.csv into capital letters and save it into a file named transformed_data.csv in the staging directory.
transform_data = BashOperator(
    task_id='transform_data',
    # Change file data to uppercase 
    bash_command="cut -d',' -f4 | tr '[:lower:]' '[:upper:]' < consolidate_data.csv > transformed_data.csv",
    dag=dag,
)

# Define pipeline
unzip_data >> extract_csv >> extract_tsv >> extract_fixed_width >> consolidate_data >> transform_data
