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
    bash_command="cut -d$'\t' -f5-7 tollplaza-data.tsv | tr '\t' ',' > tsv_data.csv",
    dag=dag,
)

#Extract data from a fixed width file
extract_fixed_width = BashOperator(
    task_id='extract_fixed_width',
    # Columns Type of Payment code, and Vehicle Code
    bash_command="cut -c59-69 payment-data.txt | tr ' ' ',' > fixed_width_data.csv",
    dag=dag,
)

# Consolidate all data into one file
consolidate_data = BashOperator(
    task_id='consolidate_data',
    # Columns in order of Rowid, Timestamp, Anonymized Vehicle number, Vehicle type, Number of axles, 
    # Tollplaza id, Tollplaza code, Type of Payment code, and Vehicle Code
    bash_command="paste -d',' csv_data.csv tsv_data.csv fixed_width_data.csv > consolidated_data.csv",
)




