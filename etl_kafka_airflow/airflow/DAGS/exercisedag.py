# ETL_Server_Access_Log_Processing

from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'root',
    'start_date': days_ago(0),
    'emaail': ['root@example'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

# Define a DAG
dag = DAG(
    'ETL_Server_Access_Log_Processing',
    default_args=default_args,
    description='A simple DAG using bash',
    schedule_interval=timedelta(days=1),
)

# Define tasks
download_file = BashOperator(
    task_id='download_file',
    bash_command='wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt -O /tmp/access_log.txt',
    dag=dag,
)

extract_from_file = BashOperator(
    # Extract the task must extract the fields timestamp and visitorid from the access_log.txt file and save it in a new file fields 1 and fields 4
    task_id='extract_from_file',
    bash_command='cut -d"#" -f1,4 /tmp/access_log.txt > /tmp/extracted_data.txt',
    dag=dag,
)

transform = BashOperator(
    task_id='transform',
    # Capitlize visitor id 
    bash_command='tr "[:lower:]" "[:upper:]" < /tmp/extracted_data.txt > /tmp/transformed_data.txt; tr "#" "," < /tmp/transformed_data.txt > /tmp/transformed_data.csv''',
    dag=dag,
)

load = BashOperator(
    # compress the extracted and transformed data.
    task_id='load',
    bash_command='gzip /tmp/transformed_data.csv',
    dag=dag,
)

# Define the order of tasks
download_file >> extract_from_file >> transform >> load
