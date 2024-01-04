# import the libraries

# Import the datetime and timedelta classes
from datetime import timedelta

# Import the DAG class from the airflow module
from airflow import DAG

# Import the BashOperator class from the airflow.operators module
from airflow.operators.bash_operator import BashOperator

# Import the days_ago function from the airflow.utils.dates module
from airflow.utils.dates import days_ago

# Define the default arguments for the DAG
default_args = {
	'owner': 'root',
    'start_date': days_ago(0),
    'email': ['root@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

# Define a DAG
dag = DAG(
    'simple_etl_dag',
    default_args=default_args,
    description='A simple DAG using bash',
    schedule_interval=timedelta(days=1),
)

# Define tasks

# Define the first task extract

extract = BashOperator(
    task_id='extract',
    # get the first, third and sixth column of the /etc/passwd file and save it in a new file
    bash_command='cut -d":" -f1,3,6 /etc/passwd > /tmp/extracted_data.txt',
    dag=dag,
)

# Define the second task transform
transform_and_load = BashOperator(
    task_id='transform and load',
    bash_command='tr ":" "," < /tmp/extracted_data.txt > /tmp/transformed_data.csv',
    dag=dag,
)

# Define the order of tasks

extract >> transform_and_load