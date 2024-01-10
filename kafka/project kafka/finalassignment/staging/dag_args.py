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
