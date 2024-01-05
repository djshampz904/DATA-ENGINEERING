# import libraries
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago


# Define the default_args dictionary
default_args = {
    'owner': 'root',
    'start_date': days_ago(0),
    'email': ['root@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

dag = DAG(
    'dummy_dag',
    default_args=default_args,
    description='A simple dummy DAG',
    schedule_interval=timedelta(days=1),
)

# Define tasks

# Define the first task

task1 = BashOperator(
    task_id='task1',
    bash_command='Sleep 1',
    dag=dag,
)

# Define the second task

task2 = BashOperator(
    task_id='task2',
    bash_command='Sleep 2',
    dag=dag,
)

# Define third task

task3 = BashOperator(
    task_id='task3',
    bash_command='Sleep 3',
    dag=dag,
)

# Define the order of tasks
task1 >> task2 >> task3