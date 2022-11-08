import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models.dagrun import DagRun
import time
from datetime import date, datetime, timedelta
from python_functions import sendPartitions, convertPartitions

today = date.today()
now = time.strftime('%H')
timestamp = str(today) + '_' + now

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': '2022-3-30',
    'email': ['shiv.narayanan@icloud.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG('kinesis-data-firehose', default_args=default_args, schedule_interval="@once") # use "timedelta(minutes=10)" for every 10 minutes

task1 = BashOperator(task_id='transactions_partition', bash_command='shuf -n 500 /usr/local/airflow/data/transactions.csv > /usr/local/airflow/data/transactions_partition.csv', retries=2, dag=dag)

task2 = PythonOperator(task_id = 'convert_partition', python_callable = convertPartitions, provide_context=True, dag=dag)

task3 = PythonOperator(task_id = 'send_partition', python_callable = sendPartitions, provide_context=True, dag=dag)


task1.set_downstream(task2)
task2.set_downstream(task3)



