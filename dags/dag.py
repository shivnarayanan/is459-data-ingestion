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
# timedelta(minutes=10)

dag = DAG('kinesis-data-firehose', default_args=default_args, schedule_interval="@once")

task1 = BashOperator(task_id='customers_partition', bash_command='shuf -n 500 /usr/local/airflow/data/customers.csv > /usr/local/airflow/data/customers_partition.csv', retries=2, dag=dag)
task2 = BashOperator(task_id='transactions_partition', bash_command='shuf -n 500 /usr/local/airflow/data/transactions.csv > /usr/local/airflow/data/transactions_partition.csv', retries=2, dag=dag)
task3 = BashOperator(task_id='articles_partition', bash_command='shuf -n 500 /usr/local/airflow/data/articles.csv > /usr/local/airflow/data/articles_partition.csv', retries=2, dag=dag)

task4 = PythonOperator(task_id = 'convert_partition', python_callable = convertPartitions, provide_context=True, dag=dag)

task5 = PythonOperator(task_id = 'send_partition', python_callable = sendPartitions, provide_context=True, dag=dag)

task1.set_downstream(task2)
task2.set_downstream(task3)
task3.set_downstream(task4)
task4.set_downstream(task5)



