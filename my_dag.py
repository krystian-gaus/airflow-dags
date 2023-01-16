from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests

default_args = {
    'owner': 'me',
    'start_date': datetime(2022, 1, 1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'call_rest_api',
    default_args=default_args,
    schedule_interval=timedelta(hours=1),
)

def call_rest_api():
    response = requests.get('https://jsonplaceholder.typicode.com/todos/1')
    print(response.json())

call_api_task = PythonOperator(
    task_id='call_api',
    python_callable=call_rest_api,
    dag=dag,
)