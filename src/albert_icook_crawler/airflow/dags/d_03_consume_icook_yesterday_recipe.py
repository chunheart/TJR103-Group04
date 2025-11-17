import os
import pendulum

from src.kafka.consumer.consume_airflow import consume_messages
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

os.makedirs(f"/opt/airflow/logs/kafka/consume", exist_ok=True)

LOCAL_TZ = pendulum.timezone("Asia/Taipei")

def consume_messages():
    consume_messages()


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['chengreentea0813@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,  #
    'retry_delay': timedelta(minutes=5),  #
}

# Define the DAG
dag = DAG(
    'd_03_consume_icook_yesterday_recipe',  #
    default_args=default_args,
    description='Python operators',  #
    schedule_interval="0 12 * * *",  #
    start_date=datetime(2025, 11, 17, tzinfo=LOCAL_TZ),
    catchup=False,
    tags=["consume", "icook"]
)

# Define the tasks
task1_obj = PythonOperator(
    task_id='consume_messages',
    python_callable=consume_messages,
    dag=dag,
)

# Task dependencies
task1_obj
