import datetime as dt
from airflow.decorators import dag, task, bash_task

import cwyeh_mysql_etl.mysql_etl_utils as myetl


# Setting
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["your_email@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=1),
}

# Define the DAG
@dag(
    dag_id='ingest_data',
    default_args=default_args,
    schedule="00 10 * * *",
    start_date=dt.datetime(2023, 1, 1),
    catchup=False,
    tags=["ingest","stagging"],
)
def ingest_data_dag():

    @task
    def task1():
        return myetl.get_recipe_data(tar_date=1,back_days=1,source='demo')

    @task
    def task2(t1_res):
        print("Running Task 2 (Saving data as json, locally)")
        for row in t1_res[:3]:
            print(row)

    t1_res = task1()
    task2(t1_res)

ingest_data_dag()