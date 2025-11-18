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
    def ingest_recipe_data():
        """
        ingest recipe data from assigned source
        - batch write to local
        """
        res = myetl.get_recipe_data(tar_date=1,back_days=1,source='demo')
        for row in res[:3]:
            print(row)

    @task
    def clean_local_storage():
        """
        clean local storage (/opt/airflow/data/stage)
        (1) combine several file under a day
        (2) remove old data
        """
        print("Running Task 2 (Saving data as json, locally)")

    res1 = ingest_recipe_data()
    clean_local_storage().set_upstream(res1)

ingest_data_dag()