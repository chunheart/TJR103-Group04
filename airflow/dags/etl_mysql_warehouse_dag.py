import datetime as dt

from airflow.decorators import dag, task, bash_task
from airflow.operators.python import get_current_context
from airflow.models.param import Param

import cwyeh_mysql_etl.mysql_etl_utils as myetl


# Setting
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["your_email@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": dt.timedelta(minutes=1),
}

# Define the DAG
@dag(
    dag_id='etl_mysql_warehouse',
    default_args=default_args,
    schedule="00 11 * * *",
    start_date=dt.datetime(2023, 1, 1),
    catchup=False,
    tags=["warehouse"],
    params = {"tar_date":Param((dt.datetime.now() - dt.timedelta(days=1)).strftime("%Y%m%d"),type='string'),
              "back_days":Param(1,type='integer')},
)
def etl_mysql_warehouse():

    @task
    def get_recipe_data():
        """
        get staged data (local)
        """
        ctx = get_current_context()
        config = ctx["dag_run"].conf or {}
        default_params = ctx["params"]
        tar_date = config.get('tar_date',default_params['tar_date'])
        back_days = config.get('back_days',default_params['back_days'])
        print(f'[TASK__GET_RECIPE_DATA]: read data from {tar_date}, backfill {back_days} days')

        res = myetl.get_recipe_data(
            tar_date=tar_date,
            back_days=back_days,
            source='local',
            path='/opt/airflow/data/stage/icook_recipe'
        )
        for row in res[:3]:
            print(row)
        return res

    @task
    def insert_mysql(res):
        """
        TBA
        """
        print("Running Task 2")
        for row in res[:3]:
            print(row)

    res1 = get_recipe_data()
    insert_mysql(res1)

etl_mysql_warehouse()