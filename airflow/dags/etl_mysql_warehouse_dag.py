import datetime as dt
import pendulum

from airflow.decorators import dag, task, bash_task
from airflow.operators.python import get_current_context
from airflow.models.param import Param

import cwyeh_mysql_etl.mysql_etl_utils as myetl


# Helpers
def print_sth_or_not(res):
    if res:
        print(f'size: {len(res)}')
        for row in res[:3]:
            print(row)
    else:
        print('Nothing')


# Setting
TW_TZ = pendulum.timezone("Asia/Taipei")
default_args = {
    "owner": "cwyeh",
    "depends_on_past": False,
    "email": ["your_email@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": dt.timedelta(minutes=1),
    "dagrun_timeout":dt.timedelta(hours=1),
}


# Define the DAG
@dag(
    dag_id='etl_mysql_warehouse',
    default_args=default_args,
    schedule="00 11 * * *",
    start_date=dt.datetime(2023, 1, 1, tzinfo=TW_TZ),
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
        print_sth_or_not(res)
        return res

    @task
    def clean_recipe_data(res):
        """
        TBA
        """
        print("Running Task 2")
        if res:
            res = myetl.clean_recipe_data(res)
            print_sth_or_not(res)
        return res

    @task
    def insert_recipe_into_mysql(res):
        """
        TBA
        """
        print("Running Task 3")
        if res:
            with myetl.get_mysql_connection(
                host='mysql',port=3306,user='root',db='EXAMPLE',
            ) as my_conn:
                myetl.register_recipe(my_conn, res)
                print('[DONE] insert into recipe')
                myetl.register_ingredient(my_conn, res)
                print('[DONE] insert into ingredient_normalize')
                myetl.register_unit(my_conn, res)
                print('[DONE] insert into unit_normalize')
                myetl.register_recipe_ingredient(my_conn, res)
                print('[DONE] insert into recipe_ingredient')
        return res

    @task
    def update_normalize_recipe(res):
        """
        TBA
        """
        if res:
            with myetl.get_mysql_connection(
                host='mysql',port=3306,user='root',password='pas4word',db='EXAMPLE',
            ) as my_conn:
                myetl.update_unit_w_u2g(my_conn)
                myetl.update_ingredient_w_normalize(my_conn)
                myetl.register_coemission_from_recipe(my_conn,res)
                myetl.update_coemission_w_query(my_conn)
            

    res1 = get_recipe_data()
    res2 = clean_recipe_data(res1)
    res3 = insert_recipe_into_mysql(res2)
    update_normalize_recipe(res3)


etl_mysql_warehouse()