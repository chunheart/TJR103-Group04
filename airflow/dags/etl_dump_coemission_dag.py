import datetime as dt

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
    dag_id='etl_dump_coemission',
    default_args=default_args,
    schedule=None,
    start_date=dt.datetime(2023, 1, 1),
    catchup=False,
    tags=["warehouse"],
)
def etl_dump_coemission():

    @task
    def get_coemission():
        """
        get carbon emission from myemission.com
        """
        res = myetl.get_coemission_data(source='myemission')
        print_sth_or_not(res)
        return res

    @task
    def dump_coemission(res):
        """
        dump data into coemission
        """
        with myetl.get_mysql_connection(
                host='mysql',port=3306,user='root',password='pas4word',db='EXAMPLE',
            ) as my_conn:
            myetl.register_coemission(my_conn, res)
            print('[DONE] dump myemission data into carbon emission')


    res1 = get_coemission()
    dump_coemission(res1)

etl_dump_coemission()