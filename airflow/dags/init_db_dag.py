import datetime as dt
import pendulum
from airflow.decorators import dag, task, bash_task
from airflow.models.param import Param
from airflow.operators.python import get_current_context

import cwyeh_mysql_etl.mysql_etl_utils as myetl
import cwyeh_mysql_etl.kafka.consume as kc


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
    dag_id='init_db',
    default_args=default_args,
    schedule=None,
    start_date=dt.datetime(2023, 1, 1, tzinfo=TW_TZ),
    catchup=False,
    tags=["init"],
    params={"db_name":Param("EXAMPLE",type='string')},
)
def ingest_data_dag():

    @task
    def init_db_task():
        """
        set up DB
        - DB default to {EXAMPLE}
        """
        ctx = get_current_context()
        config = ctx["dag_run"].conf or {}
        default_params = ctx["params"]
        db_name = config.get('db_name',default_params['db_name'])
        with myetl.get_mysql_connection(
                host='mysql',port=3306,user='root',
            ) as my_conn:

            # when init, no need to assign db for conn
            myetl.init_tables(
                conn=my_conn,
                db_name=db_name,
                insert_example_records=False,
                drop_tables_if_exists=False,
                create_index=True,
            )

    init_db_task()

ingest_data_dag()