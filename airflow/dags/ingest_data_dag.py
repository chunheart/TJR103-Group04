import datetime as dt
from airflow.decorators import dag, task, bash_task

import cwyeh_mysql_etl.mysql_etl_utils as myetl
import cwyeh_mysql_etl.kafka.consume as kc


# Setting
default_args = {
    "owner": "cwyeh",
    "depends_on_past": False,
    "email": ["your_email@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": dt.timedelta(minutes=1),
    "dagrun_timeout":dt.timedelta(hours=1),
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
        ingest recipe data from assigned source [kafka or get a demo data]
        - data write into staged storage (currently local, could be GCS or else later)
        """
        # res = myetl.get_recipe_data(tar_date='20251120',back_days=1,source='demo')
        my_logger = kc.get_logger()
        my_consumer = kc.create_consumer(group_id='etl_test02',topic_name='icook_recipes',logger=my_logger)
        my_dump_func = kc.get_dump_to_local_function(ref_date='上線日期',base_path='/opt/airflow/data/stage/icook_recipe')
        kc.ingest_kafka_message(my_consumer,my_logger,my_dump_func)

    @task
    def clean_local_storage():
        """
        clean local/staged storage
        (1) combine several file under a day
        (2) remove old data
        """
        print("Clean staged data (Currently do nothing)")
        # myetl.clean_local_storage(base_dir='/opt/airflow/data/stage/icook_re')

    res1 = ingest_recipe_data()
    clean_local_storage().set_upstream(res1)

ingest_data_dag()