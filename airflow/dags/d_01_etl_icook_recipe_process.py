import time
import pendulum

from albert_icook_crawler.src.pipeline.extract.scrapy_app_icook import IcookDailySpider
from albert_icook_crawler.src.pipeline.transformation.normalization_v1_segment_data import seg_01_recipe_only as recipe
from albert_icook_crawler.src.pipeline.transformation.normalization_v1_segment_data import seg_02_ingredients_only as ingredient
from albert_icook_crawler.src.pipeline.transformation.normalization_v2_nor_ingrd_unit import normalize_ingredients_name as n_ingrd_name
from albert_icook_crawler.src.pipeline.transformation.normalization_v2_nor_ingrd_unit.kevin_food_unit_normalization import app 

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from datetime import timedelta
from multiprocessing import Process 


LOCAL_TZ = pendulum.timezone("Asia/Taipei")

# Subprocess
def cralwer_worker():
    """Get recipe's raw data by web-crawling"""
    spider = IcookDailySpider(keyword="latest")
    spider.run()
    time.sleep(30)

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
@dag(
    'd_01_etl_icook_recipe_process',  #
    default_args=default_args,
    description='Python operators',  #
    schedule="00 9 * * *",  #
    start_date=pendulum.datetime(2025, 11, 22, tz=LOCAL_TZ),
    catchup=False,
    tags=["scrapy", "icook"]
)

def d_01_etl_icook_recipe_process():
    # Define the tasks

    @task
    def get_recipe_raw_data():
        print("[Main-Process] Starting Scrapy in a separate process...")
        # Build a subprocess
        sub_p = Process(target=cralwer_worker)
        # Activate it
        sub_p.start()
        # Wait until the subprocess finishes
        sub_p.join()

        # Check if the subprocess finishes normally
        if sub_p.exitcode != 0:
            raise Exception("Scrapy failed with errors.")
        
        print("[Main-Process] Spider task completed. Sleeping for safety...") 
        
        return "Scrapy done"

    @task
    def get_recipe(pre_task):
        recipe.get_recipe_info()
        time.sleep(8)
        return "Retrieved data for recipe table"
    
    @task
    def get_ingredients_info(pre_task):
        ingredient.get_ingredients_info()
        time.sleep(8)
        return "Retrieved data for ingredients table"

    
    @task
    def normalize_ingredient_name(pre_task):
        n_ingrd_name.normalize_ingrd_name()
        time.sleep(8)
        return "Normalised ingredient's name"


    @task
    def normalize_unit(pre_task):
        app.normalize_unit()
        return "Normalised ingredient's unit"


    workflow1 = get_recipe_raw_data()
    workflow2 = get_recipe(workflow1)
    workflow3 = get_ingredients_info(workflow2)
    workflow4 = normalize_ingredient_name(workflow3)
    normalize_unit(workflow4)


d_01_etl_icook_recipe_process()
