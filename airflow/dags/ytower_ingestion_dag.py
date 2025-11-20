import os
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

# ================= 1. 路徑設定  =================
PROJECT_ROOT = os.getenv("PROJECT_ROOT", "/opt/airflow")

# 指向 src 資料夾
SRC_PATH = os.path.join(PROJECT_ROOT, "src")

CRAWLER_PATH = os.path.join(SRC_PATH, "kevin_ytower_crawler")
NORMALIZATION_SCRIPT = os.path.join(SRC_PATH, "kevin_food_unit_normalization/main.py")

# 中間產物
OUTPUT_CSV_PATH = os.path.join(CRAWLER_PATH, "ytower_recipes.csv")

# ================= 2. DAG 設定 =================
default_args = {
    'owner': 'Kevin',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id="ytower_ingestion_pipeline",
    default_args=default_args,
    description="楊桃網食譜爬蟲與正規化 (Phase 1 & 2)",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2023, 11, 20, tz="Asia/Taipei"),
    catchup=False,
    tags=["ytower", "ingestion"],
) as dag:

    # 任務 A: 爬蟲
    # 修改重點：使用 -O (大寫) 來自動覆寫檔案，解決 Scrapy 參數解析錯誤
    task_run_crawler = BashOperator(
        task_id="run_crawler",
        bash_command=f"cd {CRAWLER_PATH} && scrapy crawl ytower_main -O {OUTPUT_CSV_PATH}"
    )

    # 任務 B: 正規化 (呼叫 AI & 寫入 Kafka)
    # 修改重點：加入 env 參數來傳遞 API Key
    task_run_normalization = BashOperator(
        task_id="run_normalization",
        bash_command=f"python3 {NORMALIZATION_SCRIPT} --input {OUTPUT_CSV_PATH} --topic ytower_recipes_normalized --source ytower",
        env={
            "GEMINI_API_KEY": "AIzaSyDUYuNsqyiuXN3PRycIDz0Y8rV1sCx0eIw", 
            **os.environ  # 保留系統原本的環境變數 
        }
    )

    task_run_crawler >> task_run_normalization