import os
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

# ================= 1. 路徑設定 =================
# 配合 Docker 內的標準路徑 /opt/airflow
PROJECT_ROOT = os.getenv("PROJECT_ROOT", "/opt/airflow")
SRC_PATH = os.path.join(PROJECT_ROOT, "src")

# 指向通用正規化程式
NORMALIZATION_SCRIPT = os.path.join(SRC_PATH, "kevin_food_unit_normalization/main.py")

# 指向「原始食譜資料」 
SEED_DATA_PATH = os.path.join(SRC_PATH, "kevin_initial_data/ytower_all_recipes.csv")

# ================= 2. DAG 設定 =================
default_args = {
    'owner': 'Kevin',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id="ytower_init_data_loader",  
    default_args=default_args,
    description="[初始化] 讀取本地 CSV 種子資料 -> 經由查表轉換 -> 寫入 Kafka",
    schedule_interval=None,            # 設定為 None，「手動觸發」才會跑
    start_date=pendulum.datetime(2023, 11, 20, tz="Asia/Taipei"),
    catchup=False,
    tags=["ytower", "init", "dump"],
) as dag:

    # 任務: 執行資料倒灌
    task_load_seed_data = BashOperator(
        task_id="load_seed_data_to_kafka",
        # 指令邏輯：
        # 呼叫 main.py
        # 輸入 (--input): 指定讀取 initial_data 資料夾裡的原始 CSV
        # 主題 (--topic): 指定寫入 ytower_recipes_normalized
        # 來源 (--source): 標記為 ytower
        bash_command=f"python3 {NORMALIZATION_SCRIPT} --input {SEED_DATA_PATH} --topic ytower_recipes_normalized --source ytower",
        
        # 環境變數設定
        env={
            # 萬一有新單位出現呼叫 API
            "GEMINI_API_KEY": "請將你的_GEMINI_API_KEY_貼在這裡", 
            **os.environ
        }
    )