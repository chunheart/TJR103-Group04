import pendulum
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import os

# ---------------------------------------------------------
# 台灣時區
# ---------------------------------------------------------
TW_TZ = pendulum.timezone("Asia/Taipei")

BASE_DIR = "/opt/airflow/logs/icook/data"  # daily.py 寫入的資料夾

default_args = {
    "owner": "airflow",
    "retries": 0,  # 有「檔案檢查」就不需要 retry
    "dagrun_timeout":timedelta(hours=1),
}

# ---------------------------------------------------------
# Python 檢查函式：昨天的 CSV 是否存在？
# ---------------------------------------------------------
def check_file_exists(**context):
    yesterday = (context['data_interval_start'] - timedelta(days=1)).in_timezone("Asia/Taipei")
    date_str = yesterday.strftime("%Y-%m-%d")

    file_path = f"{BASE_DIR}/{date_str}.csv"

    print(f"🔎 檢查是否已有檔案：{file_path}")

    if os.path.exists(file_path):
        print("🟩 檔案已存在，不執行爬蟲")
        return "skip_crawler"
    else:
        print("🟥 檔案不存在 → 應執行爬蟲")
        return "run_icook_daily"

with DAG(
    dag_id="icook_daily_crawler",
    default_args=default_args,
    schedule="0 9 * * *",    # 每天早上 09:00
    start_date=datetime(2025, 1, 1, tzinfo=TW_TZ),
    catchup=False,
    tags=["icook", "crawler", "csv"],
) as dag:

    # Step 1：檢查檔案
    check_task = PythonOperator(
        task_id="check_file",
        python_callable=check_file_exists,
        provide_context=True,
    )

    # Step 2：執行爬蟲（只有檔案不存在時）
    # adjust daily.py path
    run_crawler = BashOperator(
        task_id="run_icook_daily",
        bash_command="""
        python /opt/airflow/src/gina_icook_crawler/daily.py \
            --since "{{ (data_interval_start - macros.timedelta(days=0)).in_timezone('Asia/Taipei').strftime('%Y-%m-%d') }}" \
            --before "{{ (data_interval_start - macros.timedelta(days=0)).in_timezone('Asia/Taipei').strftime('%Y-%m-%d') }}" \
            --debug
        """,
    )

    # Step 3：跳過訊息（檔案存在時走這條）
    skip_task = BashOperator(
        task_id="skip_crawler",
        bash_command='echo "🟩 昨天資料已存在 → 自動跳過爬蟲"',
    )

    # 設定分支邏輯
    check_task >> [run_crawler, skip_task]

