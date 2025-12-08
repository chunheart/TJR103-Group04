import schedule
import time
import subprocess
import datetime
import pytz
import sys
import os
import argparse

# ===============================
# 即時輸出設定（讓 docker logs 立即顯示）
# ===============================
sys.stdout.reconfigure(line_buffering=True)

# ===============================
# 台灣時區設定
# ===============================
tz = pytz.timezone("Asia/Taipei")

# ===============================
# Log 檔案設定
# ===============================
LOG_DIR = "/app/logs"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "scheduler.log")

def write_log(message: str):
    """同時寫入 log 檔與 docker logs"""
    timestamp = datetime.datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
    formatted = f"[{timestamp}] {message}"
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(formatted + "\n")
    print(formatted, flush=True)

# ===============================
# 執行每日爬蟲任務
# ===============================
def run_daily_job(retry=False):
    today = datetime.datetime.now(tz)
    yesterday = today - datetime.timedelta(days=1)
    since = yesterday.strftime("%Y-%m-%d")
    before = yesterday.strftime("%Y-%m-%d")

    write_log("===============================================")
    write_log(f"執行日期：{today.strftime('%Y-%m-%d %H:%M:%S')}（台灣時間）")
    write_log(f"爬取目標日期：{since}")
    write_log("===============================================")

    cmd = [
        "poetry", "run", "python", "-m", "icook_crawler.daily",
        "--since", since,
        "--before", before,
        "--start-page", "1",
        "--max-pages", "10",
        "--sleep", "1.5",
        "--debug"
    ]

    try:
        write_log(f"執行指令：{' '.join(cmd)}")
        subprocess.run(cmd, check=True)
        write_log(f"{since} 爬取完成")
    except subprocess.CalledProcessError as e:
        write_log(f"❌ 爬蟲執行失敗：{e}")

        # 若尚未重試過 → 延遲 5 分鐘後自動再試一次
        if not retry:
            write_log("5 分鐘後將自動重試一次...")
            time.sleep(300)
            run_daily_job(retry=True)
        else:
            write_log("已重試過一次，停止嘗試。")

# ===============================
# 防漏保險機制設定
# ===============================
last_run_date = None   # 記錄上次執行日期，避免重複補跑

def check_backup_run():
    """若 09:00 未執行，09:05 自動補跑"""
    global last_run_date
    now = datetime.datetime.now(tz)
    current_date = now.date()
    
    if last_run_date != current_date and now.hour == 9 and now.minute >= 5:
        write_log("偵測到 09:00 任務可能未觸發，自動補跑一次！")
        run_daily_job()
        last_run_date = current_date

# ===============================
# 每天早上 09:00 自動執行
# ===============================
def wrapped_daily_job():
    """包裝 run_daily_job 並更新 last_run_date"""
    global last_run_date
    run_daily_job()
    last_run_date = datetime.datetime.now(tz).date()

schedule.every().day.at("09:00").do(wrapped_daily_job)

# ===============================
# 解析命令列參數
# ===============================
parser = argparse.ArgumentParser()
parser.add_argument("--run-now", action="store_true", help="立即執行一次爬蟲任務（跳過等待）")
args = parser.parse_args()

# ===============================
# 啟動邏輯
# ===============================
if args.run_now:
    write_log("偵測到 --run-now，立即執行每日爬蟲任務！")
    run_daily_job()
else:
    write_log("自動排程啟動中...")
    write_log("每天早上 09:00 自動執行食譜爬蟲（含防漏保險機制）。")
    write_log(f"系統時間（台灣時間）：{datetime.datetime.now(tz)}")
    write_log("===============================================")

    # 持續執行排程 + 狀態顯示 + 防漏偵測
    while True:
        schedule.run_pending()
        check_backup_run()  # 防漏保險檢查
        write_log("等候中...")
        time.sleep(10)      # 每 10 秒檢查一次

