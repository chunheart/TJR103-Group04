import subprocess
import time
from datetime import datetime, timedelta
import pytz

tz = pytz.timezone("Asia/Taipei")

def run_daily_crawler():
    now = datetime.now(tz)
    yesterday = (now - timedelta(days=1)).date().isoformat()

    print(f"🕒 目前系統時間（台灣時間）：{now}")
    print(f"📅 將自動抓取 {yesterday} 的食譜資料...")

    cmd = [
        "poetry", "run", "python", "-m", "icook_crawler.daily",
        "--since", yesterday, "--before", yesterday,
        "--sleep", "1.5", "--max-pages", "40"
    ]
    subprocess.run(cmd)

print("📅 自動排程啟動中... 每天 22:00 執行食譜爬蟲。")

while True:
    now = datetime.now(tz)
    if now.hour == 9 and now.minute == 0:
        run_daily_crawler()
        print("✅ 今日任務完成，等待明天再執行。")
        time.sleep(3600)
    else:
        time.sleep(30)
