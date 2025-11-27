import pandas as pd
from sqlalchemy import create_engine
import os
from urllib.parse import quote_plus

# ================= 設定區 =================
DB_HOST = '34.80.161.225'
DB_PORT = '3307'
DB_USER = 'root'
DB_PASS = 'password'
DB_NAME = 'EXAMPLE'  # 請確認資料庫名稱


STAGING_TABLE = 'recipes_kevin_test'

# 檔案路徑
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_FILENAME = 'recipes_for_upload.csv'
INPUT_FILE_PATH = os.path.join(BASE_DIR, INPUT_FILENAME)

def upload_staging():
    print(f"啟動安全模式：準備上傳食譜到暫存表 `{STAGING_TABLE}` ...")

    if not os.path.exists(INPUT_FILE_PATH):
        print(f"找不到上傳檔案: {INPUT_FILE_PATH}")
        print("   (請先執行 transform_recipes_full.py 產生檔案)")
        return

    # 1. 讀取 CSV
    # dtype=str 避免 ID 變成數字，keep_default_na=False 避免 NaN 問題
    df = pd.read_csv(INPUT_FILE_PATH)
    print(f"讀取 CSV 成功: {len(df)} 道食譜")

    # 2. 連線資料庫
    safe_password = quote_plus(DB_PASS)
    connection_str = f"mysql+pymysql://{DB_USER}:{safe_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(connection_str)

    try:
        # 3. 寫入暫存表
        print(f"正在寫入暫存表 `{STAGING_TABLE}`...")
        
        # if_exists='replace'：每次跑都會把舊的測試表刪掉重蓋，保證乾淨
        df.to_sql(STAGING_TABLE, engine, if_exists='replace', index=False, chunksize=1000)
        
        print(f"上傳成功！")

    except Exception as e:
        print(f"寫入失敗: {e}")

if __name__ == "__main__":
    upload_staging()