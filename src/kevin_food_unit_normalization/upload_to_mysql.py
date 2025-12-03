import pandas as pd
from sqlalchemy import create_engine
import os
from urllib.parse import quote_plus

# ================= 設定區 =================
DB_HOST = '34.80.161.225'
DB_PORT = '3307'
DB_USER = 'root'
DB_PASS = 'password'
DB_NAME = 'EXAMPLE'

TARGET_TABLE = 'unit_normalize_kevin_test' 

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_FILE_PATH = os.path.join(BASE_DIR, 'unit_mapping_db_for_upload.csv')

def upload_staging():
    print(f"上傳至暫存表 [{TARGET_TABLE}]...")

    if not os.path.exists(INPUT_FILE_PATH):
        print(f"找不到檔案: {INPUT_FILE_PATH}")
        return
    
    df = pd.read_csv(INPUT_FILE_PATH)
    print(f"讀取 CSV 成功: {len(df)} 筆")

    # 連線
    safe_password = quote_plus(DB_PASS)
    connection_str = f"mysql+pymysql://{DB_USER}:{safe_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(connection_str)

    try:
        print(f"寫入暫存表 `{TARGET_TABLE}`...")
        # if_exists='replace'：如果測試表已經存在，就刪掉重蓋,不會影響到正式的 unit_normalize
        df.to_sql(TARGET_TABLE, engine, if_exists='replace', index=False, chunksize=1000)
        
    except Exception as e:
        print(f"寫入失敗: {e}")

if __name__ == "__main__":
    upload_staging()