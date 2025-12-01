import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.dialects.mysql import insert
import os
from urllib.parse import quote_plus

# ================= 設定區 =================
DB_HOST = '34.80.161.225'
DB_PORT = '3307'
DB_USER = 'root'
DB_PASS = 'meowsql@1234'
DB_NAME = 'EXAMPLE'

BASE_DIR = 'src/kevin_initial_data'
FILES = {
    'recipe': os.path.join(BASE_DIR, 'final_upload_recipe.csv'),
    'unit_normalize': os.path.join(BASE_DIR, 'final_upload_unit_normalize.csv'),
    'recipe_ingredient': os.path.join(BASE_DIR, 'final_upload_recipe_ingredient.csv')
}

# ================= 核心邏輯：自訂寫入方法 (INSERT IGNORE) =================
def insert_ignore_method(table, conn, keys, data_iter):
    """
    自訂 Pandas to_sql 的寫入方法，使用 MySQL 的 INSERT IGNORE 語法。
    遇到重複 Primary Key 自動跳過，不報錯，並繼續寫入下一筆。
    """
    sql_table = table.table
    columns = [col.name for col in sql_table.columns]
    
    # 建立 INSERT IGNORE 語句
    # 語法: INSERT IGNORE INTO table (col1, col2) VALUES (%s, %s), (%s, %s)...
    # 注意：這裡依賴 SQLAlchemy 的 bind params 機制
    
    data = [dict(zip(keys, row)) for row in data_iter]
    
    stmt = insert(sql_table).values(data).prefix_with('IGNORE')
    
    # 執行
    conn.execute(stmt)

def upload_all_v2():
    print("啟動上傳 (INSERT IGNORE)...")
    
    safe_pass = quote_plus(DB_PASS)
    engine = create_engine(f"mysql+pymysql://{DB_USER}:{safe_pass}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4")

    for table_name, file_path in FILES.items():
        if not os.path.exists(file_path):
            print(f"跳過：找不到 {file_path}")
            continue
            
        print(f"正在處理 `{table_name}` ...")
        try:
            # 分批讀取 (避免記憶體爆掉)
            chunksize = 5000
            count = 0
            
            # 使用 Pandas 的 chunksize 讀取功能
            for df_chunk in pd.read_csv(file_path, chunksize=chunksize):
                
                # 這裡使用 method=insert_ignore_method 來執行真正的忽略重複
                df_chunk.to_sql(
                    table_name, 
                    engine, 
                    if_exists='append', 
                    index=False, 
                    method=insert_ignore_method  # <--- 這就是魔法所在
                )
                count += len(df_chunk)
                print(f"   已處理 {count} 筆...", end='\r')
            
            print(f"\n{table_name} 處理完成！所有新資料已寫入 (重複已自動略過)。")
            
        except Exception as e:
            print(f"\n{table_name} 上傳失敗: {e}")

    print("\n去 DBeaver 檢查資料筆數是否增加")

if __name__ == "__main__":
    upload_all_v2()