import pandas as pd
from datetime import datetime
import numpy as np
import os

# ================= 設定區 =================
# 原始資料路徑
SOURCE_FILE = 'src/kevin_ytower_crawler/ytower_csv_output/ytower_all_recipes.csv' 
# 轉換後的輸出檔名
OUTPUT_FILE = 'recipes_for_upload.csv'

def transform_full():
    print(f"執行完整轉換...")
    
    if not os.path.exists(SOURCE_FILE):
        print(f"找不到檔案: {SOURCE_FILE}")
        return

    # 1. 讀取所有資料
    # dtype=str 確保 ID 不會變成數字
    df_source = pd.read_csv(SOURCE_FILE, dtype=str)
    print(f"讀取原始資料: {len(df_source)} 筆")

    # 2. 去除重複 (只保留唯一的食譜 ID)
    df_recipes = df_source.drop_duplicates(subset=['ID'])
    print(f"去重後食譜數量: {len(df_recipes)} 道")

    # 3. 欄位對應與轉換
    df_target = pd.DataFrame()
    
    df_target['recipe_id'] = df_recipes['ID']
    df_target['recipe_site'] = df_recipes['site']
    df_target['recipe_name'] = df_recipes['Recipe_Title']
    df_target['recipe_url'] = df_recipes['Recipe_URL']
    
    # 處理空值 'null' -> None
    df_target['author'] = df_recipes['Author'].replace('null', np.nan)
    df_target['servings'] = df_recipes['Servings'].replace('null', np.nan)
    
    df_target['publish_time'] = df_recipes['Publish_Date']
    df_target['crawl_time'] = df_recipes['Crawl_Time']
    
    # 新增寫入時間
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df_target['ins_time'] = current_time
    df_target['upd_time'] = current_time

    # 4. 存檔
    df_target.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
    
    print(f"轉換完成！已儲存至: {OUTPUT_FILE}")
    print("一步：執行 upload_recipes.py 上傳到資料庫。")

if __name__ == "__main__":
    transform_full()