import pandas as pd
import os

# ================= 設定區 =================
INPUT_FILE = 'src/kevin_initial_data/ytower_recipes_normalized.csv'

# 輸出：只包含食材名稱的檔案
OUTPUT_FILE = 'ingredient_names_only.csv'

def extract_names():
    print(f"開始提取食材名稱...")

    if not os.path.exists(INPUT_FILE):
        print(f"找不到檔案: {INPUT_FILE}")
        return

    # 1. 讀取 CSV
    df = pd.read_csv(INPUT_FILE, dtype=str)
    print(f"📦 原始資料: {len(df)} 筆")

    # 2. 提取 Ingredient_Name 欄位
    df_names = df[['Ingredient_Name']]

    # 3. 去重：只保留唯一的名稱 (Unique)
    df_unique = df_names.drop_duplicates()
    
    print(f" 去除重複後，共有 {len(df_unique)} 種獨特食材")

    # 4. 存檔 (不存 index)
    df_unique.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
    
    print(f"檔案已建立: {OUTPUT_FILE}")

if __name__ == "__main__":
    extract_names()