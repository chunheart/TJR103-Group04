import pandas as pd
import os
import re
import unicodedata
from datetime import datetime
import numpy as np

# ================= 設定區 =================
BASE_DIR = 'src/kevin_initial_data'
CLASSMATE_FILE = os.path.join(BASE_DIR, 'ingredient_normalize_mysql.csv')
MY_YTOWER_FILE = os.path.join(BASE_DIR, 'ytower_recipes_normalized.csv')

# 輸出檔案
OUT_RECIPE = os.path.join(BASE_DIR, 'final_upload_recipe.csv')
OUT_RECIPE_INGR = os.path.join(BASE_DIR, 'final_upload_recipe_ingredient.csv')
OUT_UNIT = os.path.join(BASE_DIR, 'final_upload_unit_normalize.csv')

# ================= V6 清洗邏輯 (搬過來用) =================
def super_clean_name_v6(name):
    if pd.isna(name): return ""
    name = unicodedata.normalize('NFKC', str(name)) # 統一全形半形
    name = re.sub(r'[\(（].*?[\)）]', '', name)      # 去括號
    for sep in [':', '：', '..']:                   # 去冒號前綴
        if sep in name: name = name.split(sep)[-1]
    name = re.sub(r'^.*[A-Za-z0-9\u0370-\u03FF]+\.', '', name) # 去標題序號
    name = re.sub(r'^[A-Za-z](?=[\u4e00-\u9fa5])', '', name)   # 去開頭單字
    name = re.sub(r'\d+°C?$', '', name)             # 去溫度
    name = re.sub(r'[\d./]+$', '', name)            # 去尾隨數字
    return name.strip().strip('°')

def prepare_files_safe():
    print("啟動整合模式 ")

    # 1. 讀取檔案
    if not os.path.exists(CLASSMATE_FILE) or not os.path.exists(MY_YTOWER_FILE):
        print("找不到輸入檔案，請檢查路徑！")
        return

    # 載入同學的身分證表
    df_id_map = pd.read_csv(CLASSMATE_FILE)
    # 建立對照字典: {'雞蛋': 101, '高筋麵粉': 102}
    name_to_id = dict(zip(df_id_map['ori_ingredient_name'], df_id_map['ori_ingredient_id']))
    print(f"已載入 ID 對照表: {len(name_to_id)} 筆")

    # 載入你的原始資料
    df_ytower = pd.read_csv(MY_YTOWER_FILE, dtype=str)
    print(f"載入食譜資料: {len(df_ytower)} 筆")

    # 2. 【關鍵】現場清洗名稱 (確保能對上同學的 ID)
    print("正在對食譜資料進行清洗...")
    # 新增一個暫存欄位 'Cleaned_Name' 用來對照
    df_ytower['Cleaned_Name'] = df_ytower['Ingredient_Name'].apply(super_clean_name_v6)

    # 3. 配對 ID (用洗乾淨的名字去配)
    print("正在配對 ID...")
    df_ytower['ori_ingredient_id'] = df_ytower['Cleaned_Name'].map(name_to_id)

    # 4. 檢查配對率 (驗收)
    missing_df = df_ytower[df_ytower['ori_ingredient_id'].isna()]
    match_count = len(df_ytower) - len(missing_df)
    print(f"   - 成功配對: {match_count} 筆")
    print(f"   - 失敗(找不到ID): {len(missing_df)} 筆")
    
    if len(missing_df) > 0:
        print("\n 失敗範例 (這些食材在同學表裡找不到):")
        print(missing_df['Cleaned_Name'].unique()[:10])
        # 過濾掉失敗的，以免上傳報錯
        df_ytower = df_ytower.dropna(subset=['ori_ingredient_id'])

    # 格式整理
    df_ytower['ori_ingredient_id'] = df_ytower['ori_ingredient_id'].astype(int)
    # 處理數值
    df_ytower['Normalized_Weight_g'] = pd.to_numeric(df_ytower['Normalized_Weight_g'], errors='coerce').fillna(0)
    df_ytower['Weight'] = pd.to_numeric(df_ytower['Weight'], errors='coerce').fillna(0)
    
    # 準備時間戳記
    curr_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # ================= 產出 1: recipe =================
    print(" 產生 final_upload_recipe.csv...")
    df_recipe = df_ytower.drop_duplicates(subset=['ID'])[['ID', 'site', 'Recipe_Title', 'Recipe_URL', 'Author', 'Servings', 'Publish_Date', 'Crawl_Time']].copy()
    df_recipe.columns = ['recipe_id', 'recipe_site', 'recipe_name', 'recipe_url', 'author', 'servings', 'publish_time', 'crawl_time']
    df_recipe['ins_time'] = curr_time
    df_recipe['upd_time'] = curr_time
    df_recipe.to_csv(OUT_RECIPE, index=False, encoding='utf-8-sig')

    # ================= 產出 2: unit_normalize =================
    print("產生 final_upload_unit_normalize.csv...")
    # 這裡我們用 'Cleaned_Name' 作為名稱，確保跟 ID 對得上
    df_unit = df_ytower[['ori_ingredient_id', 'Cleaned_Name', 'Unit', 'Normalized_Weight_g']].drop_duplicates()
    
    # 如果有重複組合，取平均重
    df_unit = df_unit.groupby(['ori_ingredient_id', 'Cleaned_Name', 'Unit'])['Normalized_Weight_g'].mean().reset_index()
    
    df_unit.columns = ['ori_ingredient_id', 'ori_ingredient_name', 'unit_name', 'weight_grams']
    df_unit['ins_time'] = curr_time
    df_unit['upd_time'] = curr_time
    df_unit['u2g_status'] = 'done'
    df_unit.to_csv(OUT_UNIT, index=False, encoding='utf-8-sig')

# ================= 產出 3: recipe_ingredient =================
    print(" 正在製作 recipe_ingredient 表")
    
    df_ri = df_ytower[['ID', 'site', 'ori_ingredient_id', 'Cleaned_Name', 'Unit', 'Weight', 'Type']].copy()
    
    # 將 'Type' 改名為 'ingredient_type' (配合資料庫)
    df_ri.columns = ['recipe_id', 'recipe_site', 'ori_ingredient_id', 'ori_ingredient_name', 'unit_name', 'unit_value', 'ingredient_type']
    
    # 補上時間戳記
    df_ri['ins_time'] = curr_time
    df_ri['upd_time'] = curr_time
    
    # 存檔
    df_ri.to_csv(OUT_RECIPE_INGR, index=False, encoding='utf-8-sig')

    print("\n 全部完成！產出三個檔案：")
    print(f"1. {OUT_RECIPE} (食譜)")
    print(f"2. {OUT_UNIT} (單位換算)")
    print(f"3. {OUT_RECIPE_INGR} (食譜-食材關聯 - 含分類)")

if __name__ == "__main__":
    prepare_files_safe()