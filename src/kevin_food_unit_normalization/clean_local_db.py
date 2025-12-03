import pandas as pd
import re
import unicodedata

# ================= 設定區 =================
INPUT_CSV = 'src/kevin_food_unit_normalization/unit_mapping_db.csv'
OUTPUT_CSV = 'src/kevin_food_unit_normalization/unit_mapping_db_cleaned_v3.csv'

# ================= 清洗邏輯：單位 (修正版) =================
def clean_ingredient_unit(unit):
    if pd.isna(unit): return ""
    
    # 1. 全形轉半形 & 轉小寫
    unit = unicodedata.normalize('NFKC', str(unit))
    unit = unit.lower()
    
    # 2. 移除 "又" (例如: "又1/2個" -> "1/2個")
    unit = unit.replace('又', '')
    
    # 3. 只移除特殊符號，保留數字
    # 移除對象：~ (波浪), ∼ (全形波浪), + (加號), - (減號), × (乘號), * (星號)
    # 注意：不包含 0-9, ., / 
    unit = re.sub(r'[~∼+\-×*]', '', unit)
    
    # 4. 去除頭尾空白
    return unit.strip()

# ================= 清洗邏輯：食材名稱 (保持不變) =================
def clean_ingredient_name(name):
    if pd.isna(name): return ""
    name = unicodedata.normalize('NFKC', str(name))
    # 去除 A. B. 1. 等序號
    name = re.sub(r'^[A-Za-z0-9]+\.', '', name)
    # 去除冒號後的標籤
    name = re.sub(r'^.*[：:]', '', name)
    return name.strip()

# ================= 主程式 =================
def main():
    print(f"開始清洗：{INPUT_CSV}")
    try:
        df = pd.read_csv(INPUT_CSV)
    except FileNotFoundError:
        print("找不到檔案！")
        return

    # 備份原始資料
    df['Original_Unit'] = df['Unit']

    # 執行清洗
    df['Ingredient_Name'] = df['Ingredient_Name'].apply(clean_ingredient_name)
    df['Unit'] = df['Unit'].apply(clean_ingredient_unit)

    # 過濾空值
    df = df[df['Ingredient_Name'] != ""]
    df = df[df['Unit'] != ""]

    # 去除完全重複的行
    before_count = len(df)
    df = df.drop_duplicates(subset=['Ingredient_Name', 'Unit'], keep='first')
    after_count = len(df)

    # 找出有變動的資料印出來看
    changed_df = df[df['Unit'] != df['Original_Unit']]
    if not changed_df.empty:
        print(changed_df[['Original_Unit', 'Unit']].head(15).to_string(index=False))

    print(f"統計：")
    print(f"原始筆數：{before_count}")
    print(f"清洗後筆數：{after_count}")

    # 存檔
    df_final = df[['Ingredient_Name', 'Unit', 'Grams_Per_Unit']]
    df_final.to_csv(OUTPUT_CSV, index=False, encoding='utf-8-sig')
    
if __name__ == "__main__":
    main()