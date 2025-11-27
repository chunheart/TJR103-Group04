import pandas as pd
import re
import unicodedata
import os

# ================= 設定區 =================
# 輸出檔名
OUTPUT_FILE = 'all_ingredient_names_cleaned_v5.csv'

# ================= 核心清洗邏輯 (V5 終極版) =================
def super_clean_name_v5(name):
    if pd.isna(name):
        return ""
    
    # 1. 統一編碼 (Full-width to Half-width)
    name = unicodedata.normalize('NFKC', str(name))
    
    # 2. 移除干擾符號 (?, 換行, 引號)
    name = re.sub(r'[?？"\']', '', name)
    name = name.replace('\n', '').replace('\r', '')

    # 3. 處理分隔符號 
    # 針對 "醬汁..醬油膏" -> 取 "醬油膏"
    # 針對 "調味料:鹽" -> 取 "鹽"
    for separator in ['..', ':', '：']:
        if separator in name:
            name = name.split(separator)[-1]

    # 4. 去除前綴序號 (A., 1., a.)
    name = re.sub(r'^[a-zA-Z0-9]+\.', '', name)

    # 5. 去除後綴標籤 
    # 去除結尾的單個英文字母 (如 "細砂糖A" -> "細砂糖")
    name = re.sub(r'[A-Z]$', '', name)
    
    # 6. 去除溫度與規格 
    # 如 "熱水85°C" -> "熱水"
    name = re.sub(r'\d+°C', '', name)
    name = re.sub(r'\d+度', '', name)
    
    # 7. 去除尾隨數字 
    # 如 "水300" -> "水"
    name = re.sub(r'\d+$', '', name)

    # 8. 去除殘留符號
    name = name.strip()
    name = name.strip('°') # 去除可能剩下的度數符號

    return name

# ================= 主程式 =================
def main():
    print("資料清洗開始")

    # 自動搜尋檔案
    target_file = None
    possible_paths = [
        'src/kevin_initial_data/ytower_recipes_normalized.csv',
        'ytower_recipes_normalized.csv',
        'src/kevin_initial_data/ytower_all_recipes.csv',
        'ingredient_names_only.csv'
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            target_file = path
            print(f"讀取檔案: {target_file}")
            break
    
    if not target_file:
        print("找不到輸入檔案")
        return

    # 1. 讀取
    df = pd.read_csv(target_file, dtype=str)
    
    # 2. 備份原始名稱
    if 'Original_Name' not in df.columns:
        df['Original_Name'] = df['Ingredient_Name']

    # 3. 執行清洗
    df['Cleaned_Name'] = df['Ingredient_Name'].apply(super_clean_name_v5)

    # 4. 過濾空值
    df = df[df['Cleaned_Name'] != ""]

    # 5. 去重
    df_unique = df[['Cleaned_Name']].drop_duplicates()

    # 6. 結果
    print("\n 清洗展示：")
    weird_examples = ["醬汁..醬油膏", "細砂糖A", "水300", "熱水85°C", "?砂糖"]
    # 從資料中找出類似的 pattern 展示
    mask = df['Original_Name'].apply(lambda x: any(ex in str(x) for ex in ['..', '300', '°C', '?', 'A']))
    sample = df[mask & (df['Original_Name'] != df['Cleaned_Name'])].drop_duplicates(subset=['Cleaned_Name'])
    
    if not sample.empty:
        print(sample[['Original_Name', 'Cleaned_Name']].head(10).to_string(index=False))
    else:
        print(df[df['Original_Name'] != df['Cleaned_Name']][['Original_Name', 'Cleaned_Name']].head(10).to_string(index=False))

    # 7. 存檔
    df_unique.columns = ['Ingredient_Name']
    df_unique.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
    
    print(f" 完成！清單已儲存至: {OUTPUT_FILE}")
    print(f"總共提取 {len(df_unique)} 個食材名稱")

if __name__ == "__main__":
    main()