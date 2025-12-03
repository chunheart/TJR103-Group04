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

# 輸出的報表檔名
OUTPUT_FILE = 'final_carbon_emission_report.csv'

# 你的 SQL 查詢語法 (直接貼過來)
SQL_QUERY = """
SELECT 
    r.recipe_name, 
    r.recipe_url, 
    r.author, 
    r.servings,
    ri.recipe_id, 
    ri.recipe_site, 
    ri.ori_ingredient_name, 
    ri.unit_name, 
    ri.unit_value,
    un.weight_grams,
    i.nor_ingredient_name, 
    ce.weight_g2g,
    (ri.unit_value * un.weight_grams * ce.weight_g2g) AS ingr_total_coe
FROM recipe_ingredient ri
LEFT JOIN ingredient_normalize i ON i.ori_ingredient_id = ri.ori_ingredient_id
LEFT JOIN carbon_emission ce ON ce.nor_ingredient_name = i.nor_ingredient_name
LEFT JOIN unit_normalize un ON un.unit_name = ri.unit_name AND un.ori_ingredient_id = ri.ori_ingredient_id
LEFT JOIN recipe r ON r.recipe_id = ri.recipe_id AND r.recipe_site = ri.recipe_site
WHERE ri.recipe_site = 'ytower'  -- 我們只抓楊桃網的成果來檢查
"""

def export_report():
    print("正在連線資料庫並執行查詢 ")
    
    # 建立連線
    safe_pass = quote_plus(DB_PASS)
    engine = create_engine(f"mysql+pymysql://{DB_USER}:{safe_pass}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4")

    try:
        # 執行 SQL 並讀入 DataFrame
        df = pd.read_sql(SQL_QUERY, engine)
        
        print(f"查詢成功，共撈出 {len(df)} 筆資料")
        
        # 簡單檢查一下有沒有算出來 (檢查 ingr_total_coe 是否有值)
        valid_rows = df[df['ingr_total_coe'].notnull()]
        print(f"其中成功計算出碳排數據的有: {len(valid_rows)} 筆")
        
        # 存成 CSV
        # utf-8-sig 可以確保 Excel 打開中文不亂碼
        df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8-sig')
        
        print(f"\n報表已儲存至: {OUTPUT_FILE}")
        print("最終成果報表！")
        
    except Exception as e:
        print(f"匯出失敗: {e}")

if __name__ == "__main__":
    export_report()