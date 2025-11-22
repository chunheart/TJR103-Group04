import requests
import pandas as pd 
import os # <-- 確保有匯入 os 模組
from requests.packages.urllib3.exceptions import InsecureRequestWarning
import warnings

# ⚠️ 抑制因 verify=False 而產生的警告
warnings.filterwarnings('ignore', category=InsecureRequestWarning) 

# **請替換成您的 API Key**
YOUR_API_KEY = "a33d72bf-02c6-4deb-87fc-897ff3354d47" 
DATA_ID = "CFP_P_02"
LIMIT = 1200 # 每次最大擷取筆數

# --- ⭐ 關鍵：使用絕對路徑確保儲存位置正確 ⭐ ---
try:
    # 取得當前 Python 腳本（esg.py）所在的目錄
    # 注意：在某些環境（如 Jupyter/Colab）中，__file__ 可能不可用，但在此PowerShell環境中通常可行
    script_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    # 備用方法：如果 __file__ 不可用，則使用當前工作目錄
    script_dir = os.getcwd()

FILE_NAME = "CFP_P_02_data.csv"
output_file_path = os.path.join(script_dir, FILE_NAME) # 組合完整的絕對路徑

print(f"--- 檔案預計儲存路徑 ---")
print(f"腳本目錄: {script_dir}")
print(f"輸出檔案: {output_file_path}")
print("--------------------------")
# --------------------------------------------------

# API 基礎 URL
base_url = "https://data.moenv.gov.tw/api/v2/"

params = {
    "format": "json",
    "limit": LIMIT,
    "offset": 0,
    "api_key": YOUR_API_KEY
}

all_data = []
total_records = 1 

print("🚀 開始擷取資料 (已禁用 SSL 驗證)...")

while params["offset"] < total_records:
    api_url = f"{base_url}{DATA_ID}"
    
    try:
        # 使用 verify=False 繞過 SSL 錯誤
        response = requests.get(api_url, params=params, verify=False) 
        response.raise_for_status() 
        
        data = response.json()
        
        if params["offset"] == 0:
            total_records = data.get("total", 0)
            print(f"✅ 成功連線！總共有 {total_records} 筆資料。")

        records = data.get("records", [])
        all_data.extend(records)
        
        # 檢查是否已取得所有資料
        if not records or len(records) < LIMIT:
             break
        
        # 更新 offset，準備擷取下一頁
        params["offset"] += LIMIT
        print(f"➡️ 已擷取 {len(all_data)} / {total_records} 筆資料...")
        
    except requests.RequestException as e:
        print(f"❌ 請求發生錯誤: {e}")
        break

# 將結果轉換為 DataFrame
if all_data:
    df = pd.DataFrame(all_data)
    print("✨ 資料擷取完成！")
    
    # ⭐ 儲存檔案時，使用絕對路徑
    df.to_csv(output_file_path, index=False, encoding='utf-8-sig') 
    print(f"\n💾 資料已儲存至: {output_file_path}")
    print("--- 檢查該路徑下是否有 CFP_P_02_data.csv 檔案 ---")
else:
    print("沒有資料被擷取。")