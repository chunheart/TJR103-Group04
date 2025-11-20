# Food Unit Normalization (食材單位正規化工具)

此模組負責將爬蟲抓取的非標準食材單位（如：1條、1把、少許、1杯），
透過 **規則查表** 與 **生成式 AI (Google Gemini)** 的混合模式，轉換為標準的 **公克數 (g)**，以利後續的數據分析與碳排放計算。

##  功能特色

1.  **混合式精準換算**：
    * **特定食材規則**：能區分不同食材的比重（例如：1杯麵粉=120g，但 1杯糖=200g）。
    * **台灣在地標準**：內建台斤 (600g)、兩 (37.5g)、大匙 (15ml) 等標準換算。

2.  **AI 智慧補強**：
    * 對於未知單位（如：一條絲瓜、一朵香菇），自動呼叫 **Google Gemini 2.5 Flash** 進行估算。

3.  **成本最佳化 (Cost-Effective)**：
    * **本地快取 (Local Cache)**：AI 查過的結果會自動儲存於 `unit_mapping_db.csv`。下次遇到相同單位直接查表。
    
4.  **保護設計**： 內建自動重試 (Retry) 與編碼保護機制，防止 API 連線錯誤導致中斷。

##  安裝需求

確保專案環境已安裝以下套件（若使用 Poetry 則已包含）：
* `pandas`
* `google-genai`

##  設定 API Key
## 本程式使用 Google Gemini API。請打開 `main.py`，將您的 API Key 填入配置區：
如何使用自己的API Key，請查看教學網站如下：https://lifecheatslab.com/freegeminiapi/


## 如何執行
請在專案根目錄 ( TJR103-Group04... ) 執行以下指令：
Bash
poetry run python3 src/kevin_food_unit_normalization/main.py

##  輸入與輸出
程式會自動讀取爬蟲產生的 CSV，處理後輸出新檔案：

檔案類型	路徑	說明
輸入 (Input)	.../ytower_csv_output/ytower_all_recipes.csv	原始爬蟲資料
輸出 (Output)	.../ytower_csv_output/ytower_recipes_normalized.csv	新增 Normalized_Weight_g 欄位
知識庫 (DB)	src/kevin_food_unit_normalization/unit_mapping_db.csv	累積的 AI 換算紀錄

## 轉換邏輯優先順序
為了確保準確度與效率，程式依照以下順序進行判斷：
特定食材規則 (Specific Rules)：最優先 (如：麵粉+杯)。
通用重量規則 (Standard Rules)：處理標準單位 (如：斤、兩、少許)。
AI 知識庫 (History DB)：檢查是否曾經問過 AI。
AI API 查詢 (Gemini)：遇到全新單位時才發送請求。
容量密度估算 (Volume)：最後手段，依密度=1 估算 (如：大匙、ml)。




```python
# src/kevin_food_unit_normalization/main.py

# 建議使用環境變數，或直接填入字串
API_KEY = "請在此填入您的_API_KEY"




