import pandas as pd
import json
import time
import os
from pathlib import Path
from typing import List, Dict, Optional, Union
from google import genai
from google.genai import types

# ================= CONFIGURATION =================
# API Key 從環境變數讀取，避免推送到 GitHub
API_KEY = os.getenv("GEMINI_API_KEY", "在此填入您的_API_KEY") 
MODEL_NAME = "gemini-2.5-flash"

# 檔案路徑設定 (相對於此腳本位置)
CURRENT_DIR = Path(__file__).parent
MAPPING_DB_FILE = CURRENT_DIR / "unit_mapping_db.csv"

# ================= 轉換規則庫 =================

# 1. General Unit Conversion
# 優先級：低 (當特定食材規則無法匹配時使用)
STANDARD_RULES: Dict[str, float] = {
    "kg": 1000, "公斤": 1000, 
    "g": 1, "克": 1, "公克": 1,
    "斤": 600, "台斤": 600, 
    "兩": 37.5, 
    "磅": 453.6, "lb": 453.6, 
    "oz": 28.35, "盎司": 28.35,
    "少許": 0.5, "適量": 1.0, "一小撮": 0.5, "把": 30.0,
}

# 2. Specific Ingredient Rules
# 優先級：高 (最精準的匹配)
# 格式: (食材關鍵字, 單位): 公克數
SPECIFIC_RULES: Dict[tuple, float] = {
    # --- 蛋類 ---
    ("蛋", "個"): 50.0, ("雞蛋", "個"): 50.0, ("全蛋", "個"): 50.0,
    ("蛋黃", "個"): 20.0, ("蛋白", "個"): 30.0, ("連殼雞蛋", "個"): 65.0, 
    ("B.雞蛋", "顆"): 50.0,
    # --- 米/穀物 ---
    ("白米", "杯"): 145.0, ("米", "杯"): 145.0, ("糯米粉", "杯"): 120.0,
    ("糖", "杯"): 200.0, ("砂糖", "杯"): 200.0, ("細砂糖", "杯"): 200.0,
    ("麵粉", "杯"): 120.0, ("低筋麵粉", "杯"): 120.0, 
    ("中筋麵粉", "杯"): 120.0, ("高筋麵粉", "杯"): 120.0,
    ("油", "杯"): 227.0, ("奶油", "大匙"): 13.0,
}

# 3. Volume to Weight, assume density=1 if unknown
# 優先級：最低 (作為最後手段)
VOLUME_TO_ML: Dict[str, float] = {
    "大匙": 15, "tbsp": 15, "T": 15, "匙": 15,
    "小匙": 5, "tsp": 5, "t": 5, "茶匙": 5,
    "杯": 240, "cup": 240, "C": 240, "米杯": 180,
    "ml": 1, "cc": 1, "㏄": 1, "公升": 1000, "L": 1000,
    "又1/2杯": 360, "又1/2大匙": 22.5
}

class IngredientNormalizer:
    """
    食材單位標準化工具
    功能：將各種非標準單位 (如: 1條, 1杯, 少許) 轉換為標準公克數 (g)。
    機制：規則查表 -> 歷史資料庫 -> Gemini AI 估算
    """
    
    def __init__(self):
        self.client = genai.Client(api_key=API_KEY)
        self.mapping_db = self._load_mapping_db()
        
    def _load_mapping_db(self) -> pd.DataFrame:
        """讀取或初始化本地知識庫 CSV"""
        if MAPPING_DB_FILE.exists():
            print(f"📚 讀取 AI 知識庫：{MAPPING_DB_FILE}")
            try:
                return pd.read_csv(MAPPING_DB_FILE)
            except pd.errors.EmptyDataError:
                pass
        
        print(" 建立新的 AI 知識庫")
        return pd.DataFrame(columns=['Ingredient_Name', 'Unit', 'Grams_Per_Unit'])

    def _save_mapping_db(self):
        """儲存知識庫到 CSV"""
        if not self.mapping_db.empty:
            self.mapping_db.to_csv(MAPPING_DB_FILE, index=False, encoding='utf-8-sig')
            print(f" AI 知識庫已更新，目前共有 {len(self.mapping_db)} 筆規則")

    def ask_gemini(self, items_chunk: List[Dict]) -> Optional[Dict]:
        """
        呼叫 Gemini API 進行單位估算
        包含 Retry 機制與 JSON 編碼保護
        """
        # 強制 ASCII 編碼，防止傳輸錯誤
        json_str = json.dumps(items_chunk, ensure_ascii=True)
        
        prompt = f"""
        You are a helper for normalizing recipe ingredient units to grams (g).
        Input Data (JSON): {json_str}
        
        Please output a JSON object with format: {{ "items": [ {{ "name": "...", "unit": "...", "g_per_unit": float }} ] }}
        
        Rules:
        1. Estimate the weight in grams for 1 unit of the ingredient.
        2. For volume units (bowl, cup) not in standard list, estimate based on density.
        3. For vague units (pinch, some), use 0.5 to 2.0.
        4. If 'unit' is 'piece/stick/clove' etc., estimate average weight (e.g. 1 cucumber ~ 100g).
        5. If unknown, return 0.
        """
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = self.client.models.generate_content(
                    model=MODEL_NAME,
                    contents=prompt,
                    config=types.GenerateContentConfig(response_mime_type="application/json")
                )
                return json.loads(response.text)
            except Exception as e:
                print(f"⚠️ API Error (Attempt {attempt+1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(5 * (attempt + 1))
                else:
                    print("❌ API Call Failed after retries.")
                    return None

    def process_csv(self, input_csv_path: Path, output_csv_path: Path):
        """主處理流程"""
        print(f"\n🚀 開始處理檔案：{input_csv_path}")
        try:
            df = pd.read_csv(input_csv_path)
        except FileNotFoundError:
            print(f"❌ 找不到檔案：{input_csv_path}")
            return

        # 1. Identify Unknowns
        candidates = df[df['Unit'].notna()][['Ingredient_Name', 'Unit']].drop_duplicates()
        existing_db_keys = set(zip(self.mapping_db['Ingredient_Name'], self.mapping_db['Unit']))
        
        unknown_pairs = []
        for _, row in candidates.iterrows():
            name, unit = str(row['Ingredient_Name']), str(row['Unit'])
            
            # Skip if matches hardcoded rules
            if unit in STANDARD_RULES or unit in VOLUME_TO_ML: continue
            
            matched_specific = False
            for (r_n, r_u), _ in SPECIFIC_RULES.items():
                if r_n in name and r_u == unit: 
                    matched_specific = True; break
            if matched_specific: continue

            # Skip if already in DB
            if (name, unit) in existing_db_keys: continue
            
            unknown_pairs.append({'name': name, 'unit': unit})
        
        print(f"📊 需透過 AI 估算的特殊組合：{len(unknown_pairs)} 筆")

        # 2. AI 批次處理 (Batch Processing)
        if unknown_pairs:
            BATCH_SIZE = 10
            new_records = []
            print(f"🤖 開始呼叫 {MODEL_NAME} API...")
            
            for i in range(0, len(unknown_pairs), BATCH_SIZE):
                batch = unknown_pairs[i:i+BATCH_SIZE]
                print(f"   處理進度: {i+1}/{len(unknown_pairs)}...")
                
                result = self.ask_gemini(batch)
                if result and 'items' in result:
                    for item in result['items']:
                        new_records.append({
                            'Ingredient_Name': item['name'],
                            'Unit': item['unit'],
                            'Grams_Per_Unit': item['g_per_unit']
                        })
                time.sleep(3) # Rate limit buffer

            if new_records:
                new_df = pd.DataFrame(new_records)
                if not self.mapping_db.empty:
                     self.mapping_db = pd.concat([self.mapping_db, new_df], ignore_index=True)
                else:
                     self.mapping_db = new_df
                self._save_mapping_db()

        # 3. Data Normalization
        print(" 正在進行單位換算...")
        ai_mapping = dict(zip(zip(self.mapping_db['Ingredient_Name'], self.mapping_db['Unit']), self.mapping_db['Grams_Per_Unit']))

        def convert_row(row):
            w_str = str(row['Weight'])
            u = str(row['Unit'])
            name = str(row['Ingredient_Name'])
            
            # Parse Weight (Handle fractions and NaN)
            try:
                if pd.isna(row['Weight']) or w_str.lower() in ['nan', 'null', '']:
                    # Default weight=1 for implicit units (e.g., "salt: some")
                    w = 1.0 if (u in STANDARD_RULES or u in VOLUME_TO_ML) else 0
                elif '/' in w_str:
                    w = float(eval(w_str))
                else:
                    w = float(w_str)
            except:
                w = 0

            # Conversion Logic Hierarchy
            # 1. Specific Rules (Most accurate)
            for (r_n, r_u), val in SPECIFIC_RULES.items():
                if r_n in name and r_u == u: return w * val

            # 2. Standard Weight Units
            if u in STANDARD_RULES: return w * STANDARD_RULES[u]
            
            # 3. AI Knowledge Base
            ai_factor = ai_mapping.get((name, u))
            if ai_factor is not None: return w * ai_factor

            # 4. Volume Density Estimation
            if u in VOLUME_TO_ML: return w * VOLUME_TO_ML[u]
            
            return None

        df['Normalized_Weight_g'] = df.apply(convert_row, axis=1)
        df.to_csv(output_csv_path, index=False, encoding='utf-8-sig')
        print(f" 轉換完成！結果已儲存至：{output_csv_path}")

if __name__ == "__main__":
    # 自動定位專案根目錄 (假設此腳本在 src/sub_folder/ 下)
    project_root = Path(__file__).parents[2]
    
    # 設定輸入與輸出檔案路徑
    input_csv = project_root / "src/kevin_ytower_crawler/ytower_csv_output/ytower_all_recipes.csv"
    output_csv = project_root / "src/kevin_ytower_crawler/ytower_csv_output/ytower_recipes_normalized.csv"
    
    if input_csv.exists():
        normalizer = IngredientNormalizer()
        normalizer.process_csv(input_csv, output_csv)
    else:
        print(f" 找不到輸入檔案：{input_csv}")
        print("請確認爬蟲是否已執行並產生 CSV 檔案。")