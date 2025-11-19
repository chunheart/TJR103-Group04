import pandas as pd
import json
import time
import os
import re
from pathlib import Path
from typing import List, Dict, Optional, Union
from google import genai
from google.genai import types

# ================= CONFIGURATION =================
API_KEY = os.getenv("GEMINI_API_KEY", "AIzaSyDUYuNsqyiuXN3PRycIDz0Y8rV1sCx0eIw") 
MODEL_NAME = "gemini-2.5-flash"

# 檔案路徑
CURRENT_DIR = Path(__file__).parent
MAPPING_DB_FILE = CURRENT_DIR / "unit_mapping_db.csv"

# ================= 轉換規則庫 =================
STANDARD_RULES: Dict[str, float] = {
    "kg": 1000, "公斤": 1000, 
    "g": 1, "克": 1, "公克": 1,
    "斤": 600, "台斤": 600, 
    "兩": 37.5, 
    "磅": 453.6, "lb": 453.6, 
    "oz": 28.35, "盎司": 28.35,
    "少許": 0.5, "適量": 1.0, "一小撮": 0.5, "把": 30.0,
}

SPECIFIC_RULES: Dict[tuple, float] = {
    ("蛋", "個"): 50.0, ("雞蛋", "個"): 50.0, ("全蛋", "個"): 50.0,
    ("蛋黃", "個"): 20.0, ("蛋白", "個"): 30.0, ("連殼雞蛋", "個"): 65.0, 
    ("B.雞蛋", "顆"): 50.0,
    ("白米", "杯"): 145.0, ("米", "杯"): 145.0, ("糯米粉", "杯"): 120.0,
    ("糖", "杯"): 200.0, ("砂糖", "杯"): 200.0, ("細砂糖", "杯"): 200.0,
    ("麵粉", "杯"): 120.0, ("低筋麵粉", "杯"): 120.0, 
    ("中筋麵粉", "杯"): 120.0, ("高筋麵粉", "杯"): 120.0,
    ("油", "杯"): 227.0, ("奶油", "大匙"): 13.0,
}

VOLUME_TO_ML: Dict[str, float] = {
    "大匙": 15, "tbsp": 15, "T": 15, "匙": 15,
    "小匙": 5, "tsp": 5, "t": 5, "茶匙": 5,
    "杯": 240, "cup": 240, "C": 240, "米杯": 180,
    "ml": 1, "cc": 1, "㏄": 1, "公升": 1000, "L": 1000,
    "又1/2杯": 360, "又1/2大匙": 22.5
}

class IngredientNormalizer:
    def __init__(self):
        self.client = genai.Client(api_key=API_KEY)
        self.mapping_db = self._load_mapping_db()
        
    def _load_mapping_db(self) -> pd.DataFrame:
        if MAPPING_DB_FILE.exists():
            print(f" 讀取 AI 知識庫：{MAPPING_DB_FILE}")
            try:
                return pd.read_csv(MAPPING_DB_FILE)
            except pd.errors.EmptyDataError:
                pass
        print(" 建立新的 AI 知識庫")
        return pd.DataFrame(columns=['Ingredient_Name', 'Unit', 'Grams_Per_Unit'])

    def _save_mapping_db(self):
        if not self.mapping_db.empty:
            self.mapping_db.to_csv(MAPPING_DB_FILE, index=False, encoding='utf-8-sig')
            # print(f" (已自動存檔，目前累積 {len(self.mapping_db)} 筆規則)") 

    def _clean_and_parse_json(self, text: str) -> Optional[Dict]:
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pattern = r'```json\s*(.*?)\s*```'
            match = re.search(pattern, text, re.DOTALL)
            if match:
                try:
                    return json.loads(match.group(1))
                except: pass
            
            clean_text = text.replace('```json', '').replace('```', '').strip()
            try:
                return json.loads(clean_text)
            except:
                print(f" JSON 解析失敗 (已略過此批次)")
                return None

    def ask_gemini(self, items_chunk: List[Dict]) -> Optional[Dict]:
        json_str = json.dumps(items_chunk, ensure_ascii=True)
        prompt = f"""
        You are a helper for normalizing recipe ingredient units to grams (g).
        Input Data (JSON): {json_str}
        Please output a JSON object with format: {{ "items": [ {{ "name": "...", "unit": "...", "g_per_unit": float }} ] }}
        Rules:
        1. Estimate weight in grams for 1 unit.
        2. For vague units, use approx values (0.5-2.0).
        3. If unknown, return 0.
        """
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = self.client.models.generate_content(
                    model=MODEL_NAME,
                    contents=prompt,
                    config=types.GenerateContentConfig(response_mime_type="application/json")
                )
                return self._clean_and_parse_json(response.text)
            except Exception as e:
                print(f" API Error ({attempt+1}/{max_retries}): {e}")
                time.sleep(2) # 重試前稍微等待
        return None

    def process_csv(self, input_csv_path: Path, output_csv_path: Path):
        print(f"\n 開始處理檔案：{input_csv_path}")
        try:
            df = pd.read_csv(input_csv_path)
        except FileNotFoundError:
            print(f" 找不到檔案：{input_csv_path}")
            return

        if 'Unit' not in df.columns or 'Ingredient_Name' not in df.columns:
            print(" CSV 欄位錯誤")
            return

        candidates = df[df['Unit'].notna()][['Ingredient_Name', 'Unit']].drop_duplicates()
        existing_db_keys = set(zip(self.mapping_db['Ingredient_Name'], self.mapping_db['Unit']))
        
        unknown_pairs = []
        for _, row in candidates.iterrows():
            name, unit = str(row['Ingredient_Name']), str(row['Unit'])
            
            if unit in STANDARD_RULES or unit in VOLUME_TO_ML: continue
            matched_specific = False
            for (r_n, r_u), _ in SPECIFIC_RULES.items():
                if r_n in name and r_u == unit: 
                    matched_specific = True; break
            if matched_specific: continue

            if (name, unit) in existing_db_keys: continue
            
            unknown_pairs.append({'name': name, 'unit': unit})
        
        print(f" 需透過 AI 估算的特殊組合：{len(unknown_pairs)} 筆 (已扣除重複與已知規則)")

        # 2. AI 批次處理 (Batch Processing)
        if unknown_pairs:
            # --- 修改：使用較大的批次 (60) 搭配較長的等待 (10s) 來應對免費版限制 ---
            BATCH_SIZE = 30
            print(f"🤖 開始呼叫 {MODEL_NAME} API (每 {BATCH_SIZE} 筆自動存檔)...")
            
            for i in range(0, len(unknown_pairs), BATCH_SIZE):
                batch = unknown_pairs[i:i+BATCH_SIZE]
                print(f"   處理進度: {i+1}/{len(unknown_pairs)}...")
                
                result = self.ask_gemini(batch)
                
                batch_new_records = []
                if result and 'items' in result:
                    for item in result['items']:
                        batch_new_records.append({
                            'Ingredient_Name': item.get('name', 'Unknown'),
                            'Unit': item.get('unit', 'Unknown'),
                            'Grams_Per_Unit': item.get('g_per_unit', 0)
                        })
                
                if batch_new_records:
                    new_df = pd.DataFrame(batch_new_records)
                    if not self.mapping_db.empty:
                         self.mapping_db = pd.concat([self.mapping_db, new_df], ignore_index=True)
                    else:
                         self.mapping_db = new_df
                    
                    self._save_mapping_db() 

                # --- 修改：每批次處理後等待 10 秒，降低 RPM ---
                print("   等待 10 秒 (避免 429 Rate Limit)...")
                time.sleep(10) 

        # 3. 最終資料轉換
        print(" 正在進行最終單位換算...")
        if not self.mapping_db.empty:
            ai_mapping = dict(zip(zip(self.mapping_db['Ingredient_Name'], self.mapping_db['Unit']), self.mapping_db['Grams_Per_Unit']))
        else:
            ai_mapping = {}

        def convert_row(row):
            w_str = str(row.get('Weight', 0))
            u = str(row.get('Unit', ''))
            name = str(row.get('Ingredient_Name', ''))
            
            try:
                if pd.isna(row.get('Weight')) or w_str.lower() in ['nan', 'null', '']:
                    w = 1.0 if (u in STANDARD_RULES or u in VOLUME_TO_ML) else 0
                elif '/' in w_str:
                    w = float(eval(w_str))
                else:
                    w = float(w_str)
            except:
                w = 0

            for (r_n, r_u), val in SPECIFIC_RULES.items():
                if r_n in name and r_u == u: return w * val

            if u in STANDARD_RULES: return w * STANDARD_RULES[u]
            
            ai_factor = ai_mapping.get((name, u))
            if ai_factor is not None: return w * ai_factor

            if u in VOLUME_TO_ML: return w * VOLUME_TO_ML[u]
            
            return None

        df['Normalized_Weight_g'] = df.apply(convert_row, axis=1)
        df.to_csv(output_csv_path, index=False, encoding='utf-8-sig')
        print(f" 全部完成！結果已儲存至：{output_csv_path}")

if __name__ == "__main__":
    project_root = Path(__file__).parents[2]
    input_csv = project_root / "src/kevin_ytower_crawler/ytower_csv_output/ytower_all_recipes.csv"
    output_csv = project_root / "src/kevin_ytower_crawler/ytower_csv_output/ytower_recipes_normalized.csv"
    
    if input_csv.exists():
        normalizer = IngredientNormalizer()
        normalizer.process_csv(input_csv, output_csv)
    else:
        print(f" 找不到輸入檔案：{input_csv}")