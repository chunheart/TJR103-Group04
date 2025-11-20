import pandas as pd
import json
import time
import os
import re
import argparse
from pathlib import Path
from typing import List, Dict, Optional, Union
from google import genai
from google.genai import types
from kafka import KafkaProducer

# ================= CONFIGURATION =================
# 請記得確認這裡的 API KEY
API_KEY = os.getenv("GEMINI_API_KEY", "請填入API KEY") 
MODEL_NAME = "gemini-2.5-flash"
KAFKA_SERVER = 'kafka:9092'  # Docker 內部使用的 Kafka 位址

# 檔案路徑
CURRENT_DIR = Path(__file__).parent
MAPPING_DB_FILE = CURRENT_DIR / "unit_mapping_db.csv"

# ================= 轉換規則庫  =================
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
    def __init__(self, kafka_server=KAFKA_SERVER):
        self.client = genai.Client(api_key=API_KEY)
        self.mapping_db = self._load_mapping_db()
        self.kafka_producer = self._init_kafka_producer(kafka_server)
        
    def _init_kafka_producer(self, server):
        """初始化 Kafka (新增功能)"""
        try:
            producer = KafkaProducer(
                bootstrap_servers=[server],
                value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode('utf-8')
            )
            print(f" Kafka 連線成功：{server}")
            return producer
        except Exception as e:
            print(f" Kafka 連線失敗 ({server}) - 將只執行運算不傳送: {e}")
            return None

    def _load_mapping_db(self) -> pd.DataFrame:
        """(完全保留你的原始邏輯)"""
        if MAPPING_DB_FILE.exists():
            try:
                return pd.read_csv(MAPPING_DB_FILE)
            except pd.errors.EmptyDataError:
                pass
        print(" 建立新的 AI 知識庫")
        return pd.DataFrame(columns=['Ingredient_Name', 'Unit', 'Grams_Per_Unit'])

    def _save_mapping_db(self):
        """(完全保留你的原始邏輯)"""
        if not self.mapping_db.empty:
            self.mapping_db.to_csv(MAPPING_DB_FILE, index=False, encoding='utf-8-sig')

    def _clean_and_parse_json(self, text: str) -> Optional[Dict]:
        """(完全保留你的原始邏輯)"""
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pattern = r'```json\s*(.*?)\s*```'
            match = re.search(pattern, text, re.DOTALL)
            if match:
                try: return json.loads(match.group(1))
                except: pass
            clean_text = text.replace('```json', '').replace('```', '').strip()
            try: return json.loads(clean_text)
            except: return None

    def ask_gemini(self, items_chunk: List[Dict]) -> Optional[Dict]:
        """(完全保留你的原始邏輯)"""
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
                time.sleep(2)
        return None

    def process_pipeline(self, input_csv_path: Path, kafka_topic: str, source_name: str):
        """
        主流程：讀取 CSV -> AI 補全 -> 計算重量 -> 寫入 Kafka
        """
        print(f"\n 開始處理 Pipeline")
        print(f" 來源檔案: {input_csv_path}")
        
        try:
            df = pd.read_csv(input_csv_path)
        except FileNotFoundError:
            print(f" 找不到檔案：{input_csv_path}")
            return

        if 'Unit' not in df.columns or 'Ingredient_Name' not in df.columns:
            print(" CSV 欄位錯誤")
            return

        # === 第一階段：AI 補全 (完全保留你的原始邏輯) ===
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
        
        print(f" 需透過 AI 估算的特殊組合：{len(unknown_pairs)} 筆")

        if unknown_pairs:
            BATCH_SIZE = 30
            print(f"🤖 開始呼叫 Gemini API (共 {len(unknown_pairs)} 筆)...")
            for i in range(0, len(unknown_pairs), BATCH_SIZE):
                batch = unknown_pairs[i:i+BATCH_SIZE]
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
                    self.mapping_db = pd.concat([self.mapping_db, new_df], ignore_index=True) if not self.mapping_db.empty else new_df
                    self._save_mapping_db()
                
                time.sleep(2) 

        # === 第二階段：計算與傳輸 (改為迴圈以支援 Kafka) ===
        print(" 正在進行單位換算並寫入 Kafka...")
        
        if not self.mapping_db.empty:
            ai_mapping = dict(zip(zip(self.mapping_db['Ingredient_Name'], self.mapping_db['Unit']), self.mapping_db['Grams_Per_Unit']))
        else:
            ai_mapping = {}

        count = 0
        for _, row in df.iterrows():
            # --- [核心邏輯開始] ---
            w_str = str(row.get('Weight', 0))
            u = str(row.get('Unit', ''))
            name = str(row.get('Ingredient_Name', ''))
            
            try:
                if pd.isna(row.get('Weight')) or w_str.lower() in ['nan', 'null', '']:
                    w = 1.0 if (u in STANDARD_RULES or u in VOLUME_TO_ML) else 0
                elif '/' in w_str: w = float(eval(w_str))
                else: w = float(w_str)
            except: w = 0

            normalized_g = 0.0
            found = False
            
            # 優先順序 1: 特殊規則 (Specific Rules)
            for (r_n, r_u), val in SPECIFIC_RULES.items():
                if r_n in name and r_u == u: 
                    normalized_g = w * val
                    found = True
                    break
            
            if not found:
                # 優先順序 2: 標準單位 (Standard Rules)
                if u in STANDARD_RULES:
                    normalized_g = w * STANDARD_RULES[u]
                # 優先順序 3: AI 知識庫 (AI Mapping)
                elif (name, u) in ai_mapping:
                    normalized_g = w * ai_mapping[(name, u)]
                # 優先順序 4: 容積單位 (Volume Rules)
                elif u in VOLUME_TO_ML:
                    normalized_g = w * VOLUME_TO_ML[u]
                else:
                    normalized_g = None 

            # 準備寫入資料
            data_row = row.to_dict()
            data_row['Normalized_Weight_g'] = normalized_g
            data_row['data_source'] = source_name # 新增：來源標記

            # 寫入 Kafka
            if self.kafka_producer:
                self.kafka_producer.send(kafka_topic, value=data_row)
                count += 1
                if count % 50 == 0:
                    print(f" 已傳送 {count} 筆...")

        if self.kafka_producer:
            self.kafka_producer.flush()
            self.kafka_producer.close()
        
        print(f" 全部完成！共寫入 {count} 筆資料到 Topic: {kafka_topic}")

if __name__ == "__main__":
    # 接收 Airflow 傳來的參數
    parser = argparse.ArgumentParser(description='通用食材正規化工具')
    parser.add_argument('--input', required=True, help='輸入的 CSV 檔案路徑')
    parser.add_argument('--topic', required=True, help='要寫入的 Kafka Topic')
    parser.add_argument('--source', required=True, help='資料來源標記 (如: ytower, icook)')
    
    args = parser.parse_args()
    
    input_path = Path(args.input)
    if input_path.exists():
        normalizer = IngredientNormalizer()
        normalizer.process_pipeline(input_path, args.topic, args.source)
    else:
        print(f" 錯誤：找不到輸入檔案 {input_path}")