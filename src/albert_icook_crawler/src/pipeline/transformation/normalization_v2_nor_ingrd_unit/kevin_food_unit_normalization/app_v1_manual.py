import pandas as pd
import json
import time
import os
import re

from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional
from google import genai
from google.genai import types
from dotenv import load_dotenv
from zoneinfo import ZoneInfo 

# ================= CONFIGURATION =================
# Suggestion: Use dynamic or relative paths for better portability
ROOT_DIR = Path(__file__).resolve().parents[5] # Root directory structure assumption
ENV_PATH = Path(__file__).resolve().parents[5] / "src" / "utils" / ".env"
load_dotenv(ENV_PATH)

API_KEY = os.getenv("API_KEY") 
# Raise error if API Key is missing
if not API_KEY:
    raise ValueError("API_KEY not detected. Please check your .env path.")

MODEL_NAME = "gemini-2.0-flash" 

TZ = ZoneInfo("Asia/Taipei")
MANUAL_DATE = "2025-11-12" 

MAPPING_DB_FILE = ROOT_DIR / "data" / "db_unit_normalization" / "unit_normalization_db.csv"

# ================= CONVERSION RULES LIBRARY =================
# Note: Keys remain in Chinese to match the input CSV data
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
    "ml": 1, "毫升": 1, "cc": 1, "㏄": 1, "公升": 1000, "L": 1000,
}

class IngredientNormalizer:
    def __init__(self):
        self.client = genai.Client(api_key=API_KEY)
        self.mapping_db = self._load_mapping_db()
        
    def _load_mapping_db(self) -> pd.DataFrame:
        if MAPPING_DB_FILE.exists():
            print(f" Reading AI knowledge base: {MAPPING_DB_FILE}")
            try:
                return pd.read_csv(MAPPING_DB_FILE)
            except pd.errors.EmptyDataError:
                pass
        print(" Creating new AI knowledge base")
        return pd.DataFrame(columns=['ingredients', 'unit_name', 'grams_per_unit'])

    def _save_mapping_db(self):
        # Ensure directory exists
        MAPPING_DB_FILE.parent.mkdir(parents=True, exist_ok=True)
        if not self.mapping_db.empty:
            self.mapping_db.to_csv(MAPPING_DB_FILE, index=False, encoding='utf-8-sig')

    def _clean_and_parse_json(self, text: str) -> Optional[Dict]:
        """Enhanced JSON parsing, handles Markdown Code Blocks."""
        text = text.strip()
        # Remove Markdown tags
        if text.startswith("```"):
            text = re.sub(r"^```(json)?|```$", "", text, flags=re.MULTILINE).strip()
        
        try:
            return json.loads(text)
        except json.JSONDecodeError as e:
            print(f" JSON parsing failed: {e}")
            print(f" Original response text: {text[:100]}...") # Print first 100 chars to avoid clutter
            return None

    def ask_gemini(self, items_chunk: List[Dict]) -> Optional[Dict]:
        json_str = json.dumps(items_chunk, ensure_ascii=False)
        
        # === Optimized Prompt (CoT + RaR) ===
        prompt = f"""
        # Role
        You are an expert Culinary Data Scientist specializing in ingredient unit conversion.
        
        # Task
        Convert the following ingredient units into **Grams (g)** for exactly **ONE UNIT**.
        
        # Input Data
        {json_str}

        # Chain of Thought & Rules (Re-read Carefully)
        Please follow this thinking process for EACH item:
        1. **Identify**: What is the ingredient? (Solid, Liquid, Vegetable, Meat?)
        2. **Analyze Unit**: Is the unit a volume (cup/spoon), a count (piece), or vague (pinch)?
        3. **Recall Standards**:
           - **Volume**: 1 Tbsp ~ 15g (water), but flour is lighter (~8g), salt is heavier (~18g).
           - **Count (Must differentiate sizes)**: 
             - Small (Garlic clove, Date) ~ 5g
             - Medium (Egg, Scallop, Lemon) ~ 30-50g
             - Large (Apple, Onion, Potato) ~ 150-200g
             - Heavy (Cabbage, Pumpkin) ~ 500g+
           - **Stalks/Strips**: 
             - Chili/Scallion ~ 10g
             - Cucumber/Carrot ~ 100-150g
        4. **Reasoning**: Write a short explanation of your estimation.
        5. **Finalize**: Output the float value. NO NULLS allowed. Default to 100.0 if completely unknown.

        # Output Schema (Strict JSON)
        Please output a JSON object exactly like this:
        {{
            "items": [
                {{
                    "name": "Ingredient Name",
                    "unit": "Unit Name",
                    "reasoning": "Short explanation (e.g., 'Medium scallop meat typically weighs 30g')",
                    "normalized_weight": float
                }}
            ]
        }}
        """

        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = self.client.models.generate_content(
                    model=MODEL_NAME,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                        temperature=0.5 # Lower temperature for stable answers
                    )
                )
                return self._clean_and_parse_json(response.text)
            except Exception as e:
                print(f" API Error ({attempt+1}/{max_retries}): {e}")
                time.sleep(3)
        return None

    def process_csv(self, input_csv: Path, output_csv_path: Path) -> pd.DataFrame:
        print(f"\n Start processing: {input_csv}")
        try:
            df = pd.read_csv(input_csv)
        except Exception as e:
            print(f" File not found: {input_csv}, {e}")
            return

        # --- VALIDATION: Check Columns ---
        # Normalize column names to avoid case sensitivity issues (Weight vs weight)
        df_cols_lower = {c.lower(): c for c in df.columns}
        
        required_cols = ['unit_name', 'ingredients']
        for rc in required_cols:
            if rc not in df_cols_lower:
                print(f" Error: Missing required column '{rc}' in CSV.")
                return

        # Identify the actual column name for 'Weight'
        weight_col = df_cols_lower.get('weight') # Tries to find 'weight' or 'Weight'
        if not weight_col:
            print(" Warning: 'Weight' column not found. Assuming weight = 1.0 for all items.")
        
        # --- PRE-PROCESSING ---
        # Clean strings to ensure matching works
        df['ingredients'] = df['ingredients'].astype(str).str.strip()
        df['unit_name'] = df['unit_name'].astype(str).str.strip()
        
        # 1. Identify candidates
        candidates = df[df['unit_name'].notna()][['ingredients', 'unit_name']].drop_duplicates()
        
        # Load DB and strip strings
        self.mapping_db['ingredients'] = self.mapping_db['ingredients'].astype(str).str.strip()
        self.mapping_db['unit_name'] = self.mapping_db['unit_name'].astype(str).str.strip()
        existing_db_keys = set(zip(self.mapping_db['ingredients'], self.mapping_db['unit_name']))
        
        unknown_pairs = []
        for _, row in candidates.iterrows():
            name, unit = row['ingredients'], row['unit_name']
            
            # Skip standards
            if unit in STANDARD_RULES or unit in VOLUME_TO_ML: continue
            
            # Skip specifics
            matched_specific = False
            for (r_n, r_u), _ in SPECIFIC_RULES.items():
                if r_n in name and r_u == unit: 
                    matched_specific = True; break
            if matched_specific: continue

            # Skip existing
            if (name, unit) in existing_db_keys: continue
            
            unknown_pairs.append({'name': name, 'unit': unit})
        
        print(f" Combinations requiring AI estimation: {len(unknown_pairs)}")

        # 2. AI Batch Processing
        if unknown_pairs:
            BATCH_SIZE = 20 
            print(f" Starting API call to {MODEL_NAME}...")
            for i in range(0, len(unknown_pairs), BATCH_SIZE):
                batch = unknown_pairs[i:i+BATCH_SIZE]
                print(f"   Processing batch: {i+1} - {min(i+BATCH_SIZE, len(unknown_pairs))} / {len(unknown_pairs)}...")
                
                result = self.ask_gemini(batch)
                
                batch_new_records = []
                if result and 'items' in result:
                    for item in result['items']:
                        w = item.get('normalized_weight')
                        if w is None: w = item.get('g_per_unit', 0)
                        
                        batch_new_records.append({
                            'ingredients': item.get('name', 'Unknown').strip(),
                            'unit_name': item.get('unit', 'Unknown').strip(),
                            'grams_per_unit': w
                        })
                
                if batch_new_records:
                    new_df = pd.DataFrame(batch_new_records)
                    self.mapping_db = pd.concat([self.mapping_db, new_df], ignore_index=True)
                    self._save_mapping_db() 
                time.sleep(5) 

        # 3. Final Unit Conversion (CRITICAL FIX)
        print(" Finalizing unit conversion...")
        
        # Re-strip DB just in case
        self.mapping_db['ingredients'] = self.mapping_db['ingredients'].astype(str).str.strip()
        self.mapping_db['unit_name'] = self.mapping_db['unit_name'].astype(str).str.strip()
        
        # Build Lookup Map
        ai_mapping = dict(zip(
            zip(self.mapping_db['ingredients'], self.mapping_db['unit_name']), 
            self.mapping_db['grams_per_unit']
        ))

        def convert_row(row):
            # --- 1. Get Weight (Robust Logic) ---
            w = 0.0
            raw_w = row.get(weight_col) if weight_col else None
            
            try:
                w_str = str(raw_w).strip().lower()
                # Check for empty/nan/null
                if pd.isna(raw_w) or w_str in ['nan', 'null', '', 'none']:
                    # Implicit Quantity Rule: If weight is missing but unit exists, assume 1.0
                    w = 1.0
                elif '/' in w_str:
                    w = float(eval(w_str))
                else:
                    w = float(w_str)
            except Exception:
                # If parsing fails entirely, fallback to 0.0 (or 1.0 if you prefer)
                w = 0.0

            u = str(row.get('unit_name', '')).strip()
            name = str(row.get('ingredients', '')).strip()
            
            # Factor: How many grams per 1 Unit?
            factor = None

            # Check Rule 1: Specific Rules
            for (r_n, r_u), val in SPECIFIC_RULES.items():
                if r_n in name and r_u == u: 
                    factor = val
                    break

            # Check Rule 2: Standard Mass Units
            if factor is None and u in STANDARD_RULES:
                factor = STANDARD_RULES[u]

            # Check Rule 3: AI DB Mapping
            if factor is None:
                factor = ai_mapping.get((name, u))

            # Check Rule 4: Volume Units
            if factor is None and u in VOLUME_TO_ML:
                factor = VOLUME_TO_ML[u]
            
            # --- Calculation ---
            if factor is not None:
                # If weight was missing (Implicit 1.0) or parsed (e.g. 100), multiply by factor
                return w * factor
            
            # If no factor found, we cannot convert
            return None

        # Apply the conversion to the Input DataFrame
        df['Normalized_Weight_g'] = df.apply(convert_row, axis=1)
        
        # Check coverage
        filled_count = df['Normalized_Weight_g'].notna().sum()
        total_count = len(df)
        print(f" Conversion Coverage: {filled_count}/{total_count} rows filled.")

        output_csv_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_csv_path, index=False, encoding='utf-8-sig')
        print(f" All done! Results saved to: {output_csv_path}")

def normalize_unit():
    # Adjust path structure as needed
    input_csv = ROOT_DIR / f"data/db_ingredients/icook_recipe_{MANUAL_DATE}_recipe_ingredients.csv"
    output_csv = ROOT_DIR / f"data/db_ingredients/icook_recipe_{MANUAL_DATE}_recipe_ingredients_unitN.csv"

    if input_csv.exists():
        normalizer = IngredientNormalizer()
        normalizer.process_csv(input_csv, output_csv)
    else:
        print(f" Input file not found: {input_csv}")

if __name__ == "__main__":
    normalize_unit()
