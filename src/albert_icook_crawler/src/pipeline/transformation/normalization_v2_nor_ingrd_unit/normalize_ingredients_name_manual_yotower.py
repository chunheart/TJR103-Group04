import glob
import math
import json
import google.generativeai as genai
import pandas as pd
import os, sys
import time
import re
import albert_icook_crawler.src.utils.mongodb_connection as mondb
from albert_icook_crawler.src.utils.get_logger import get_logger
from google.ai.generativelanguage_v1beta.types import content
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from zoneinfo import ZoneInfo # only above python 3.9

ROOT_DIR = Path(__file__).resolve().parents[4] # Root: /opt/airflow/src/albert_icook_crawler
ENV_PATH = Path(ROOT_DIR/ "src" / "utils"/ ".env")
load_dotenv(ENV_PATH)

API_KEY = os.getenv("API_KEY")
MODEL_NAME = "gemini-2.5-flash"

FILENAME = os.path.basename(__file__).split(".")[0]
LOG_FILE_DIR = ROOT_DIR / "src" / "logs" / f"logs={datetime.today().date()}"
LOG_FILE_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE_PATH = LOG_FILE_DIR /  f"{FILENAME}_{datetime.today().date()}.log"

MANUAL_DATE = "2025-11-12"

# CSV_FILE_PATH = ROOT_DIR / "data" / "db_ingredients" / f"icook_recipe_{MANUAL_DATE}_recipe_ingredients.csv"
CSV_FILE_PATH = "/opt/airflow/src/albert_icook_crawler/data/yotower_ori_ingredient/all_ingredient_names_cleaned_ytower.csv"
COLLECTION = "ingredient_normalize"

REFERNCE_FILE_DIR = ROOT_DIR / "data" / "db_ingredient_normalize"
REFERNCE_FILE_DIR.mkdir(parents=True, exist_ok=True)
REFERNCE_FILE_PATH =  REFERNCE_FILE_DIR / "icook_recipe_ingredient_normalize.csv"

logger = get_logger(log_file_path=LOG_FILE_PATH, logger_name=FILENAME)

def remove_parentheses(s:str) -> str:
    if_parentheses = r"[(){}\"\[\]（）]"
    pattern = r"\((.*?)\)|\[(.*?)\]|\{(.*?)\}|\"(.*?)\"|（(.*?)）"
    if re.search(if_parentheses, s):
        return re.sub(pattern, "", s)
    return s

def fetch_gemini_normalization(unknown_ingredients):
    """
    Sends a list of unknown ingredients to Google Gemini API to get standardized names.
    Uses JSON mode to ensure valid output.
    
    Args:
        unknown_ingredients (list): List of strings (raw ingredient names).
        
    Returns:
        dict: A dictionary mapping { "raw_name": "normalized_name" }
    """
    if not unknown_ingredients:
        return {}

    print(f"Sending {len(unknown_ingredients)} items to Gemini API...")
    
    # Configure the API
    genai.configure(api_key=API_KEY)
    
    # Configuration for JSON output (Best practice for data pipelines)
    generation_config = {
        "temperature": 0.8,
        "top_p": 0.95,
        "top_k": 64,
        "max_output_tokens": 8192,
        "response_mime_type": "application/json",
    }

    model = genai.GenerativeModel(
        model_name=MODEL_NAME,
        generation_config=generation_config,
    )

    # Construct the prompt
    # We provide strict instructions on how to handle food names
    # prompt = f"""
    # You are an expert data engineer specializing in food ingredients.
    # Your task is to normalize the following list of raw ingredient names into their standard, simplified forms (Traditional Chinese).
    
    # ### Rules:
    # 1. **Remove Adjectives**: Ignore quantity, temperature, cutting styles, or brands (e.g., "Hot Water" -> "Water", "Diced Pork" -> "Pork").
    # 2. **Standardize**: Use the most common, generic name for the ingredient (e.g., "High-gluten flour" -> "麵粉").
    # 3. **Cooking Oils**: Generalize ALL types of edible oils (e.g., Olive oil, Sesame oil, Canola oil, Butter) to "食用油".
    # 4. **Output Format**: Return a JSON object {{ "raw_name": "normalized_name" }}.
    
    # ### Input List:
    # {json.dumps(unknown_ingredients, ensure_ascii=False)}
    # """  
    prompt = f"""
    You are an expert Data Engineer and Sustainability Analyst specializing in food supply chains.
    Your task is to normalize a list of raw ingredient names into their **Single Primary Raw Material** (Traditional Chinese), based on **Carbon Footprint Priority**.

    ### 1. The Core Rule (Carbon Hierarchy)
    When a name implies multiple ingredients (e.g., "Dumplings"), you must identify the components and select the ONE with the **Highest Carbon Footprint** based on this hierarchy (High to Low):
    1. **Red Meat** (Beef, Lamb) --- [HIGHEST PRIORITY]
    2. **White Meat/Seafood** (Pork, Chicken, Fish)
    3. **Dairy & Eggs** (Milk, Cheese, Butter)
    4. **Oils** (All types map to "食用油")
    5. **Grains & Nuts** (Rice, Wheat, Beans)
    6. **Vegetables, Fruits, Spices** --- [LOWEST PRIORITY]

    ### 2. Few-Shot Learning (Examples)
    Learn from these logic patterns:
    - Input: "韭菜水餃" (Leek + Pork + Flour). Carbon: Pork > Flour > Leek. -> Output: "豬肉"
    - Input: "拿鐵咖啡" (Milk + Coffee). Carbon: Milk > Coffee. -> Output: "牛奶"
    - Input: "特級初榨橄欖油" (Oil). Rule: Generalize. -> Output: "食用油"
    - Input: "黑醋栗" (Blackcurrant). It is a raw fruit/berry. -> Output: "黑醋栗"
    - Input: "酥炸雞腿" (Chicken + Oil + Flour). Carbon: Chicken > Oil. -> Output: "雞肉"
    - Input: "蘋果醋" (Apple + Vinegar). Carbon: Apple (Low). -> Output: "蘋果"

    ### 3. Step-by-Step Execution (Chain of Thought)
    For each item in the input list:
    1. **Analyze**: Identify all potential raw ingredients in the name.
    2. **Filter**: Remove adjectives (hot, diced, spicy) and brands.
    3. **Compare**: Apply the [Carbon Hierarchy] to find the "heaviest" ingredient.
    4. **Normalize**: Output only that specific raw material name in Traditional Chinese.

    ### Input List:
    {json.dumps(unknown_ingredients, ensure_ascii=False)}

    ### Output Format:
    Return ONLY a valid JSON object. Do not include markdown formatting (like ```json).
    {{
        "raw_name_1": "normalized_result_1",
        "raw_name_2": "normalized_result_2"
    }}
    """

    try:
        # specific call to generate content
        response = model.generate_content(prompt)
        
        # Parse the JSON response
        # Since we enforced response_mime_type="application/json", text should be valid JSON.
        normalized_map = json.loads(response.text)
        return normalized_map

    except Exception as e:
        logger.error(f" Gemini API Error: {e}")
        return {}

def process_ingredients_pipeline(
        dt:pd.DataFrame, 
        ref_path:Path,
        output_file:Path,
    ):
    """
    Main pipeline: Load -> Local Lookup -> Gemini API Analysis -> Save.
    
    Strategy:
    1. Use 't_ingredient_name' (hint) to help find the match locally.
    2. If not found, batch raw names and hints to Gemini API.
    3. Save the mapping: 'ingredients' (raw) -> 'normalized_name'.
    """
    # ==========================================
    # 1. Load Data
    # ==========================================
    logger.info("Loading datasets...")
    df_sample = dt
    df_ref = pd.read_csv(ref_path, dtype=str)

    # Clean column names
    df_sample.columns = [c.strip() for c in df_sample.columns]
    df_ref.columns = [c.strip() for c in df_ref.columns]

    # ==========================================
    # 2. Local Lookup (Fast Layer)
    # ==========================================
    # Create dictionary from existing DB
    ref_unique = df_ref.drop_duplicates(subset=['ori_ingredient_name'])
    local_map = dict(zip(ref_unique['ori_ingredient_name'], ref_unique['nor_ingredient_name']))
    
    processing_queue = []
    unknown_buffer = [] # List to store items for Gemini
    
    # Iterate rows
    for idx, row in df_sample.iterrows():
        ori = str(row.get('ingredients', '')).strip()       # Raw input (Key for DB)
        hint = str(row.get('t_ingredient_name', '')).strip() # Hint (Tool for Search)
        
        # Priority 1: Check Local DB with Original Name
        if ori in local_map:
            processing_queue.append({'ori': ori, 'norm': local_map[ori], 'source': 'local'})
            
        # Priority 2: Check Local DB with Hint Name
        elif hint in local_map:
            processing_queue.append({'ori': ori, 'norm': local_map[hint], 'source': 'local_hint'})
            
        else:
            # Priority 3: Not found locally -> Add to Gemini buffer
            # We send the 'hint' to Gemini if available, as it's cleaner and saves tokens/confusion
            target_to_ask = hint if hint else ori
            
            if target_to_ask: # Avoid empty strings
                unknown_buffer.append(target_to_ask)
                processing_queue.append({'ori': ori, 'ask_val': target_to_ask, 'source': 'api_pending'})
            else:
                # Both are empty? Just mark as empty
                processing_queue.append({'ori': ori, 'norm': '', 'source': 'empty'})

    # ==========================================
    # 3. Gemini API Analysis (Smart Layer)
    # ==========================================
    unique_unknowns = list(set(unknown_buffer)) # Deduplicate to save costs
    ai_generated_map = {}

    if unique_unknowns:
        total_unknowns = len(unique_unknowns)
        BATCH_SIZE = 30 # Process 30 ingredients at a time
        total_batches = math.ceil(total_unknowns / BATCH_SIZE)
        
        logger.info(f"Analyzing {total_unknowns} unique unknown items in {total_batches} batches...")

        for i in range(0, total_unknowns, BATCH_SIZE):
            # Slice the batch
            current_batch = unique_unknowns[i : i + BATCH_SIZE]
            batch_num = (i // BATCH_SIZE) + 1
            
            logger.info(f"Processing Batch {batch_num}/{total_batches} ({len(current_batch)} items)...")
            
            ### Retry system starts ###
            max_retries = 2
            batch_result = None
            for attempt in range(max_retries):
                try:
                    # Call API
                    batch_result = fetch_gemini_normalization(current_batch)
                    break # If succeeding, will stop this retry loop
                
                except Exception as e:
                    logger.warning(f"Batch {batch_num} failed (Attempt {attempt+1}/{max_retries}). Error: {e}")
                    if attempt < max_retries - 1:
                        time.sleep(10) 
                    else:
                        # If error times reach to max retries, this program will cease accordingly
                        logger.critical("Max retries reached. Stopping pipeline to prevent data corruption.")
                        sys.exit(1)
            ### Retry system ends ###


            # Update the main map
            if batch_result:
                ai_generated_map.update(batch_result)
                logger.info(f"Batch {batch_num} success. Mapped {len(batch_result)} items.")
            else:
                logger.warning(f"Batch {batch_num} failed or returned empty.")
            
            # Sleep to respect Rate Limit (RPM)
            time.sleep(5)

        logger.info(f"Gemini successfully mapped a total of {len(ai_generated_map)} items.")
    else:
        logger.info("All items matched locally. No API call needed.")

    # ==========================================
    # 4. Merge & Update
    # ==========================================
    final_normalized_column = []
    new_db_entries = []
    current_time = datetime.now(tz=ZoneInfo("Asia/Taipei")).strftime("%Y-%m-%d %H:%M:%S")
    
    for item in processing_queue:
        norm_name = None
        
        # Retrieve result based on source
        if item['source'] in ['local', 'local_hint']:
            norm_name = item['norm']
        elif item['source'] == 'api_pending':
            ask_val = item['ask_val']
            # Try to get from AI map, fallback to original if AI missed it
            norm_name = ai_generated_map.get(ask_val, item['ori'])
        elif item['source'] == 'empty':
            norm_name = ''

        final_normalized_column.append(norm_name)

        # DB Update Logic:
        # Only add if we have a valid name AND the Raw Original (ori) is not in local map
        if norm_name and item['ori'] and item['ori'] not in local_map:
            # Prevent duplicates in the current batch
            is_duplicate_in_batch = any(d['ori_ingredient_name'] == item['ori'] for d in new_db_entries)
            
            if not is_duplicate_in_batch:
                new_entry = {
                    'ori_ingredient_name': item['ori'], # Raw data
                    'nor_ingredient_name': norm_name,   # Normalized data
                    'ins_timestamp': current_time,
                    'upd_timestamp': current_time,
                    'status': 'processed'
                }
                new_db_entries.append(new_entry)

    # ==========================================
    # 5. Save Results
    # ==========================================
    # Update Sample File
    df_sample['normalized_name'] = final_normalized_column
    
    # Update Reference DB
    if new_db_entries:
        df_new = pd.DataFrame(new_db_entries)
        df_ref_updated = pd.concat([df_ref, df_new], ignore_index=True)
        print(f"Added {len(new_db_entries)} new rules to the database.")
    else:
        df_ref_updated = df_ref
        print("Database remains unchanged.")

    # Save to disk
    output_norm_ingred_name_file = output_file
    output_ref_file = REFERNCE_FILE_PATH
    
    df_sample.to_csv(output_norm_ingred_name_file, index=False, encoding='utf-8-sig')
    df_ref_updated.to_csv(output_ref_file, index=False, encoding='utf-8-sig')
    
    file_index = str(output_file).split("/")[-1].split("_")[-1].split(".")[0]
    target_file = str(output_file).split("/")[-1]
    move_dir = ROOT_DIR / "data" / "yotower_ori_ingredient" / "chunks" / f"prcocessed={file_index}_{datetime.today().date()}"
    move_dir.mkdir(exist_ok=True, parents=True)
    moved_file_path = move_dir / target_file

    logger.info("Processing Complete!")
    logger.info(f"1. Processed file: {output_norm_ingred_name_file}")
    logger.info(f"2. Updated Ref Database:  {output_ref_file}")
    os.rename(
        src=output_file,
        dst=moved_file_path,
    )


def normalize_ingrd_name():
    """
    Retrieve icook recipe CSV file from the determined field to MongoDB, collection is ingredient_normalize
    The field is listed below:
    1) ori_ingredient_id    INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    2) ori_ingredient_name  varchar(255),
    3) nor_ingredient_name  varchar(255), 
    4) ins_time` DATETIME DEFAULT CURRENT_TIMESTAMP
    5) upd_time` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    6) normalize_status ENUM('pending','running','done','error','manual') NOT NULL DEFAULT 'pending',
    
    ### Algorithm:
    - establish logging V
    - MySQL connection V
    - db collection V
    - collection connection V
    - find today's icook recipe CSV file V
    - convert it into pandas dataframe V
    - select determined field V
    - convert str to required data type V
    - collect transformed data V
    - Save it into CSV file
    - insert transformed data to targeted collection
    - MySQL disconnection
    """

    logger.info(f"Starting execution of {FILENAME}")
    file_pattern = ROOT_DIR/ "data" / "yotower_ori_ingredient" / "chunks" / "*.csv"
    csv_file_list = glob.glob(str(file_pattern))
    
    for csv_file in csv_file_list:
        try:
            with open(file=csv_file, mode="r", encoding="utf-8-sig") as csv:
                df = pd.read_csv(csv)
                logger.info(f"Opened CSV file: {str(CSV_FILE_PATH).split("/")[-1].strip()}")

            # mask = [
            #     "ingredients",
            # ]

            # # Filter field, ingredients
            # ingredient_norm_df = raw_df[mask]

            # # Add int_time, upd_time, status
            # logger.info(f"Adding field \"int_time\", \"upd_time\", \"status\" ...")

            # int_time, upd_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S"), datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            # status = "pending"
            # ingredient_norm_df["ins_timestamp"] = int_time
            # ingredient_norm_df["upd_timestamp"] = upd_time
            # ingredient_norm_df["status"] = status
            
            # logger.info(f"Added field \"int_time\", \"upd_time\", \"status\" ...")

            # Remove parentheses of values of field ingredients
            logger.info(f"Removing parenthese")
            df["t_ingredient_name"] = df["ingredients"].apply(remove_parentheses)
            logger.info(f"Removed parenthese")
            

            ### Call Gemini to help normalize ingredient names
            """
            AI will get the dataframe and it has to categorise the original ingredient names to get the standarized name.
            I have the data that contain normalized ingredient names saved in CSV file, icook_recipe_ingredient_normalize.csv.
            If the original ingredient names that has been saved in the csv file, AI will neglect them and find others to get the standarised name save it.
            When all data has been through, AI needs to add a column to list all relative standarised name based on the origial names to assure all data has its relatable standarised name. 
            """
            # File configurations
            
            input_ref_file = REFERNCE_FILE_PATH 
            
            output_file = Path(csv_file)

            # Check for file existence
            if os.path.exists(input_ref_file):
                process_ingredients_pipeline(
                    df,
                    REFERNCE_FILE_PATH,
                    output_file,
                )
            else:
                logger.error(" Error: Input files not found. Please verify filenames.")

        except Exception as e:
            logger.error(f"{e}")


    logger.info(f"Finished execution of {FILENAME}")


if __name__ == "__main__":
    normalize_ingrd_name()
