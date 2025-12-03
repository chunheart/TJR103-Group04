import google.generativeai as genai
import json
from typing import Tuple, Dict, Optional
import pandas as pd
import os, sys
import re
import albert_icook_crawler.src.utils.mongodb_connection as mondb
from albert_icook_crawler.src.utils.get_logger import get_logger
from albert_icook_crawler.src.pipeline.transformation.get_num import get_num_field_quantity as num
from albert_icook_crawler.src.pipeline.transformation.get_unit import get_unit_field_quantity as unit
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from zoneinfo import ZoneInfo

ROOT_DIR = Path(__file__).resolve().parents[4] # Root: /opt/airflow/src/albert_icook_crawler
ENV_PATH = Path(ROOT_DIR/ "src" / "utils"/ ".env")
load_dotenv(ENV_PATH)

FILENAME = os.path.basename(__file__).split(".")[0]
LOG_FILE_DIR = ROOT_DIR / "src" / "logs" / f"logs={datetime.today().date()}"
LOG_FILE_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE_PATH = LOG_FILE_DIR /  f"{FILENAME}_{datetime.today().date()}.log"

MANUAL_DATE = "2025-11-12"
CSV_FILE_PATH = ROOT_DIR / "data" / "daily" / f"Created_on_{MANUAL_DATE}" / f"icook_recipe_{MANUAL_DATE}.csv"
COLLECTION = "recipe_ingredients"

DATA_ROOT_DIR = Path(__file__).resolve().parents[4]
SAVED_FILE_DIR = DATA_ROOT_DIR / "data" / "db_ingredients"
SAVED_FILE_DIR.mkdir(parents=True, exist_ok=True)
SAVED_FILE_PATH =  SAVED_FILE_DIR / f"icook_recipe_{MANUAL_DATE}_{COLLECTION}.csv"

TZ = ZoneInfo("Asia/Taipei")

# Gemini Configuration
API_KEY = os.getenv("API_KEY")
genai.configure(api_key=API_KEY)
model = genai.GenerativeModel('gemini-2.5-flash')

logger = get_logger(log_file_path=LOG_FILE_PATH, logger_name=FILENAME)


def remove_parentheses(s:str) -> str:
    if_parentheses = r"[(){}\"\[\]（）]"
    pattern = r"\((.*?)\)|\[(.*?)\]|\{(.*?)\}|\"(.*?)\"|（(.*?)）"
    if re.search(if_parentheses, s):
        return re.sub(pattern, "", s)
    return s

def unwind(df:pd.DataFrame, col_name:str)-> pd.DataFrame | None:
    mix_separator_pattern = r"[,，/|]+"
    df_exploded = (
        df.assign(ingredients=df[col_name].str.split(pat=mix_separator_pattern, regex=True))
        .explode(col_name)
        .assign(ingredients=lambda x: x[col_name].str.strip())
    )
    return df_exploded

def unit_convertion(s:str)-> str:
    if s == "克" or s == "公克":
        return "g"
    elif s == "公斤":
        return "kg"
    elif s == "CC" or s == "毫升" or s == "ml":
        return "cc"
    else:
        return s


def extract_metric_priority(quantity_text: str) -> Tuple[Optional[float], Optional[str]]:
    """
    Extracts metric weight (g, kg) from the quantity string with high priority.
    This is crucial for accurate Carbon Footprint calculations.

    Logic:
        If the string is "2 units / 200g", it ignores "2 units" and returns (200.0, 'g').
        If the string is "1kg", it returns (1000.0, 'g') or (1.0, 'kg').

    Args:
        quantity_text (str): The raw quantity string (e.g., "2 pieces/200g").

    Returns:
        Tuple[float, str]: (unit_number, unit_name) if found, otherwise (None, None).
    """
    if pd.isna(quantity_text):
        return None, None

    text = str(quantity_text).lower().strip()

    # Pattern to find numbers explicitly followed by 'g' or 'kg'
    # It handles decimals (e.g., 0.5g) and integers (200g)
    match_metric = re.search(r'(\d+(?:\.\d+)?)\s*(kg|g)', text)

    if match_metric:
        number = float(match_metric.group(1))
        unit = match_metric.group(2)
        return number, unit

    return None, None


def get_rows_needing_processing(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filters rows where 'unit_name' or 'unit_number' is missing.
    It first attempts to fix them using the Metric Priority Logic.
    Only rows that remain incomplete are returned for LLM processing.

    Args:
        df (pd.DataFrame): The dataframe containing recipe data.

    Returns:
        pd.DataFrame: A subset of the dataframe that still requires LLM inference.
    """
    logger.info("[Status] Pre-screening data for metric units...")

    # We work on a copy to avoid SettingWithCopy warnings on the original df temporarily
    # But since we want to update the original logic flow, we will identify indices.
    mask = df['unit_name'].isna() | df['unit_number'].isna()
    target_indices = df[mask].index

    still_missing_indices = []

    for idx in target_indices:
        qty_text = df.loc[idx, 'quantity']

        # Attempt local regex extraction first (Metric Priority)
        num, unit = extract_metric_priority(qty_text)

        if num is not None and unit is not None:
            # If regex found a metric unit, fill it immediately
            # This saves API costs and ensures precision for carbon calc
            if pd.isna(df.loc[idx, 'unit_number']):
                df.loc[idx, 'unit_number'] = num
            if pd.isna(df.loc[idx, 'unit_name']):
                df.loc[idx, 'unit_name'] = unit
        else:
            # If regex failed, add to list for LLM
            # Re-check if it is still missing (in case only one field was missing)
            if pd.isna(df.loc[idx, 'unit_name']) or pd.isna(df.loc[idx, 'unit_number']):
                still_missing_indices.append(idx)

    result_df = df.loc[still_missing_indices].copy()
    logger.info(f"[Status] Pre-processing complete. {len(result_df)} rows require LLM inference.")
    return result_df


def construct_hybrid_language_prompt(target_rows: pd.DataFrame) -> str:
    """
    Constructs the prompt in English for the LLM, emphasizing carbon footprint requirements.

    Args:
        target_rows (pd.DataFrame): The subset of data to process.

    Returns:
        str: The formatted prompt string.
    """
    prompt_text = """
    # Role
    You are an expert Recipe Data Scientist.
    Your task is to infer missing 'unit_number' and 'unit_name' from the 'Ingredient' and 'Quantity_Text'.

    # Core Objective
    We need to standardize units for Carbon Footprint calculation, but keep natural units readable in Chinese.
    One thing you must comply to is that there is no way to leave null in the field of 'unit_number' and 'unit_name'.
    You must fill an answer by analyzing the information given in row. 

    # Rules for unit_name Output (CRITICAL)
    1. **Metric unit_names (Keep English)**: 
       - If the unit_name is weight or volume, use: 'g', 'kg', 'ml', 'L'.
    
    2. **Non-Metric unit_names (Use Traditional Chinese)**:
       - For all other count-based or rough units, **YOU MUST USE TRADITIONAL CHINESE**.
       - Examples: 
         - 'piece' -> '顆' or '個' or '片' (depend on ingredient)
         - 'pack' -> '包' or '盒'
         - 'tablespoon' -> '大匙'
         - 'teaspoon' -> '小匙'
         - 'strip' -> '條'
         - 'bunch' -> '束'
         - 'cup' -> '杯'

    # Logic Priorities
    1. **Weight Priority**: If the text says "2 pieces / 200g", ignore pieces and output unit_name="g", unit_number=200.0.
    2. **Fuzzy Conversion**: 
       - "少許" (Few) -> unit_name="小匙", unit_number=0.25 (Estimate)
       - "適量" (Moderate) -> Estimate based on ingredient type.
       - Pure unit_number "6" (for Tomato) -> unit_name="顆", unit_number=6.0

    # Input Data
    """

    data_lines = []
    for idx, row in target_rows.iterrows():
        q_text = str(row['quantity']) if not pd.isna(row['quantity']) else "Empty"
        line = f"Index: {idx}, Ingredient: {row['ingredients']}, Quantity_Text: '{q_text}'"
        data_lines.append(line)

    prompt_text += "\n".join(data_lines)

    prompt_text += """

    # Output Format
    Return a strictly valid JSON object. No markdown.
    Example:
    {
        "46": {"unit_name": "g", "unit_number": 200.0},
        "47": {"unit_name": "條", "unit_number": 0.5},
        "48": {"unit_name": "大匙", "unit_number": 1.0}
    }
    """
    return prompt_text


def fetch_gemini_response(prompt: str) -> Dict:
    """
    Calls the Gemini API and parses the JSON response.

    Args:
        prompt (str): The constructed prompt.

    Returns:
        Dict: The parsed JSON dictionary. Returns empty dict on failure.
    """
    logger.info("[Status] Calling Gemini API...")
    try:
        response = model.generate_content(prompt)
        cleaned_text = response.text.replace("```json", "").replace("```", "").strip()
        result = json.loads(cleaned_text)
        logger.info("[Status] Response parsed successfully.")
        return result
    except Exception as e:
        logger.error(f"[Error] API call or parsing failed: {e}")
        return {}


def update_dataset(df: pd.DataFrame, predictions: Dict) -> Tuple[pd.DataFrame, int]:
    """
    Updates the DataFrame with the predictions from LLM.

    Args:
        df (pd.DataFrame): The original dataframe.
        predictions (Dict): The dictionary of inferred values.

    Returns:
        Tuple[pd.DataFrame, int]: Updated dataframe and count of updated rows.
    """
    df_updated = df.copy(deep=True)
    success_count = 0

    logger.info("[Status] Updating dataset...")

    for idx_str, data in predictions.items():
        try:
            idx = int(idx_str)
            if idx in df_updated.index:
                # Only overwrite if the value is still NaN
                # (Although our logic ensures we only asked for NaNs, double-check is safe)
                if pd.isna(df_updated.loc[idx, 'unit_name']):
                    df_updated.loc[idx, 'unit_name'] = data.get('unit_name')
                if pd.isna(df_updated.loc[idx, 'unit_number']):
                    df_updated.loc[idx, 'unit_number'] = data.get('unit_number')
                success_count += 1
        except Exception as e:
            logger.error(f"[Warning] Failed to update Index {idx_str}: {e}")

    return df_updated, success_count


def get_ingredients_info():
    """
    Retrieve icook recipe CSV file from the determined field to MongoDB, collection is recipe_ingredients
    The field is listed below:
    1) recipe_id            varchar(64) NOT NULL
    2) recipe_source        varchar(100) NOT NULL
    3) ori_ingredient_id    INT NOT NUlL (?)
    4) ori_ingredient_name  varchar(255)
    5) ingredient_type      varchar(100)
    6) unit_name            varchar(50)
    7) unit_value           float
    8) ins_timestamp        datetime.timestamp()
    9) update_timestamp    datetime.timestamp()
    ### Algorithm:
    - establish logging V
    - mongodb connection V
    - db collection V
    - collection connection V
    - find today's icook recipe CSV file V
    - convert it into pandas dataframe V
    - select determined field V
    - convert str to required data type V
    - collect transformed data V
    - insert transformed data to targeted collection
    - mongodb disconnection
    """

    logger.info(f"Starting execution of {FILENAME}")

    try:
        with open(file=CSV_FILE_PATH, mode="r", encoding="utf-8-sig") as csv:
            raw_df = pd.read_csv(csv)
            logger.info(f"Opened CSV file: {str(CSV_FILE_PATH).split("/")[-1].strip()}")

        switch = False
        for col in raw_df.columns:
            if col == "recipe_source":
                switch = True
                break

        if not switch:
            raw_df["recipe_source"] = "icook"

        mask = [
            "recept_id",
            "recipe_source",
            "recept_type",
            "ingredients",
            "quantity",
        ]
        ingredient_df = raw_df[mask]
        int_time, upd_time = datetime.now(tz=TZ).strftime("%Y-%m-%d %H:%M:%S"), datetime.now(tz=TZ).strftime("%Y-%m-%d %H:%M:%S")
        ingredient_df["ins_timestamp"] = int_time
        ingredient_df["upd_timestamp"] = upd_time

        # Drop Null values of field ingredients
        logger.info("Dropping the values that are Null...")
        ingredient_df.dropna(subset=["ingredients"], inplace=True)
        logger.info("Dropping completed")

        # Unwind values of field ingredients
        logger.info("Unwinding ...")
        ingredient_df_explode = unwind(ingredient_df, "ingredients")
        logger.info("Unwinding completed")

        ### Separate the quantity to get the number part and the unit part dependently
        # Get unit
        logger.info("Retrieving unit only from quantity ...")
        ingredient_df_explode["unit_name"] = ingredient_df_explode["quantity"].apply(unit)
        logger.info("Retrieved unit only from quantity")

        ingredient_df_explode["unit_name"] = ingredient_df_explode["unit_name"].apply(unit_convertion)

        # Get num
        ingredient_df_explode["unit_number"] = ingredient_df_explode["quantity"].apply(num)


        criteria = ["適量", "少許", "依喜好"]
        ingredient_df_explode.loc[ingredient_df_explode["unit_name"].isin(criteria), "unit_number"] = float(1)

        ### Starting LLM out of Gemini 2.5 flash

        # 1.Filter rows (Auto-fill metric units first, return rest for LLM)
        df = ingredient_df_explode.reset_index(drop=True)
        df_to_process = get_rows_needing_processing(df)

        df_final = pd.DataFrame()
        if not df_to_process.empty:
            # 2. Generate English Prompt with Carbon Footprint context
            prompt = construct_hybrid_language_prompt(df_to_process)

            # 3. Call API
            predictions = fetch_gemini_response(prompt)

            if predictions:
                # 4. Update Data
                df_final, count = update_dataset(df, predictions)

                logger.info(f"\n[Result] Successfully updated {count} rows via LLM.")

        else:
            logger.info("[Result] No rows required LLM inference. All data is clean or auto-filled.")
            df_final = ingredient_df_explode
        ### Processing LLM out of Gemini 2.5 flash

        df_final["unit_number"] = df_final["unit_number"].apply(lambda x: float(x)) 

        # Save the final result into CSV file
        df_final.to_csv(SAVED_FILE_PATH, index=False)

    except Exception as e:
        logger.error(f"{e}")

    finally:
        logger.info(f"Finished execution of {FILENAME}")


if __name__ == "__main__":
    get_ingredients_info()
