import glob 
import pandas as pd

from datetime import datetime
from pathlib import Path

from albert_icook_crawler.src.utils.mysql_connection import *
from albert_icook_crawler.src.utils.get_logger import *

ROOT_DIR = Path(__file__).resolve().parents[3] # Root: albert_icook_crawler
FILENAME = os.path.basename(__file__).split(".")[0]

### Log ###
LOG_FILE_DIR = ROOT_DIR/ "src" / "logs" / f"logs={datetime.today().date()}"
LOG_FILE_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE_PATH = LOG_FILE_DIR /  f"{FILENAME}_{datetime.today().date()}.log"
LOGGER = get_logger(LOG_FILE_PATH, FILENAME)

### MySQL ###
# DB_NAME = "TJR103"
TABLE_NAME = "ingredient_normalize"
MANUAL_DATE = "2025-11-27"
### CSV ###
# CSV_FILE_PATH = ROOT_DIR / "data" / "db_ingredients" / f"icook_recipe_{MANUAL_DATE}_recipe_ingredients_name_norm.csv"
TARGET_FILE_DIR = ROOT_DIR / "data" / "yotower_ori_ingredient" / "chunks"

### MOVE ###
MOVE_DIR = ROOT_DIR / "data" / "db_ingredients" / "ingredient_name_normalization" / f"processed_ingredient_name={MANUAL_DATE}"
MOVE_DIR.mkdir(exist_ok=True, parents=True)
MOVE_PATH = MOVE_DIR / f"icook_recipe_{MANUAL_DATE}_recipe_ingredients_name_norm.csv"

def load_unit_normalization():
    """
    Goal:
    Upload the processed data of ingredient names into MySQL

    Algorithm:
    1- connect to MySQL V
    2- find csv V
    3- check data type (optional)
    4- load data into the table (for loop)
    5- if succeeding, close
    6- rename file name to mark as processed
    """

    LOGGER.info(f"Start processing {FILENAME} ...")
    
    # Connect to MySQL
    LOGGER.info(f"Start connecting to MySQL ...")
    try:
        conn = vm_mysql_connection()
        LOGGER.info(f"Connected to MySQL")        

        # Glob, find all CSV file according to the structure
        file_pattern = str(TARGET_FILE_DIR / "processed=*" / "*.csv")
        csv_file_list = glob.glob(file_pattern)
        
        ### Insert data
        LOGGER.info(f"Start retrieving data from {TARGET_FILE_DIR} ...")
        # Get CSV file

        for csv_file in csv_file_list:
            try:
                with open(file=csv_file, mode="r", encoding="utf-8-sig") as csv:
                    recipe_df = pd.read_csv(csv)
                    data_dict_list = recipe_df.to_dict(orient="records")
                    LOGGER.info(f"Converted {len(data_dict_list)} rows into dictionary format.")

                LOGGER.info(f"Start processing uploading data.")
                try:
                    cursor = conn.cursor()
                    sql_template = f"""
                    INSERT INTO `{TABLE_NAME}`
                        (`ori_ingredient_id`, `ori_ingredient_name`, `nor_ingredient_name`, `ins_time`, `upd_time`, `normalize_status`)
                    VALUES
                        (0 ,%(ingredients)s, %(normalized_name)s, DEFAULT, DEFAULT, 'done');
                    """
                    for item in data_dict_list:
                        try:        
                            cursor.execute(sql_template, item)
                            conn.commit()
                            LOGGER.info(f"Successfully inserted {item} records.")
                        except sql.MySQLError as e:
                            LOGGER.critical(f"Batch insert failed. Error: {e}")
                            continue

                    move_dir = TARGET_FILE_DIR / f"load_processed={datetime.today().date()}"
                    move_dir.mkdir(exist_ok=True, parents=True)
                    LOGGER.info(f"The processed file: {csv_file}\nMoving to the path:{move_dir}")
                    
                    moved_file = str(csv_file).split("/")[-1]
                    os.rename(
                        src=csv_file, 
                        dst=move_dir / moved_file,
                    )
                    LOGGER.info(f"The file has been moved.")


                except sql.MySQLError as e:
                    LOGGER.error(f"Cursor connection failed, {e}")
                    continue
            
            except FileExistsError as e:
                LOGGER.error(f"File: {csv_file} does't exist.")
                LOGGER.error(f"System error: {e}")
                continue

    except sql.MySQLError as e:
        LOGGER.error(f"Connection failed,{e}")
    
    finally:
        cursor.close()
        LOGGER.info(f"Cursor has been closed.")
        conn.close()
        LOGGER.info(f"Disconnected the connection to MySQL server.")
        

if __name__ == "__main__":
    load_unit_normalization()
