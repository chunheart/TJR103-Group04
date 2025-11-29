import os 
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
TABLE_NAME = "recipe"

### CSV ###
CATEGORY = "video"
CATEGORY_NUMBER = "583"
MANUAL_DATE = "2025-10-23"
COLLECTION = "recipes"

CSV_FILE_PATH = ROOT_DIR / "data" / "db_recipe" / f"icook_recipe_{CATEGORY}_{CATEGORY_NUMBER}_{MANUAL_DATE}_{COLLECTION}.csv"
# icook_recipe_baby_404_2025-10-24_recipes
# icook_recipe_chinese_349_2025-10-24_recipes
# icook_recipe_cookers_59_2025-10-23_recipes
# icook_recipe_effect_352_2025-10-23_recipes
# icook_recipe_fantasy_19_2025-10-23_recipes
# icook_recipe_festival_31_2025-10-23_recipes
# icook_recipe_general_684_2025-10-23_recipes
# icook_recipe_general_ingds_608_2025-10-23_recipes
# icook_recipe_jam_460_2025-10-23_recipes
# icook_recipe_pets_606_2025-10-23_recipes
# icook_recipe_recipe_58_2025-10-23_recipes
# icook_recipe_saving_money_437_2025-10-23_recipes
# icook_recipe_snack_57_2025-10-23_recipes
# icook_recipe_vegetable_28_2025-10-23_recipes
# icook_recipe_video_583_2025-10-23_recipes
### Move ###
MOVE_DIR = ROOT_DIR / "data" / "db_recipe" / f"processed_recipe={MANUAL_DATE}"
MOVE_DIR.mkdir(exist_ok=True, parents=True)
MOVE_FILE_PATH = ROOT_DIR / "data" / "db_recipe" / f"processed_recipe={MANUAL_DATE}" / f"icook_recipe_{CATEGORY}_{CATEGORY_NUMBER}_{MANUAL_DATE}_{COLLECTION}.csv"

def mysql_recipe():
    """
    Goal:
    Upload the processed recipe data into MySQL

    Algorithm:
    1- connect to MySQL V
    2- connect to selected database V
    2- create table if not exists V
    3- find csv
    4- check data type
    5- load data into the table
    6- if succeeding, close
    7- rename file name to mark as processed
    """
    
    LOGGER.info(f"Start processing {FILENAME} ...")
    
    # Connect to MySQL
    LOGGER.info(f"Start connecting to MySQL ...")
    try:
        conn = vm_mysql_connection()
        LOGGER.info(f"Connected to MySQL")        
    
        LOGGER.info(f"Start retrieving data from {CSV_FILE_PATH} ...")
        try:
            ### Insert data
            # Get CSV file
            with open(file=CSV_FILE_PATH, mode="r", encoding="utf-8-sig") as csv:
                recipe_df = pd.read_csv(csv)
                data_dict_list = recipe_df.to_dict(orient="records")
                LOGGER.info(f"Converted {len(data_dict_list)} rows into dictionary format.")
            
            sql_template = f"""
            INSERT INTO `{TABLE_NAME}`
                (`recipe_id`, `recipe_site`, `recipe_name`, `recipe_url`, `author`, `servings`, `publish_time`, `crawl_time`)
            VALUES
                (%(recept_id)s, %(recipe_source)s, %(recipe_name)s, %(recipe_url)s, %(author)s, %(people)s, %(recipe_upload_date)s, %(crawl_datetime)s);
            """       
            count = 0
            cursor = conn.cursor()
            for item in data_dict_list:
                try:
                    cursor.execute(sql_template, item)
                    conn.commit()
                    LOGGER.info(f"Successfully inserted {item} into {TABLE_NAME}.")
                    count += 1
                    
                except sql.MySQLError as e:
                    LOGGER.error(f"Batch insert failed. Error: {e}")
                    continue

            cursor.close()
            LOGGER.info(f"Cursor has been closed.")
            conn.close()
            LOGGER.info(f"Disconnected the connection to MySQL server.")
            LOGGER.info(f"Inserted {count} data into {TABLE_NAME}.")
            os.rename(src=CSV_FILE_PATH, dst=MOVE_FILE_PATH)
            LOGGER.info(f"{CSV_FILE_PATH} has been moved to {MOVE_FILE_PATH}.")
            
        except Exception as e:
            LOGGER.error(f"System error: {e}")
    
    except sql.MySQLError:
        print(f"Connection failed,{e}")


if __name__ == "__main__":
    mysql_recipe()
