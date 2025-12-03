import glob
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
MANUAL_DATE = "2025-11-30"
COLLECTION = "recipes"
CSV_FILE_DIR = ROOT_DIR / "data" / "db_recipe"
CSV_FILE_PATH = ROOT_DIR / "data" / "db_recipe" / f"icook_recipe_{CATEGORY}_{CATEGORY_NUMBER}_{MANUAL_DATE}_{COLLECTION}.csv"

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
        try:
            LOGGER.info(f"Start finding all CSV files by glob ...")

            file_pattern =  CSV_FILE_DIR / "*.csv"
            csv_file_list = glob.glob(str(file_pattern))

            for csv_file in csv_file_list:
                LOGGER.info(f"Start retrieving data from {csv_file} ...")
                try:
                    ### Insert data
                    # Get CSV file
                    with open(file=csv_file, mode="r", encoding="utf-8-sig") as csv:
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
                    
                    LOGGER.info(f"Inserted {count} data into {TABLE_NAME}.")

                    # Set move file path for processed files 
                    target_csv_file = str(csv_file).split("/")[-1]
                    move_file_path = MOVE_DIR / target_csv_file
                    
                    os.rename(src=csv_file, dst=move_file_path)
                    LOGGER.info(f"{csv_file} has been moved to {move_file_path}.")
                    
                except Exception as e:
                    LOGGER.error(f"System error: {e}")
                    continue

        except Exception as e:
            print(f"Glob function has an issue, {e}")
    
    except sql.MySQLError:
        print(f"Connection failed,{e}")
    
    finally:
        cursor.close()
        LOGGER.info(f"Cursor has been closed.")
        conn.close()
        LOGGER.info(f"Disconnected the connection to MySQL server.")


if __name__ == "__main__":
    mysql_recipe()
