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

ROOT_DIR = Path(__file__).resolve().parents[4] # Root: /opt/airflow/src/albert_icook_crawler


FILENAME = os.path.basename(__file__).split(".")[0]
LOG_FILE_DIR = ROOT_DIR / "src" / "logs" / f"logs={datetime.today().date()}"
LOG_FILE_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE_PATH = LOG_FILE_DIR /  f"{FILENAME}_{datetime.today().date()}.log"


CSV_FILE_PATH = ROOT_DIR / "data" / "daily" / f"Created_on_{datetime.today().date()}" / f"icook_recipe_{datetime.today().date()}.csv"
COLLECTION = "recipe_ingredients"

DATA_ROOT_DIR = Path(__file__).resolve().parents[4]
SAVED_FILE_DIR = DATA_ROOT_DIR / "data" / "db_ingredients"
SAVED_FILE_DIR.mkdir(parents=True, exist_ok=True)
SAVED_FILE_PATH =  SAVED_FILE_DIR / f"icook_recipe_{datetime.today().date()}_{COLLECTION}.csv"

logger = get_logger(log_file_path=LOG_FILE_PATH, logger_name=FILENAME)


def remove_parentheses(s:str) -> str:
    if_parentheses = r"[(){}\"\[\]（）]"
    pattern = r"\((.*?)\)|\[(.*?)\]|\{(.*?)\}|\"(.*?)\"|（(.*?)）"
    if re.search(if_parentheses, s):
        return re.sub(pattern, "", s)
    return s

def unwind(df:pd.DataFrame, col_name:str)-> pd.DataFrame | None:
    mix_separator_pattern = r"[,，/| ]+"
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


def main():
    """
    Retrieve icook recipe CSV file from the determined field to MongoDB, collection is ingredient_normalize
    The field is listed below:
    1) ori_ingredient_id    INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    2) ori_ingredient_name  varchar(255),
    3) nor_ingredient_name  varchar(255), 
    4) ins_time` DATETIME DEFAULT CURRENT_TIMESTAMP
    5) upd_time` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    6) status
    
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

    # logger.info("Connecting to MongoDB...")
    # try:
    #     conn = mondb.connect_to_local_mongodb()
    # except Exception as e:
    #     logger.error(f"Failed to establish MongoDB connection: {e}")

    #     logger.critical(f"Aborting execution of {FILENAME} due to fatal error")
    #     sys.exit(1)

    # logger.info(f"Connected to MongoDB")

    # logger.info("Connecting to Database, mydatabase...")
    # try:
    #     db = conn[DATABASE]
    #     logger.info("Connected to Database, mydatabase...")
    # except Exception as e:
    #     logger.error(f"Failed to connect to Database: {e}")
    #     conn.close()
    #     logger.critical(f"Closed connection to MongoDB")
    #     logger.critical(f"Aborting execution of {FILENAME} due to fatal error")
    #     sys.exit(1)

    # collection = db[COLLECTION]
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
        int_time, upd_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S"), datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
        # Remove parentheses of values of field ingredients
        # ingredient_df_explode["Ingredient_Name"] = ingredient_df_explode["ingredients"].apply(remove_parentheses)
        # ingredient_df_explode.info()

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



        df_final = pd.DataFrame()


        df_final["unit_number"] = df_final["unit_number"].apply(lambda x: float(x)) 

        # Save the final result into CSV file
        df_final.to_csv(SAVED_FILE_PATH, index=False)

        # Convert DataFrame into Dict
        # logger.info("Converting dataframe to list of dictionaries...")
        # result_list = df_final.to_dict(orient="records")

        # Load to MongoDB(MySQL)
        # try:
        #     lines = len(result_list)
        #     collection.insert_many(result_list)
        #     result_list.clear()
        #     logger.info(f"Inserted {lines} records")
        #     logger.info(f"Execution completed")


        # except Exception as e:
            # logger.error(f"Failed to upload converted file to {collection}: {e}")

    except Exception as e:
        logger.error(f"{e}")

    finally:
        # conn.close()
        # logger.info(f"Disconnect to MongoDB")
        logger.info(f"Finished execution of {FILENAME}")


if __name__ == "__main__":
    main()
