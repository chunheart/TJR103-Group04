import pandas as pd
import os, sys
import re
import src.utils.mongodb_connection as mondb
from src.utils.get_logger import get_logger
from src.pipeline.transformation.get_num import get_num_field_quantity as num
from src.pipeline.transformation.get_unit import get_unit_field_quantity as unit
from pathlib import Path
from datetime import datetime


FILENAME = os.path.basename(__file__).split(".")[0]
LOG_ROOT_DIR = Path(__file__).resolve().parents[3] # root is dir src
LOG_FILE_DIR = LOG_ROOT_DIR / "logs" / f"logs=2025-11-19"
LOG_FILE_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE_PATH = LOG_FILE_DIR /  f"{FILENAME}_2025-11-19.log"

CSV_ROOT_DIR = Path(__file__).resolve().parents[4]
CSV_FILE_PATH = CSV_ROOT_DIR / "data" / "daily" / f"Created_on_2025-11-19" / f"icook_recipe_2025-11-19.csv"
DATABASE = "mydatabase"
COLLECTION = "recipe_ingredients"


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

def unit_g_convertion(s:str)-> str:
    if s == "克" or s == "公克":
        return "g"
    elif s == "公斤":
        return "kg"
    else:
        return s


def main():
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

    logger = get_logger(log_file_path=LOG_FILE_PATH, logger_name=FILENAME)
    logger.info(f"Starting execution of {FILENAME}")

    logger.info("Connecting to MongoDB...")
    try:
        conn = mondb.connect_to_local_mongodb()
    except Exception as e:
        logger.error(f"Failed to establish MongoDB connection: {e}")

        logger.critical(f"Aborting execution of {FILENAME} due to fatal error")
        sys.exit(1)

    logger.info(f"Connected to MongoDB")

    logger.info("Connecting to Database, mydatabase...")
    try:
        db = conn[DATABASE]
    except Exception as e:
        logger.error(f"Failed to connect to Database: {e}")
        conn.close()
        logger.critical(f"Closed connection to MongoDB")
        logger.critical(f"Aborting execution of {FILENAME} due to fatal error")
        sys.exit(1)

    collection = db[COLLECTION]
    try:
        with open(file=CSV_FILE_PATH, mode="r", encoding="utf-8-sig") as csv:
            raw_df = pd.read_csv(csv)
            logger.info(f"Opened CSV file: {str(CSV_FILE_PATH).split("/")[-1].strip()}")
        # raw_df.info()

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
            "unit",
        ]
        ingredient_df = raw_df[mask]
        int_time, upd_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S"), datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ingredient_df["ins_timestamp"] = int_time
        ingredient_df["upd_timestamp"] = upd_time
        # ingredient_df.info()

        # Drop Null values of field ingredients
        logger.info("Dropping the values that are Null...")
        ingredient_df.dropna(subset=["ingredients"], inplace=True)
        # ingredient_df.info()
        logger.info("Dropping completed")

        # Unwind values of field ingredients
        ingredient_df_explode = unwind(ingredient_df, "ingredients")

        # Remove parentheses of values of field ingredients
        ingredient_df_explode["t_ingredients"] = ingredient_df_explode["ingredients"].apply(remove_parentheses)
        # ingredient_df_explode.info()

        ### Separate the quantity to get the number part and the unit part dependently
        # Get unit
        ingredient_df_explode["t_unit"] = ingredient_df_explode["quantity"].apply(unit)
        # Get num
        ingredient_df_explode["t_number"] = ingredient_df_explode["quantity"].apply(num)
        ingredient_df_explode["t_unit"] = ingredient_df_explode["t_unit"].apply(unit_g_convertion)
        with open(file="test.csv", mode="w", encoding="utf-8-sig", newline="") as csv_file:
            csv_file.write(ingredient_df_explode.to_csv(index=False))




    #     # Convert DataFrame into Dict
    #     logger.info("Converting dataframe to list of dictionaries...")
    #     result_list = duplicates_removal_df.to_dict(orient="records")
    #
    #     # Load
    #     try:
    #         lines = len(result_list)
    #         collection.insert_many(result_list)
    #         result_list.clear()
    #         logger.info(f"Inserted {lines} records")
    #         logger.info(f"Execution completed")
    #     except Exception as e:
    #         logger.error(f"Failed to upload converted file to {collection}: {e}")
    #
    except Exception as e:
        logger.error(f"{e}")

    finally:
        conn.close()
        logger.info(f"Disconnect to MongoDB")
        logger.info(f"Finished execution of {FILENAME}")


if __name__ == "__main__":
    main()
    # # data = {
    # #     "type": ["parentheses", "parentheses", "parentheses"],
    # #     "food": ["香料鹽（牛肉用和魚用的不同）", "高麗菜(甘藍)", "福山萵苣(大陸妹)"]
    # # }
    # # df = pd.DataFrame(data)
    # data = ["香料鹽（牛肉用和魚用的不同）"]
    # ans = ""
    # for _ in data:
    #     _ = _.strip().replace(" ", "")
    #     print(_)
    #     t_s = remove_parentheses(_)
    #     ans += t_s + "\n"
    # print(ans)