import pandas as pd
import os, sys

# import albert_icook_crawler.src.utils.mongodb_connection as mondb
from albert_icook_crawler.src.pipeline.transformation import get_ppl_num as ppl
from albert_icook_crawler.src.utils.get_logger import get_logger

from pathlib import Path
from datetime import datetime

FILENAME = os.path.basename(__file__).split(".")[0]
LOG_ROOT_DIR = Path(__file__).resolve().parents[3] # Root dir : src
LOG_FILE_DIR = LOG_ROOT_DIR / "logs" / f"logs={datetime.today().date()}"
LOG_FILE_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE_PATH = LOG_FILE_DIR /  f"{FILENAME}_{datetime.today().date()}.log"

DATABASE = "mydatabase"
COLLECTION = "recipes"

DATA_ROOT_DIR = Path(__file__).resolve().parents[4] # Root dir : /opt/airflow/src/albert_icook_crawler
CSV_FILE_DIR = DATA_ROOT_DIR / "data" / "daily"
CSV_FILE_DIR.mkdir(parents=True, exist_ok=True)
CSV_FILE_PATH = CSV_FILE_DIR / f"Created_on_{datetime.today().date()}" / f"icook_recipe_{datetime.today().date()}.csv"

SAVED_FILE_DIR = DATA_ROOT_DIR / "data" / "db_recipe"
SAVED_FILE_DIR.mkdir(parents=True, exist_ok=True)
SAVED_FILE_PATH = SAVED_FILE_DIR / f"icook_recipe_{datetime.today().date()}_{COLLECTION}.csv"

logger = get_logger(log_file_path=LOG_FILE_PATH, logger_name=FILENAME)

def main():
    """
    Retrieve icook recipe CSV file from the determined field to MongoDB, collection is recipes
    The field is listed below:
    1) recipe_id            varchar(64) NOT NULL
    2) recipe_source        varchar(100) NOT NULL
    3) recipe_name          varchar(255)
    4) recipe_url           varchar(255)
    5) author               varchar(100)
    6) people               INT
    7) publish_date         datetime
    8) crawl_datetime       datetime
    9) ins_timestamp        datetime.timestamp()
    10) update_timestamp    datetime.timestamp()
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
        raw_df.info()

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
            "recipe_name",
            "recipe_url",
            "author",
            "people",
            "recipe_upload_date",
            "crawl_datetime",
        ]
        recipe_df = raw_df[mask]
        int_time, upd_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S"), datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        recipe_df["ins_timestamp"] = int_time
        recipe_df["upd_timestamp"] = upd_time

        # Remove duplication
        logger.info("Removing duplicates...")
        duplicates_removal_df = recipe_df.drop_duplicates(subset="recept_id", keep="last", ignore_index=True)
        duplicates_removal_df.info()
        logger.info("Removing duplicates completed")

        ### Transform
        # Fill None with "1人份"
        logger.info("Processing fillna for column, people with 1人份 ...")
        duplicates_removal_df["people"] = duplicates_removal_df["people"].fillna("1人份", inplace=False)
        # Convert number into integer type
        duplicates_removal_df["people"] = duplicates_removal_df["people"].apply(ppl.get_recipe_ppl_num)
        logger.info("Processed fillna for column, people with 1人份 ...")
        duplicates_removal_df.info()

        ### Save Data into CSV file
        logger.info(f"Saving data to the path: {SAVED_FILE_PATH}")
        duplicates_removal_df.to_csv(SAVED_FILE_PATH, index=False)
        logger.info(f"Saved data to the path: {SAVED_FILE_PATH}")

        # # Convert DataFrame into Dict
        # logger.info("Converting dataframe to list of dictionaries...")
        # result_list = duplicates_removal_df.to_dict(orient="records")

        # # Load to MongoDB(MySQL)
        # try:
        #     lines = len(result_list)
        #     collection.insert_many(result_list)
        #     result_list.clear()
        #     logger.info(f"Inserted {lines} records")
        #     logger.info(f"Execution completed")
        # except Exception as e:
        #     logger.error(f"Failed to upload converted file to {collection}: {e}")

    except Exception as e:
        logger.error(f"Failed to open CSV file: {e}")

    finally:
        # conn.close()
        # logger.info(f"Disconnect to MongoDB")
        logger.info(f"Finished execution of {FILENAME}")


if __name__ == "__main__":
    main()
