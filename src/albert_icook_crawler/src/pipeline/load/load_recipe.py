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
DB_NAME = "TJR103"
TABLE_NAME = "recipe"

### CSV ###
CSV_FILE_PATH = ROOT_DIR / "data" / "db_recipe" / f"icook_recipe_{datetime.today().date()}_recipes.csv"

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
        conn = mysql_connection()
        LOGGER.info(f"Connected to MySQL")        
    except sql.MySQLError:
        print(f"Connection failed,{e}")

    # Create a database if not exist
    LOGGER.info(f"Connecting to database: {DB_NAME} ...")
    try:
        db_name = DB_NAME
        create_db(conn, db_name)
        LOGGER.info(f"Connected to database: {DB_NAME}")
    except Exception as e:
        print(f"Failed to build a new table, {e}")
    
    # Create a table if not exist
    LOGGER.info(f"Start checking if {TABLE_NAME} exists ...")
    try:
        schema = f"""
        CREATE TABLE `recipe` (
            `recipe_id`      VARCHAR(64)  NOT NULL,
            `recipe_site`    VARCHAR(100) NOT NULL,
            `recipe_name`    VARCHAR(255),
            `recipe_url`  VARCHAR(255),
            `author`         VARCHAR(100),
            `servings`       FLOAT,
            `publish_time`   DATETIME,
            `crawl_time`     DATETIME,
            `ins_time` DATETIME DEFAULT CURRENT_TIMESTAMP,
            `upd_time` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (`recipe_id`, `recipe_site`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        table_info = f"{schema}"
        LOGGER.info(f"Show {TABLE_NAME} Schema :\n{table_info}")
        create_table(conn, schema)
        LOGGER.info(f"{TABLE_NAME} exists")
    except Exception as e:
        LOGGER.error(e)

    

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
        
        cursor = conn.cursor()
        try:
            cursor.executemany(sql_template, data_dict_list)
            conn.commit()
            LOGGER.info(f"Successfully inserted {len(data_dict_list)} records.")
            
        except sql.MySQLError as e:
            conn.rollback()
            LOGGER.critical(f"Batch insert failed. Error: {e}")

    #     for index, row in recipe_df.iterrows():
    #         try:
    #             recipe_id = row.iloc[0]
    #             recipe_site = row.iloc[1]
    #             recipe_name = row.iloc[2]
    #             recipe_url = row.iloc[3]
    #             author = row.iloc[4]
    #             servings = float(row.iloc[5])
    #             publish_time = datetime.date(row.iloc[6])
    #             crawl_time = datetime(row.iloc[7]).strftime("%Y-%m-%d %H:%M:%S")

    #             insert_syntax = f"""
    #             INSERT INTO `{TABLE_NAME}`
    #                 (`recipe_id`, `recipe_site`, `recipe_name`, `recipe_url`, `author`, `servings`, `publish_time`, `crawl_time`)
    #             VALUES
    #                 ('{recipe_id}', '{recipe_site}', '{recipe_name}','{recipe_url}', '{author}', {servings}, '{publish_time}', {crawl_time});
    #             """
    #             cursor.execute(insert_syntax)
    #         except sql.MySQLError as e:
    #             LOGGER.critical(f"{index} has an issue, the data cannot be uploaded into {TABLE_NAME}")
    #             continue
    #     cursor.close()

    except Exception as e:
        LOGGER.error(f"System error: {e}")

    cursor.close()
    LOGGER.info(f"Cursor is Disconnected")

    conn.close()
    LOGGER.info(f"MySQL server is Disconnected")
    

if __name__ == "__main__":
    mysql_recipe()
