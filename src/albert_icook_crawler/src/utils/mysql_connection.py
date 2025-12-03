import os
import pymysql as sql

from dotenv import load_dotenv
from pymongo.server_api import ServerApi
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2] # Root : /opt/airflow/src/albert_icook_crawler
ENV_FILE_PATH = PROJECT_ROOT / "src" / "utils" / ".env"
load_dotenv(ENV_FILE_PATH)


"""connect to local MySQL Server"""
HOST = os.getenv("MYSQL_LOCAL_HOST")
USER = os.getenv("MYSQL_LOCAL_USER")
PASSWORD = os.getenv("MYSQL_LOCAL_PASSWORD")
PORT = int(os.getenv("MYSQL_LOCAL_PORT"))
CHARSET = os.getenv("MYSQL_LOCAL_CHARSET")
WRITE_TIMEOUT = int(120)
READ_TIMEOUT = int(120)

"""connect to GCP MySQL Server"""
GCP_HOST = os.getenv("MYSQL_GCP_HOST")
GCP_USER = os.getenv("MYSQL_GCP_USER")
GCP_PASSWORD = os.getenv("MYSQL_GCP_PASSWORD")
GCP_PORT = int(os.getenv("MYSQL_GCP_PORT"))
GCP_DATABASE= os.getenv("MYSQL_GCP_DATABASE")
GCP_CHARSET = os.getenv("MYSQL_GCP_CHARSET")



def mysql_connection():
    """connect to MySQL Server"""

    try:
        connection = sql.connect(
            host=HOST,
            user=USER,
            password=PASSWORD,
            port=PORT,
            charset=CHARSET,
            read_timeout=READ_TIMEOUT,
            write_timeout=WRITE_TIMEOUT,
        )
        print("Connected to MySQL server")
        return connection

    except Exception as e:
        raise(f"Authentication failed: {e}")


def vm_mysql_connection():
    """connect to MySQL Server"""

    try:
        connection = sql.connect(
            host=GCP_HOST,
            user=GCP_USER,
            password=GCP_PASSWORD,
            port=GCP_PORT,
            charset=GCP_CHARSET,
            database=GCP_DATABASE,
            read_timeout=READ_TIMEOUT,
            write_timeout=WRITE_TIMEOUT,
        )
        print("Connected to MySQL server")
        return connection

    except Exception as e:
        raise(f"Authentication failed: {e}")



def create_db(conn, db_name):
    # """
    # Create db if not exists

    # Param conn: MySQL connection instance
    # Param db_name: str, expected table name
    # Return: None
    # """
    # try:
    #     with conn.cursor() as cursor:
    #         create_db_sql = f"CREATE DATABASE IF NOT EXISTS {db_name} DEFAULT CHARACTER SET utf8mb4"
    #         cursor.execute(create_db_sql)
    #         print(f"{db_name} has been created")

    # except sql.MySQLError as e:
    #     print(f"{db_name} has existed, not allowed to be recreated")
    #     print(e)
    
    # finally:
    #     conn.select_db(db_name)
    #     print(f"Switched current connection to: {db_name}")
    # 1. Security: Basic validation to prevent injection
    """
    Create db if not exists and switch connection to it.

    Param conn: MySQL connection instance
    Param db_name: str, expected DATABASE name
    Return: None
    """
    if not db_name.isalnum():
        raise ValueError("Database name must be alphanumeric to prevent SQL injection.")

    try:
        with conn.cursor() as cursor:
            # 2. Removed 'IF NOT EXISTS' so we can catch the error explicitly
            create_db_sql = f"CREATE DATABASE {db_name} DEFAULT CHARACTER SET utf8mb4"
            cursor.execute(create_db_sql)
            print(f"SUCCESS: '{db_name}' has been created.")

            # 3. Switch inside the try block. Only switch if creation or check succeeds.
            conn.select_db(db_name)
            print(f"Switched current connection to: {db_name}")

    except sql.MySQLError as e:
        # Check specific error code for "Database exists" (Error 1007)
        if e.args[0] == 1007:
            print(f"INFO: '{db_name}' already exists. Skipping creation.")
            # Still need to switch to it if it exists
            conn.select_db(db_name) 
            print(f"Switched current connection to: {db_name}")
        else:
            # If it's a different error (e.g., Permission denied), print it real error
            print(f"ERROR: Failed to create database. Reason: {e}")
    
def create_table(conn:object, 
                 table_name:str, 
                 schema:str,
    ):
    try:
        with conn.cursor() as cursor:
            cursor.execute(schema)
            print(f"Table {table_name} has been created")
    except sql.MySQLError as e:
        print(e)
        raise e


if __name__ == "__main__":
    # client = connect_to_online_mongodb()
    # close_connection(client)

    ddls = {
        "recipe": """
            CREATE TABLE IF NOT EXISTS `recipe` (
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
        """,
        "recipe_ingredient": """
            CREATE TABLE IF NOT EXISTS `recipe_ingredient` (
                `recipe_id` VARCHAR(64) NOT NULL,
                `recipe_site` VARCHAR(100) NOT NULL,
                `ori_ingredient_id` INT NOT NULL,
                `ori_ingredient_name` VARCHAR(255),
                `ingredient_type` VARCHAR(100),
                `unit_name` VARCHAR(50),
                `unit_value` FLOAT,
                `ins_time` DATETIME DEFAULT CURRENT_TIMESTAMP,
                `upd_time` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (`recipe_id`,`recipe_site`,`ori_ingredient_id`)
                FOREIFN KEY (`ori_ingredient_id`) REFERENCE ingredient_normalize (`ori_ingredient_id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """,
        "unit_normalize": """
            CREATE TABLE IF NOT EXISTS `unit_normalize` (
                `ori_ingredient_id`  INT NOT NULL,
                `ori_ingredient_name` VARCHAR(255),
                `unit_name`           VARCHAR(50) NOT NULL,
                `weight_grams`        FLOAT,
                `ins_time` DATETIME DEFAULT CURRENT_TIMESTAMP,
                `upd_time` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                `u2g_status` ENUM('pending','running','done','error','manual')
                        NOT NULL DEFAULT 'pending',
                PRIMARY KEY (`ori_ingredient_id`, `unit_name`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """,
        "ingredient_normalize": """
            CREATE TABLE IF NOT EXISTS `ingredient_normalize` (
                `ori_ingredient_id`   INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                `ori_ingredient_name` VARCHAR(255),
                `nor_ingredient_name` VARCHAR(255),
                `ins_time` DATETIME DEFAULT CURRENT_TIMESTAMP,
                `upd_time` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                `normalize_status` ENUM('pending','running','done','error','manual')
                              NOT NULL DEFAULT 'pending',
                UNIQUE KEY `uniq_ori_ingredient_name` (`ori_ingredient_name`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """,
        "carbon_emission": """
            CREATE TABLE IF NOT EXISTS `carbon_emission` (
                `coe_source`          VARCHAR(100),
                `nor_ingredient_name` VARCHAR(255),
                `ref_ingredient_name` VARCHAR(255) DEFAULT null,
                `publish_time`        DATETIME,
                `crawl_time`          DATETIME,
                `coe_category`        VARCHAR(100),
                `weight_g2g`          FLOAT,
                `ins_time` DATETIME DEFAULT CURRENT_TIMESTAMP,
                `upd_time` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                `coe_status` ENUM('pending','running','done','error','manual')
                        NOT NULL DEFAULT 'pending',
                PRIMARY KEY (`coe_source`,`nor_ingredient_name`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """,
    }

    try:
        conn = mysql_connection()
        if conn:
            print("Connection succeed")
        conn.select_db("TJR103")
        table_name = "ingredient_normalize"
        schema_ingredient_name_norm = f"""
            CREATE TABLE IF NOT EXISTS `{table_name}` (
                `ori_ingredient_id`   INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                `ori_ingredient_name` VARCHAR(255),
                `nor_ingredient_name` VARCHAR(255),
                `ins_time` DATETIME DEFAULT CURRENT_TIMESTAMP,
                `upd_time` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                `normalize_status` ENUM('pending','running','done','error','manual')
                              NOT NULL DEFAULT 'pending',
                UNIQUE KEY `uniq_ori_ingredient_name` (`ori_ingredient_name`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        create_table(
            conn=conn,
            table_name=table_name,
            schema=schema_ingredient_name_norm
        )
        table_name = "unit_normalize"
        schema_unit_normalize = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
        `ori_ingredient_id`   INT NOT NULL COMMENT 'FK: 對應父表ID',
        `ori_ingredient_name` VARCHAR(255) COMMENT '冗餘欄位: 方便查看名稱',
        `unit_name`           VARCHAR(50) NOT NULL COMMENT '單位名稱',
        `weight_grams`        FLOAT COMMENT '換算重量',
        `ins_time`            DATETIME DEFAULT CURRENT_TIMESTAMP,
        `upd_time`            DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        `u2g_status`          ENUM('pending','running','done','error','manual') NOT NULL DEFAULT 'pending',
        
        -- 設定複合主鍵 (同一個食材 ID 不能有重複的 unit_name)
        PRIMARY KEY (`ori_ingredient_id`, `unit_name`),
        
        -- 建立外鍵約束
        CONSTRAINT `fk_unit_to_ingredient`
            FOREIGN KEY (`ori_ingredient_id`) 
            REFERENCES `ingredient_normalize` (`ori_ingredient_id`)
            ON DELETE CASCADE 
            ON UPDATE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='單位換算表'
        """
        create_table(
            conn=conn,
            table_name=table_name,
            schema=schema_unit_normalize,
        )



    except sql.MySQLError as e:
        print(f"Connection failed, {e}")
    
    finally:
        conn.close()
        print("MySQL is disconnected")


