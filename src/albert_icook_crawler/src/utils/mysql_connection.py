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
    
def create_table(conn, schema):
    try:
        with conn.cursor() as cursor:
            cursor.execute(schema)
    except sql.MySQLError as e:
        raise e


if __name__ == "__main__":
    # client = connect_to_online_mongodb()
    # close_connection(client)

    try:
        conn = vm_mysql_connection()
        if conn:
            print("Connection suceeded")

    except sql.MySQLError as e:
        print(f"Connection failed, {e}")
    
    finally:
        conn.close()
        print("MySQL is disconnected")