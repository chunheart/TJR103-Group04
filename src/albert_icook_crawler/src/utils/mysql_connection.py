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
CURSOR_CLASS = os.getenv("MYSQL_LOCAL_CURSORCLASS")
WRITE_TIMEOUT = int(120)
READ_TIMEOUT = int(120)


def mysql_connection():
    """connect to MySQL Server"""

    try:
        connection = sql.connect(
            host=HOST,
            user=USER,
            password=PASSWORD,
            port=PORT,
            charset=CHARSET,
            cursorclass=CURSOR_CLASS,
            read_timeout=READ_TIMEOUT,
            write_timeout=WRITE_TIMEOUT,
        )
        print("connect successfully to MySQL server")
        return connection

    except Exception as e:
        raise(f"Authentication failed: {e}")


def create_db(conn, db_name):
    """
    Create db if not exists

    Param conn: MySQL connection instance
    Param db_name: str, expected table name
    Return: None
    """
    try:
        with conn.cursor() as cursor:
            create_db_sql = f"CREATE DATABASE IF NOT EXISTS {db_name} DEFAULT CHARACTER SET utf8mb4"
            cursor.execute(create_db_sql)
            print(f"{db_name} has been created")
        
        conn.select_db(db_name)
        print(f"Switched current connection to: {db_name}")

    except sql.MySQLError as e:
        raise e
    
def create_table(conn, table_name, shema):
    pass



if __name__ == "__main__":
    # client = connect_to_online_mongodb()
    # close_connection(client)

    try:
        conn = mysql_connection()
        if conn:
            print("Connection suceeded")
    
            try:
                conn.close()
                print("MySQL is disconnected")
            except sql.MySQLError as e:
                print(f"{e}")    
    
    except sql.MySQLError as e:
        print(f"Connection failed, {e}")
    