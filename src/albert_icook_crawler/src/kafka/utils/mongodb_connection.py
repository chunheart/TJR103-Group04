import os
import pymongo as mon

from dotenv import load_dotenv
from pymongo.server_api import ServerApi
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3] # root directory
ENV_FILE_PATH = PROJECT_ROOT / "kafka" / ".env"
load_dotenv(ENV_FILE_PATH)


"""connect to local MongoDB Server"""
USERNAME=os.getenv("MONGO_LOCAL_USERNAME")
PASSWORD=os.getenv("MONGO_LOCAL_PASSWORD")
HOST=os.getenv("MONGO_LOCAL_HOST")
PORT=os.getenv("MONGO_LOCAL_PORT")
AUTH_DB=os.getenv("MONGO_LOCAL_AUTH_DB")

ONLINE_USERNAME=os.getenv("MONGO_ONLINE_USERNAME")
ONLINE_PASSWORD=os.getenv("MONGO_ONLINE_PASSWORD")

def connect_to_online_mongodb():
    """connect to MongoDB Server"""
    connection_uri = f"mongodb+srv://{ONLINE_USERNAME}:{ONLINE_PASSWORD}@cluster0.fyyzcmn.mongodb.net/?appName=Cluster0"
    server_api = ServerApi("1")
    try:
        connection = mon.MongoClient(connection_uri, server_api=server_api) # connecting
        print(connection.server_info())
        print("connect successfully to MongoDB")
        return connection

    except Exception as e:
        print(f"Authentication failed: {e}")
        return None

def connect_to_local_mongodb():
    """connect to MongoDB Server"""
    connection_string = f"mongodb://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/?authSource={AUTH_DB}"
    try:
        connection = mon.MongoClient(connection_string) # connecting
        print(connection.server_info())
        print("connect successfully to MongoDB")
        return connection

    except Exception as e:
        print(f"Authentication failed: {e}")
        return None

def close_connection(db_connection):
    """ disconnect to MongoDB Server"""
    print("All data has been uploaded and MongoDB is disconnected")
    db_connection.close()


if __name__ == "__main__":
    # client = connect_to_online_mongodb()
    # close_connection(client)
    pass