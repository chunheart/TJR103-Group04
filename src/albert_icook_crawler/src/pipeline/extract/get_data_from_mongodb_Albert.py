import csv, os
import utils.mongodb_connection as mongo
import pandas as pd

from datetime import datetime
from pathlib import Path
"""Here we will retrieve data from the MongoDB server and save it as a csv file."""

PROJECT_ROOT = Path(__file__).resolve().parents[3] # find the root directory


if __name__ == "__main__":
    """1. connect to MongoDB"""
    # get connected object, called connection
    client = mongo.connect_to_local_mongodb()
    db = client["TJR103_icook_recipe_Albert"]  # connect to targeted database
    collection = db["icook_recipe_Albert_stage"]  # connect to targeted collection
    """2. get data from MongoDB"""
    projection = {"_id": 0, "ingredients": 1}
    cursor = collection.find({}, projection)
    data_list = list(cursor)
    df = pd.DataFrame(data_list)
    if df is not None :
        print("succeed in retrieving data")
    """3. save the data to csv file"""
    root_dir = PROJECT_ROOT / "data" / "mongodb" / "Albert" # from root dir
    root_dir.mkdir(exist_ok=True, parents=True)
    date = datetime.date(datetime.today())
    save_file = root_dir /  f"ingredient_data_{date}.csv"
    """4. close mongodb"""
    mongo.close_connection(client)
    # ingredient_counts = df.count()
    # print(ingredient_counts)
    # ingredient_dif_counts = df["ingredients"].nunique()
    # print(ingredient_dif_counts)
    try:
        df.to_csv(save_file, index=False)
        print(f"{save_file} saved!")
    except Exception as e:
        print(e)
