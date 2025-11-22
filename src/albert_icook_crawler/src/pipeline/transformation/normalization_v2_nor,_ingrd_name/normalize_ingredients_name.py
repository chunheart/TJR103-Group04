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
ENV_PATH = Path(ROOT_DIR/ "src" / "utils"/ ".env")
load_dotenv(ENV_PATH)

FILENAME = os.path.basename(__file__).split(".")[0]
LOG_FILE_DIR = ROOT_DIR / "src" / "logs" / f"logs={datetime.today().date()}"
LOG_FILE_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE_PATH = LOG_FILE_DIR /  f"{FILENAME}_{datetime.today().date()}.log"

CSV_FILE_PATH = ROOT_DIR / "data" / "db_ingredients" / f"icook_recipe_{datetime.today().date()}_recipe_ingredients.csv"
COLLECTION = "ingredient_normalize"

DATA_ROOT_DIR = Path(__file__).resolve().parents[4]
SAVED_FILE_DIR = DATA_ROOT_DIR / "data" / "db_ingredient_normalize"
SAVED_FILE_DIR.mkdir(parents=True, exist_ok=True)
SAVED_FILE_PATH =  SAVED_FILE_DIR / f"icook_recipe_{datetime.today().date()}_{COLLECTION}.csv"

logger = get_logger(log_file_path=LOG_FILE_PATH, logger_name=FILENAME)

INGREDIENT_MAP = {
    # --- 1. 基本 (Staples) ---
    # ori_ingredient_name: nor_ingredient_name
    "水": "水",
    "熱水": "水",
    "冷水": "水",
    "清水": "水",
    "開水": "水",
    "冷開水": "水",
    "冰塊": "水",
    "溫水": "水",

    "鹽": "鹽",
    "鹽巴": "鹽",
    "海鹽": "鹽",
    "塩": "鹽",
    "胡椒鹽": "鹽",  # (或 "胡椒", 視情況而定)

    "糖": "糖",
    "砂糖": "糖",
    "細砂糖": "糖",
    "冰糖": "糖",
    "黑糖": "糖",
    "二砂糖": "糖",
    "白砂糖": "糖",
    "糖粉": "糖",

    # --- 2. 調味料 (Seasonings) ---
    "醬油": "醬油",
    "醬油膏": "醬油膏",
    "蠔油": "蠔油",
    "素蠔油": "蠔油",
    "魚露": "魚露",
    "烏醋": "烏醋",
    "白醋": "白醋",
    "醋": "白醋",
    "番茄醬": "番茄醬",
    "蕃茄醬": "番茄醬",
    "辣豆瓣醬": "豆瓣醬",
    "豆瓣醬": "豆瓣醬",
    "味噌": "味噌",
    "美乃滋": "美乃滋",

    # --- 3. 油脂 (Oils & Fats) ---
    "油": "食用油",
    "食用油": "食用油",
    "沙拉油": "食用油",
    "植物油": "食用油",
    "橄欖油": "橄欖油",
    "香油": "芝麻油",
    "麻油": "芝麻油",
    "奶油": "奶油",
    "無鹽奶油": "奶油",
    "奶油乳酪": "奶油乳酪",

    # --- 4. 辛香料 (Aromatics) ---
    "洋蔥": "洋蔥",
    "蒜頭": "蒜",
    "蒜": "蒜",
    "蒜末": "蒜",
    "大蒜": "蒜",
    "蒜泥": "蒜",
    "薑": "薑",
    "薑片": "薑",
    "薑末": "薑",
    "老薑": "薑",
    "薑絲": "薑",
    "薑泥": "薑",
    "蔥": "蔥",
    "青蔥": "蔥",
    "蔥花": "蔥",
    "蔥段": "蔥",
    "蔥末": "蔥",
    "紅蔥頭": "紅蔥頭",
    "油蔥酥": "油蔥酥",
    "辣椒": "辣椒",
    "紅辣椒": "辣椒",
    "香菜": "香菜",
    "九層塔": "九層塔",

    # --- 5. 穀物 & 澱粉 (Grains & Starches) ---
    "低筋麵粉": "麵粉",
    "中筋麵粉": "麵粉",
    "高筋麵粉": "麵粉",
    "麵粉": "麵粉",
    "太白粉": "太白粉",
    "玉米粉": "玉米粉",
    "地瓜粉": "地瓜粉",
    "糯米粉": "糯米粉",
    "在來米粉": "在來米粉",
    "麵包粉": "麵包粉",
    "白飯": "米飯",
    "白米": "米",
    "米": "米",
    "義大利麵": "義大利麵",

    # --- 6. 蛋奶 (Dairy & Egg) ---
    "雞蛋": "雞蛋",
    "蛋": "雞蛋",
    "蛋黃": "雞蛋",  # (註：您可決定是否要拆分)
    "蛋白": "雞蛋",
    "全蛋": "雞蛋",
    "蛋液": "雞蛋",
    "水煮蛋": "雞蛋",
    "牛奶": "牛奶",
    "鮮奶": "牛奶",
    "鮮奶油": "鮮奶油",
    "動物性鮮奶油": "鮮奶油",

    # --- 7. 蔬菜 & 菇類 (Vegetables & Mushrooms) ---
    "紅蘿蔔": "紅蘿蔔",
    "胡蘿蔔": "紅蘿蔔",
    "馬鈴薯": "馬鈴薯",
    "小黃瓜": "小黃瓜",
    "高麗菜": "高麗菜",
    "白蘿蔔": "白蘿蔔",
    "番茄": "番茄",
    "蕃茄": "番茄",
    "牛番茄": "番茄",
    "牛蕃茄": "番茄",
    "小番茄": "番茄",
    "香菇": "香菇",
    "乾香菇": "香菇",
    "杏鮑菇": "杏鮑菇",
    "金針菇": "金針菇",
    "蘑菇": "蘑菇",
    "黑木耳": "木耳",
    "木耳": "木耳",
    "鴻喜菇": "鴻喜菇",
    "鴻禧菇": "鴻喜菇",  # (錯字同義)
    "玉米": "玉米",
    "玉米粒": "玉米",
    "玉米筍": "玉米筍",  # (註：玉米筍和玉米在生物和碳排上可能不同)
    "地瓜": "地瓜",
    "南瓜": "南瓜",

    # --- 8. 肉類 & 海鮮 (Meat & Seafood) ---
    "豬絞肉": "豬肉",
    "絞肉": "豬肉",  # (假設 "絞肉" 預設為豬肉)
    "五花肉": "豬肉",
    "排骨": "豬肉",
    "雞胸肉": "雞肉",
    "去骨雞腿肉": "雞肉",
    "雞腿肉": "雞肉",
    "蝦米": "蝦",
    "蝦仁": "蝦",
    "蝦子": "蝦",
    "蛤蜊": "蛤蜊",
    "鮭魚": "鮭魚",

    # --- 9. 其他 (Others) ---
    "米酒": "米酒",
    "紹興酒": "米酒",
    "味醂": "味醂",
    "味霖": "味醂",
    "檸檬汁": "檸檬",
    "檸檬": "檸檬",
    "蜂蜜": "蜂蜜",
    "泡打粉": "泡打粉",
    "無鋁泡打粉": "泡打粉",
    "酵母": "酵母",
    "速發酵母": "酵母",
    "酵母粉": "酵母",
    "白芝麻": "芝麻",
    "芝麻": "芝麻",
    "紅豆": "紅豆",
    "豆腐": "豆腐",
    "板豆腐": "豆腐",
    "嫩豆腐": "豆腐",
}


def remove_parentheses(s:str) -> str:
    if_parentheses = r"[(){}\"\[\]（）]"
    pattern = r"\((.*?)\)|\[(.*?)\]|\{(.*?)\}|\"(.*?)\"|（(.*?)）"
    if re.search(if_parentheses, s):
        return re.sub(pattern, "", s)
    return s

# def unwind(df:pd.DataFrame, col_name:str)-> pd.DataFrame | None:
#     mix_separator_pattern = r"[,，/| ]+"
#     df_exploded = (
#         df.assign(ingredients=df[col_name].str.split(pat=mix_separator_pattern, regex=True))
#         .explode(col_name)
#         .assign(ingredients=lambda x: x[col_name].str.strip())
#     )
#     return df_exploded

# def unit_convertion(s:str)-> str:
#     if s == "克" or s == "公克":
#         return "g"
#     elif s == "公斤":
#         return "kg"
#     elif s == "CC" or s == "毫升" or s == "ml":
#         return "cc"
#     else:
#         return s


def main():
    """
    Retrieve icook recipe CSV file from the determined field to MongoDB, collection is ingredient_normalize
    The field is listed below:
    1) ori_ingredient_id    INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    2) ori_ingredient_name  varchar(255),
    3) nor_ingredient_name  varchar(255), 
    4) ins_time` DATETIME DEFAULT CURRENT_TIMESTAMP
    5) upd_time` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    6) normalize_status ENUM('pending','running','done','error','manual') NOT NULL DEFAULT 'pending',
    
    ### Algorithm:
    - establish logging V
    - MySQL connection V
    - db collection V
    - collection connection V
    - find today's icook recipe CSV file V
    - convert it into pandas dataframe V
    - select determined field V
    - convert str to required data type V
    - collect transformed data V
    - Save it into CSV file
    - insert transformed data to targeted collection
    - MySQL disconnection
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

        # switch = False
        # for col in raw_df.columns:
        #     if col == "recipe_source":
        #         switch = True
        #         break

        # if not switch:
        #     raw_df["recipe_source"] = "icook"

        mask = [
            "ingredients",
        ]

        # Filter field, ingredients
        ingredient_norm_df = raw_df[mask]

        # Add int_time, upd_time, status
        logger.info(f"Adding field \"int_time\", \"upd_time\", \"status\" ...")

        int_time, upd_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S"), datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        status = "pending"
        ingredient_norm_df["ins_timestamp"] = int_time
        ingredient_norm_df["upd_timestamp"] = upd_time
        ingredient_norm_df["status"] = status
        
        logger.info(f"Added field \"int_time\", \"upd_time\", \"status\" ...")

        # Remove parentheses of values of field ingredients
        logger.info(f"Removing parenthese")
        ingredient_norm_df["t_ingredient_name"] = ingredient_norm_df["ingredients"].apply(remove_parentheses)
        ingredient_norm_df.info()
        logger.info(f"Removed parenthese")
        
        # # Drop Null values of field ingredients
        # logger.info("Dropping the values that are Null...")
        # ingredient_df.dropna(subset=["ingredients"], inplace=True)
        # logger.info("Dropping completed")

        # # Unwind values of field ingredients
        # logger.info("Unwinding ...")
        # ingredient_df_explode = unwind(ingredient_df, "ingredients")
        # logger.info("Unwinding completed")
        


        # Save the final result into CSV file
        # df_final.to_csv(SAVED_FILE_PATH, index=False)

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
    # main()
    pass
