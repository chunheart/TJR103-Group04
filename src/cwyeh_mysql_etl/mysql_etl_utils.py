import random
import json
import math
import time
import os
import shutil
import glob
from difflib import SequenceMatcher

import numpy as np
import pandas as pd
import datetime as dt
import pymysql
import requests
from google import genai

import cwyeh_coemission.carboncloud_crawler as cbc
import cwyeh_coemission.myemission_crawler as mec
import kevin_food_unit_normalization.main as unor


"""
Utils for mysql ETL, CRUD.
- including tools for develop and testing, helps functions
"""

# GLOBALs ------------------------------------------------------------
TRANS_API_KEY = os.getenv("MY_GOOGLE_TRANS_API_KEY")
GEMINI_API_KEY = os.getenv("MY_GEMINI_API_KEY")
MYSQL_PASSWORD = os.getenv("MYSQL_ROOT_PASSWORD")


### Helpers
def get_normalize(ori_ingredient_name: str) -> str:
    """回傳正規化後的食材名稱（暫時直接回原名或做簡單標準化）"""
    return ori_ingredient_name


def query_unit2gram(pair_list,batch_size=30) -> list:
    """
    查詢該食材 + 單位對應的克重
    pair_list - [{'name':'雞蛋','unit':'顆'},]

    #random.uniform(0,200)
    """

    class SimpleUnitNormalize(unor.IngredientNormalizer):
        def __init__(self,key):
            self.client = genai.Client(api_key=key)
            self.mapping_db = super()._load_mapping_db()
    my_nor_unit = SimpleUnitNormalize(key=GEMINI_API_KEY)

    size = len(pair_list)
    batch = math.ceil(size/batch_size)
    res_collect = []
    for b in range(batch):
        b_res = my_nor_unit.ask_gemini(pair_list[b*batch_size:(b+1)*batch_size])
        res_collect += b_res.get('items',[])
    return res_collect



def translate(qlist: list[str], key, target_lang: str = "zh-TW") -> list[str]:
    """
    translate ingredient name to [target_lang] by google translate-API
        - source language is detected
        - target_lang = [EN, zh-TW]
    """
    tar_url = f'https://translation.googleapis.com/language/translate/v2?key={key}'
    payload = {
      "q": qlist,
      "target": target_lang,
      "format": "text",
    }
    res = requests.post(tar_url,data=payload)
    tar_json = res.json()['data']['translations']
    res_list = [r['translatedText'] for r in tar_json]
    return res_list


def query_coemission(nor_ingredient_name: list[str]) -> dict:
    """
    查詢食材名稱的碳排係數，回 dict
    - take zh-TW, translate to EN and query
    - return example
        {'牛肉': {'eng_query':'beef','coe_values':100,'coe_status':'done'}}
    """
    
    ### translate - mapping
    eng_to_query = translate(
        qlist=nor_ingredient_name,
        key=TRANS_API_KEY,
        target_lang='EN',
    )
    eng_to_query = [s.lower().strip() for s in eng_to_query]
    print('Query ingredient coemission:',nor_ingredient_name,eng_to_query)

    ### on-fail return
    fail_res = pd.DataFrame({"query": nor_ingredient_name,"eng_query":eng_to_query,"coe_values":None,"coe_status":'error',})
    fail_res = fail_res.set_index("query").to_dict(orient="index")

    ### crawl and get res (pd.Dataframe)
    res = cbc.carboncloud_crawler(query_list=eng_to_query)
    try:
        if res == None:
            print('[Warning] On crawling fail, return pre-defined data')
            return fail_res
    except:
        pass

    ### clean response carbon data [keep 1 for each query]
    # (1) prod_name contain in query (2) keep top 3 similar name and avg
    filter_if_contain = res.apply(lambda r: r["query"] in r["prod_name"],axis=1)
    cleaned_res = res[filter_if_contain].copy()  # To avoid [SettingWithCopyWarning]
    if cleaned_res.shape[0] == 0:
        print('[Warning] No at least 1 valid record')
        return fail_res
    cleaned_res['score'] = cleaned_res.apply(lambda r: SequenceMatcher(None, r['query'], r['prod_name']).ratio(),axis=1)
    cleaned_res['total_coe'] = pd.to_numeric(cleaned_res['total_coe'], errors="coerce")
    cleaned_res = cleaned_res.sort_values(["query", "score"], ascending=[True, False]).groupby("query").head(3)
    cleaned_res = cleaned_res.groupby("query", as_index=False)["total_coe"].mean()\
            .rename(columns={"total_coe":"coe_values","query":"eng_query"})

    ### combine, set status
    main_df = pd.DataFrame({"query": nor_ingredient_name,"eng_query":eng_to_query})
    main_df = main_df.merge(cleaned_res,on='eng_query',how="left")
    main_df['coe_status'] = np.where(main_df["coe_values"].isnull(), "error", "done")
    main_df['coe_values'] = np.where(main_df["coe_values"].isnull(), None, main_df['coe_values'])
    return main_df.set_index("query").to_dict(orient="index")


def get_where_in_string(a, warp_with_string=True):
    """transform an array into a string of where-in expr"""
    if warp_with_string:
        return ','.join(map(lambda x: '"'+str(x)+'"', a))
    else:
        return ','.join(map(str,a))


def batch_operator(batch_size,func):
    pass


# Init ------------------------------------------------------------
def get_mysql_connection(
        host,port,user,
        password=MYSQL_PASSWORD,
        db=None,
        charset="utf8mb4",
        connect_timeout=5,
        cursorclass=pymysql.cursors.DictCursor,
):
    """
    get an mysql-connection instance
        - note, DB is not assigned

    example:
        HOST = "mysql"
        PORT = 3306
        USER = "root"
        PASSWORD = "<your password>"
    """
    # === 嘗試連線 ===
    conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            charset=charset,
            connect_timeout=connect_timeout,
            cursorclass=cursorclass,
        )
    if db:
        try:
            conn.select_db(db)
        except:
            print('WARNING: DB not exists, return conn with db not selected')
            pass
    return conn


def init_tables(
    conn,
    db_name='EXAMPLE',
    insert_example_records=True,
    drop_tables_if_exists=False,
    create_index=True,
):
    """
    Initialize of tables in assigned DB
    - used for init and create some demo records to play with

    TBA
    - protection with user_permission !?
    """

    ### 建立資料庫
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
    conn.select_db(db_name)

    ### Create tables (DDL)
    # UNIQUE KEY `uniq_ori_ingredient_name` (`ori_ingredient_name`)
    tables = ["recipe_ingredient", "unit_normalize", "ingredient_normalize", "carbon_emission", "recipe"]
    if drop_tables_if_exists:
        for t in tables:
            cursor.execute(f"DROP TABLE IF EXISTS `{t}`;")
    
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
    for name, ddl in ddls.items():
        print('Create table',name,ddl)
        cursor.execute(ddl)

    ### create additional index
    if create_index:
        try:
            index_ddls = [
                """
                CREATE INDEX idx_u2g_status
                ON unit_normalize (u2g_status);
                """,
                """
                CREATE INDEX idx_coe_status
                ON carbon_emission (coe_status);
                """,
                """
                CREATE INDEX idx_normalize_status
                ON ingredient_normalize (normalize_status);
                """,
            ]
            for ddl in index_ddls:
                cursor.execute(ddl)
        except pymysql.err.OperationalError as e:
            # 若索引已存在會拋錯 “Duplicate key name”
            print('Duplicated idx:',e)
        except Exception as e:
            print('Other errors:',e)

    ### Insert example records
    if insert_example_records:
        cursor.execute("""
            INSERT INTO `recipe`
                (`recipe_id`, `recipe_site`, `recipe_name`, `recipe_url`, `author`, `servings`, `publish_time`, `crawl_time`)
            VALUES
                ('F05-0228', 'icook', '番茄炒蛋A','icook.com/12345', 'Alice', 4, '2024-10-10 12:00:00', NOW()),
                ('F05-0229', 'icook', '番茄炒蛋B','icook.com/67890', 'Bob', 2, '2024-10-11 14:30:00', NOW());
        """)
        # 假設自動編號為 1,2
        cursor.execute("""
            INSERT INTO `recipe_ingredient`
                (`recipe_id`, `recipe_site`, `ori_ingredient_id`, `ori_ingredient_name`, `ingredient_type`, `unit_name`, `unit_value`)
            VALUES
                ('F05-0228','icook', 101, '番茄',   '食材', '顆', 2),
                ('F05-0228','icook', 102, '橄欖油', '調味料', '茶匙', 1.5),
                ('F05-0228','icook', 103, '有機雞蛋',   '食材',  '顆', 3),
                ('F05-0229','icook', 104, '牛番茄',   '食材', '顆', 2),
                ('F05-0229','icook', 103, '有機雞蛋',   '食材',  '顆', 2);;
        """)
        cursor.execute("""
            INSERT INTO `unit_normalize`
                (`ori_ingredient_id`, `ori_ingredient_name`, `unit_name`, `weight_grams`)
            VALUES
                (101, '番茄',  '顆', 80),
                (102, '橄欖油', '茶匙', 8),
                (103, '有機雞蛋',   '顆', 50),
                (104, '牛番茄',  '顆', 90);
        """)
        cursor.execute("""
            INSERT INTO `ingredient_normalize`
                (`ori_ingredient_id`, `ori_ingredient_name`, `nor_ingredient_name`)
            VALUES
                (101, '番茄', '番茄'),
                (102, '橄欖油', '橄欖油'),
                (103, '有機雞蛋', '雞蛋'),
                (104, '牛番茄', '番茄');
        """)
        cursor.execute("""
            INSERT INTO `carbon_emission`
                (`nor_ingredient_name`, `publish_time`, `crawl_time`, `coe_source`, `coe_category`, `weight_g2g`)
            VALUES
                ('番茄', '2024-09-01 00:00:00', NOW(), '環保署', 'Vegetable', 0.25),
                ('雞蛋',  '2024-09-01 00:00:00', NOW(), '環保署', 'Protein',   4.5);
        """)
        conn.commit()
        cursor.close()


### Ingest Data from sources
def clean_local_storage(
    base_dir:str='/opt/airflow/data/stage/icook_recipe',
    tar_date=None,
    retention_days:int=14,
):
    """
    清理本地端暫存的原始資料
    """
    if not tar_date:
        tar_date = dt.datetime.today()
    cutoff_date = (tar_date - dt.timedelta(days=retention_days)).date()
    print(f'Clean staged data before {cutoff_date}')

    if not os.path.exists(base_dir):
        print(f"[cleanup] base_dir not exists: {base_dir}")
        return

    for entry in os.scandir(base_dir):
        print(entry)
        if not entry.is_dir():
            continue
    
        date_str = entry.name  # 預期是 yyyymmdd
        dir_path = entry.path

        # 解析資料夾名稱是不是合法日期
        try:
            date_obj = dt.datetime.strptime(date_str, "%Y%m%d").date()
        except ValueError:
            print(f"[cleanup] skip non-date dir: {dir_path}")
            continue

        # (1) Clean outdated data
        if date_obj < cutoff_date:
            print(f"[cleanup] remove old dir: {dir_path}")
            shutil.rmtree(dir_path, ignore_errors=True)
            continue
        
        # (2) Combine data
        # pending


### Get Data ------------------------------------------------------------
def get_recipe_data(
    tar_date:str,
    back_days:int,
    source:str='demo',
    path:str=None,
) -> list:
    """
    get recipe data from several available source
        - some basic cleaning: uncapitalize, strip

    INPUTs
    -----------
    tar_date (yyyymmdd)
    back_days (backfill days since tar_date (including))
    source
      demo: return demo records of recipe
      local: return staged records of recipe
      kafka: (not support, use ingest_data_from_kafka() instead)
    """
    ### setting
    tar_date_dt = dt.datetime.strptime(tar_date, "%Y%m%d").date()
    dates_to_read = [(tar_date_dt - dt.timedelta(days=i)).strftime("%Y%m%d")
                     for i in range(back_days)]
    print(f"[Task1] target_date={tar_date_dt}, back_days={back_days}")
    print(f"[Task1] dates_to_read={dates_to_read}")
    
    if source=='demo':
        demo_recipe = [
          {
            "recipe_id": "F05-7777",
            "recipe_site": "icook",
            "recipe_name": "番茄炒蛋C",
            "recipe_url": "https://www.ytower.com.tw/recipe/iframe-recipe.asp?seq=F05-0228",
            "author": "Cathy",
            "servings": None,
            "cook_time": None,
            "ingredient_type": "食材",
            "ingredient_name": "小番茄",
            "weight_value": 50,
            "weight_unit": "公克",
            "publish_date": tar_date_dt.strftime('%Y-%m-%d'),
            "crawl_time": dt.datetime.now(),
          },
          {
            "recipe_id": "F05-7777",
            "recipe_site": "icook",
            "recipe_name": "番茄炒蛋C",
            "recipe_url": "https://www.ytower.com.tw/recipe/iframe-recipe.asp?seq=F05-0228",
            "author": "Cathy",
            "servings": 2,
            "cook_time": None,
            "ingredient_type": "食材",
            "ingredient_name": "雞蛋",
            "weight_value": 15,
            "weight_unit": "公克",
            "publish_date": tar_date_dt.strftime('%Y-%m-%d'),
            "crawl_time": dt.datetime.now(),
          },
          {
            "recipe_id": "F05-8888",
            "recipe_site": "icook",
            "recipe_name": "茶葉蛋",
            "recipe_url": "https://www.ytower.com.tw/recipe/iframe-recipe.asp?seq=F05-0229",
            "author": "Cathy",
            "servings": 1,
            "cook_time": None,
            "ingredient_type": "食材",
            "ingredient_name": "茶包",
            "weight_value": None,
            "weight_unit": "適量",
            "publish_date": tar_date_dt.strftime('%Y-%m-%d'),
            "crawl_time": dt.datetime.now(),
          },
        ]
        return demo_recipe
        
    if source=='local':

        ### read files from a local path
        results = []
        for date_str in dates_to_read:
            date_dir = os.path.join(path, date_str)
            if not os.path.exists(date_dir):
                print(f"[Task1] skip missing dir: {date_dir}")
                continue
        
            pattern = os.path.join(date_dir, "*.json")
            files = sorted(glob.glob(pattern))
            if not files:
                print(f"[Task1] no files in {date_dir}")
                continue
            
            ### read valid files <valid date>.<...>.json
            print(f"[Task1] reading {len(files)} files from {date_dir}")
            for fp in files:
                print(f'Open local file {fp}')
                try:
                    with open(fp, "r", encoding="utf-8") as f:
                        content = f.read().strip()
                        if not content:
                            continue

                        # assume it's json, not jsonl or else
                        if content.startswith("["):
                            arr = json.loads(content)
                            if isinstance(arr, list):
                                results.extend(arr)
                            else:
                                print(f"[Task1] WARN: {fp} is not JSON array.")

                except Exception as e:
                    print(f"[Task1] failed to read {fp}: {e}")
        return results


def get_coemission_data(source='myemission',translate_api_key=TRANS_API_KEY) -> list[dict]:
    """
    get and clean data from several carbon emission site
    - this is likely to be an one-time task

    Backlog
    - the dumpped data should also pass a normalize layer
    """
    if source=='myemission':

        ### Crawl data
        myemission_df = mec.main_init(ingr_batch_size=20,if_write=False)
        print('Crawl Done')
        
        ### batch translate (EN -> zh-TW)
        size = myemission_df.shape[0]
        batch_size = 20
        batch = math.ceil(size/batch_size)
        trans_name = []
        for b in range(batch):
            qlist = list(myemission_df['name'].iloc[b*batch_size:(b+1)*batch_size])
            res_list = translate(
                qlist=qlist,
                key=translate_api_key,
                target_lang='zh-TW',
            )
            trans_name += res_list
            time.sleep(1)
        myemission_df['nor_ingredient_name'] = trans_name
        print('Translate Done')
        
        ### Transform data
        myemission_df['coe_source'] = 'myemission'
        myemission_df['publish_time'] = dt.datetime.now().isoformat()
        myemission_df['crawl_time'] = dt.datetime.now().isoformat()
        myemission_df = myemission_df.rename(columns = {'category':'coe_category','emissions':'weight_g2g','name':'ref_ingredient_name'})
        myemission_df = myemission_df[['coe_source','nor_ingredient_name','ref_ingredient_name','publish_time','crawl_time','coe_category','weight_g2g']]
        print('Clean Done')
        
        return json.loads(myemission_df.to_json(orient='records'))


### Clean Data ------------------------------------------------------------
def clean_recipe_data(recipe_json:list[dict]) -> list[dict]:
    """
    clean recipe data, so that it is the correct form for downstream use (mysql)
    (1) uncapital
    (2) strip
    """
    if not recipe_json:
        print('No input data')
        return
    df = pd.DataFrame(recipe_json)
    
    ### column mapping
    df = df.rename(columns={
        'ID':'recipe_id',
        'site':'recipe_site',
        '食譜名稱':'recipe_name',
        "來源":'recipe_url',
        '作者':'author',
        '食用人數':'servings',
        '類型':'ingredient_type',
        '名稱':'ingredient_name',
        '重量':'weight_value',
        '重量單位':'weight_unit',
        '上線日期':'publish_date',
        '爬蟲時間':'crawl_time',
    })

    ### unifiy columns
    need_cols = [
        "recipe_id", "recipe_site", "recipe_name", "recipe_url","author", "servings",
        "ingredient_type","ingredient_name","weight_value","weight_unit","publish_date", "crawl_time"
    ]
    for c in need_cols:
        if c not in df.columns:
            df[c] = None
    df = df[need_cols]

    ### clean
    # df.rename(columns={})
    # str: uncapital, strip, replace None
    str_clean_cols = [
        "recipe_id", "recipe_site", "recipe_name", "recipe_url","author","ingredient_type",
        "ingredient_name","weight_unit"
    ]
    for c in str_clean_cols:
        df[c] = df[c].astype(str).str.lower().str.strip().replace({"": None}) 

    # date: valid date, replace None
    date_clean_cols = ["publish_date", "crawl_time"]
    for c in date_clean_cols:
        df[c] = pd.to_datetime(df[c], errors="coerce")
        df[c] = df[c].dt.strftime("%Y-%m-%d %H:%M:%S").replace({pd.NaT: None})

    # numbers (try to extract, convert)
    df["servings"] = df["servings"].str.extract(r"(\d+)")
    for c in ["servings", "weight_value"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
        df[c] = df[c].where(df[c] > 0)
    df['weight_value'] = df['weight_value'].fillna(1)

    ### packing results format
    return json.loads(df.to_json(orient='records'))


### Insert/Register Data ------------------------------------------------------------
def register_recipe(conn, recipe_json: dict):
    """
    1) 取出 recipe 相關的欄位、每個 recipe 僅留一筆
    2) 寫入 recipe 使用 upsert: 若已有相同 recipe_id x recipe site (PK)，則更新欄位 (ON DUPLICATE clause)
    """

    ### Get unique recipe level data and relavant columns
    # empty list = False
    if not recipe_json:
        print('Input data not exist')
        return

    recipe_values = []
    unique_recipe = set()
    for row in recipe_json:
        if (row["recipe_id"],row["recipe_site"]) in unique_recipe:
            continue
        else:
            unique_recipe.add((row["recipe_id"],row["recipe_site"]))
            recipe_values.append((
                row["recipe_id"],
                row["recipe_site"],
                row["recipe_name"],
                row["recipe_url"],
                row["author"],
                row["servings"],
                row["publish_date"],
                row["crawl_time"],
            ))

    sql = """
    INSERT INTO `recipe`
      (`recipe_id`, `recipe_site`, `recipe_name`, `recipe_url`, `author`, `servings`, `publish_time`, `crawl_time`)
    VALUES
      (%s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      `recipe_name`   = VALUES(`recipe_name`),
      `recipe_url`    = VALUES(`recipe_url`),
      `author`        = VALUES(`author`),
      `servings`      = VALUES(`servings`),
      `publish_time`  = VALUES(`publish_time`),
      `crawl_time`    = VALUES(`crawl_time`);
    """
    with conn.cursor() as cur:
        cur.executemany(sql, recipe_values)
        print(f"total rows to insert: {len(recipe_values)}; affected rows: {cur.rowcount}")
        conn.commit()


def register_ingredient(conn, recipe_json: list[dict]):
    """
    1) 針對此次食譜的「所有食材」，寫入 ingredient_normalize
        - 找出 ingredient_normalize 還不存在的食材 (ori_ingredient_name)
        - insert(upsert) 進 ingredient_normalize
    2) 食材的 get_normalize()，延到後面再處理，以 normalize_status 管理 (暫設為 pending)
    """

    ### 準備 insert(upsert) 的資料與模板
    unique_ingr = set({})
    to_insert = []
    for row in recipe_json:
        if row['ingredient_name'] in unique_ingr:
            continue
        else:
            unique_ingr.add(row['ingredient_name'])
            to_insert.append((
                row['ingredient_name'],'pending'
            ))

    sql = """
    INSERT INTO `ingredient_normalize`
        (`ori_ingredient_name`, `normalize_status`)
    VALUES (%s, %s)
    ON DUPLICATE KEY UPDATE
        ori_ingredient_name = ori_ingredient_name;
    """

    ### insert(upsert) 
    with conn.cursor() as cur:
        cur.executemany(sql, to_insert)
        print(f"total rows to insert: {len(to_insert)}; affected rows: {cur.rowcount}")
        conn.commit()


def register_unit(conn, recipe_json: list[dict]):
    """
    1) unit_normalize upsert
        - unique (ori_ingredient_name, unit_name) pair
        - 查 ori_ingredient_name -> ori_ingredient_id
    2) query_unit2gram() 延後再 update，透過 u2g_status 管理
    """
    
    ### Get unique (ingr_name, id) pair
    ingr_unit_pairs = {(row["ingredient_name"],row['weight_unit']) 
                       for row in recipe_json 
                       if row.get("ingredient_name") and row.get("weight_unit")}
    if not ingr_unit_pairs:
        print('No valid data received')
        return
    
    ### Query ingr id and get ingr_name -> ingr_id map
    names = sorted({name for name, _ in ingr_unit_pairs})
    names_to_id = {}
    with conn.cursor() as cur:
        sql = f"""
            SELECT ori_ingredient_id, ori_ingredient_name
            FROM ingredient_normalize 
            WHERE ori_ingredient_name IN ({get_where_in_string(names)})
            """
        cur.execute(sql)
        for row in cur.fetchall():
            # DictCursor: row["ori_ingredient_id"]；Tuple: row[0]
            ori_id = row["ori_ingredient_id"] if isinstance(row, dict) else row[0]
            ori_name = row["ori_ingredient_name"] if isinstance(row, dict) else row[1]
            names_to_id[ori_name] = ori_id

    ### Insert to unit_normalize
    to_insert = [(names_to_id[p[0]],
                p[0],
                p[1],
                None,
                'pending') for p in ingr_unit_pairs]
    sql_ins = """
    INSERT INTO `unit_normalize`
      (`ori_ingredient_id`, `ori_ingredient_name`, `unit_name`, `weight_grams`,`u2g_status`)
    VALUES (%s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        ori_ingredient_id = ori_ingredient_id;
    """
    with conn.cursor() as cur:
        cur.executemany(sql_ins,to_insert)
        print(f"total rows to insert: {len(to_insert)}; affected rows: {cur.rowcount}")
        conn.commit()


def register_coemission_from_recipe(conn, recipe_json: list[dict]):
    """
    1) find normalized ingredient that should be queried for coemission (query later)
    2) insert 
    """
    
    ### 1. get noramalized name from ingredients
    names = {row.get('ingredient_name') for row in recipe_json if row.get("ingredient_name")}
    if not names:
        print('No valid data received')
        return
    with conn.cursor() as cur:
        sql = f"""
            SELECT nor_ingredient_name 
            FROM ingredient_normalize
            WHERE ori_ingredient_name IN ({get_where_in_string(names)})
            """
        cur.execute(sql)
        nor_names = {r["nor_ingredient_name"] for r in cur.fetchall()}

    ### 2. get noramalized name not in carbon_emission
    with conn.cursor() as cur:
        sql = f"""
            SELECT nor_ingredient_name 
            FROM carbon_emission 
            WHERE nor_ingredient_name IN ({get_where_in_string(nor_names)})
            """
        cur.execute(sql)
        exists_nor_names = {r["nor_ingredient_name"] for r in cur.fetchall()}
    query_nor_names = {n for n in nor_names if n not in exists_nor_names}
    if not query_nor_names:
        print('All corresponding names have at least 1 coemission ref.')
        return
    print(query_nor_names)

    ### 3. insert carbon_emission
    insert_values = [(
        'TBA', name, None, None, None, None, 'pending'
    )
    for name in query_nor_names]
    sql_ins = """
    INSERT INTO `carbon_emission`
      (`coe_source`,`nor_ingredient_name`,`publish_time`,`crawl_time`,`coe_category`,`weight_g2g`,`coe_status`)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
    nor_ingredient_name = nor_ingredient_name;
    """
    with conn.cursor() as cur:                        
        cur.executemany(sql_ins, insert_values)
        conn.commit()


def register_recipe_ingredient(conn, recipe_json: list[dict]):
    """
    1) 將此次食譜的食材寫入 recipe_ingredient
       PK = (recipe_id, recipe_site, ori_ingredient_id)
    """

    ### get ingredient_id
    ori_names = {row["ingredient_name"] for row in recipe_json if row.get("ingredient_name")}
    if not ori_names:
        print('No valid ingredients received')
        return
    
    names_to_id = {}
    with conn.cursor() as cur:
        sql = f"""
            SELECT ori_ingredient_id, ori_ingredient_name
            FROM ingredient_normalize 
            WHERE ori_ingredient_name IN ({get_where_in_string(ori_names)})
            """
        cur.execute(sql)
        for row in cur.fetchall():
            # DictCursor: row["ori_ingredient_id"]；Tuple: row[0]
            ori_id = row["ori_ingredient_id"] if isinstance(row, dict) else row[0]
            ori_name = row["ori_ingredient_name"] if isinstance(row, dict) else row[1]
            names_to_id[ori_name] = ori_id    
    
    ### Insert (Upsert)
    sql = """
    INSERT INTO `recipe_ingredient`
      (`recipe_id`, `recipe_site`, `ori_ingredient_id`, `ori_ingredient_name`, `ingredient_type`, `unit_name`, `unit_value`)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      `ingredient_type` = VALUES(`ingredient_type`),
      `unit_name`       = VALUES(`unit_name`),
      `unit_value`      = VALUES(`unit_value`);
    """
    ins_values = [(
        row['recipe_id'],
        row['recipe_site'],
        names_to_id[row['ingredient_name']],
        row['ingredient_name'],
        row['ingredient_type'],
        row['weight_unit'],
        row['weight_value'],
    ) for row in recipe_json if row.get("ingredient_name")]
    with conn.cursor() as cur:
        cur.executemany(sql, ins_values)
        print(f"total rows to insert: {len(ins_values)}; affected rows: {cur.rowcount}")
        conn.commit()


def register_coemission(conn, coe_json: list[dict]):
    """
    TBA
    """

    ### check
    if not coe_json:
        print('No valid data received')
        return

    ### Insert (Upsert)
    sql = """
    INSERT INTO `carbon_emission`
      (`coe_source`,`nor_ingredient_name`,`ref_ingredient_name`,`publish_time`,
       `crawl_time`,`coe_category`,`weight_g2g`,`coe_status`)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      `ref_ingredient_name` = VALUES(`ref_ingredient_name`),
      `publish_time` = VALUES(`publish_time`),
      `crawl_time`   = VALUES(`crawl_time`),
      `coe_category`  = VALUES(`coe_category`),
      `weight_g2g`  = VALUES(`weight_g2g`),
      `coe_status`  = VALUES(`coe_status`);
    """
    ins_values = [(
        row['coe_source'],
        row['nor_ingredient_name'],
        row['ref_ingredient_name'],
        row['publish_time'],
        row['crawl_time'],
        row['coe_category'],
        row['weight_g2g'],
        'done',
    ) for row in coe_json if row.get("nor_ingredient_name")]
    print(ins_values[:3])
    
    with conn.cursor() as cur:
        cur.executemany(sql, ins_values)
        conn.commit()


### Update Data ------------------------------------------------------------
def update_ingredient_w_normalize(conn):
    """
    Update ingredient's normalized name

    Backlog
    - make it batch update
    """

    ### 1. get records to be updated (status = 'pending')
    with conn.cursor() as cur:
        sql ="""
        select ori_ingredient_id, ori_ingredient_name
        from ingredient_normalize
        where normalize_status='pending'
        """
        cur.execute(sql)
        rows = cur.fetchall()
        if not rows:
            print("No pending ingredient to normalize.")
            return
        print(f"Found {len(rows)} pending rows")

    ### 2. normalize
    update_values = []
    for row in rows:
        normalized = get_normalize(row["ori_ingredient_name"])
        if normalized is None:
            new_status = "error"
        else:
            new_status = "done"
        update_values.append(
            (normalized, new_status, row["ori_ingredient_id"])
        )

    ### 3. update
    with conn.cursor() as cur:
        sql ="""
        UPDATE ingredient_normalize
        SET nor_ingredient_name = %s,
            normalize_status = %s
        WHERE ori_ingredient_id = %s;
        """
        cur.executemany(sql,update_values)
        print(f"total rows to update: {len(update_values)}; affected rows: {cur.rowcount}")
        conn.commit()


def update_unit_w_u2g(conn):
    """
    Update unit_normalize's corresponding grams
    """   

    ### 1. get records to be updated (status = 'pending')
    with conn.cursor() as cur:
        sql ="""
        select ori_ingredient_id, ori_ingredient_name, unit_name
        from unit_normalize
        where u2g_status='pending'
        """
        cur.execute(sql)
        rows = cur.fetchall()
        if not rows:
            print("No pending ingr-unit pair to normalize.")
            return
        print(f"Found {len(rows)} pending rows")
    
    ### 2. ingredient_name-unit pair to gram (u2g)
    unit_pair_to_query = [{'name':row.get("ori_ingredient_name"),'unit':row.get("unit_name")} for row in rows]
    unit_pair_res = query_unit2gram(unit_pair_to_query)
    print(unit_pair_res)
    unit_pair_to_g_map = {(r.get('name'), r.get('unit')):r.get('g_per_unit') for r in unit_pair_res} # dict[(pair):value]
    update_values = []
    for row in rows:
        u2g_values = unit_pair_to_g_map.get((row.get('ori_ingredient_name'),row.get('unit_name')))
        if u2g_values is None:
            new_status = "error"
        else:
            new_status = "done"
        update_values.append(
            (u2g_values, new_status, row["ori_ingredient_id"],row["unit_name"])
        )
    
    ### 3. update
    with conn.cursor() as cur:
        sql ="""
        UPDATE unit_normalize
        SET weight_grams = %s,
            u2g_status = %s
        WHERE ori_ingredient_id = %s and unit_name = %s;
        """
        cur.executemany(sql,update_values)
        print(f"total rows to update: {len(update_values)}; affected rows: {cur.rowcount}")
        conn.commit()


def update_coemission_w_query(conn):
    """
    Update (normalized) ingredient's coemission
    - by crawl data from carboncloud
    """

    ### 1. get records to be updated (status = 'pending')
    with conn.cursor() as cur:
        sql ="""
        select coe_source, nor_ingredient_name
        from carbon_emission
        where coe_status='pending' and coe_source='TBA';
        """
        cur.execute(sql)
        rows = cur.fetchall()
        if not rows:
            print("No pending ingredient to normalize.")
            return
        print(f"Found {len(rows)} pending rows")
    
    ### 2. query coemission
    update_values = []
    for row in rows:
        query_ingr_name = row["nor_ingredient_name"]
        coe_values = query_coemission([query_ingr_name])
        print(coe_values)
        update_values.append((
            'carboncloud',
            coe_values.get(query_ingr_name,{}).get('eng_query'),
            None,
            coe_values.get(query_ingr_name,{}).get('coe_values'),
            dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            coe_values.get(query_ingr_name,{}).get('coe_status','error'),
            query_ingr_name,
        ))

    ### 3. update
    with conn.cursor() as cur:
        sql ="""
        UPDATE carbon_emission
        SET coe_source = %s,
            ref_ingredient_name = %s,
            coe_category = %s,
            weight_g2g = %s,
            crawl_time = %s,
            coe_status = %s
        WHERE nor_ingredient_name = %s and coe_source='TBA';
        """
        cur.executemany(sql,update_values)
        print(f"total rows to update: {len(update_values)}; affected rows: {cur.rowcount}")
        conn.commit()