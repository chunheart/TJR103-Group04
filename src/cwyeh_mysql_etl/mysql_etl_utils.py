import random
import json
import math
import time

import numpy as np
import pandas as pd
import datetime as dt
import pymysql
import requests

import cwyeh_coemission.carboncloud_crawler as cbc
import cwyeh_coemission.myemission_crawler as mec


"""
Utils for mysql ETL, CRUD.
- including tools for develop and testing, helps functions
"""

# GLOBALs ------------------------------------------------------------
TRANS_API_KEY = os.getenv("MY_VAR")


### Helpers
def get_normalize(ori_ingredient_name: str) -> str:
    """回傳正規化後的食材名稱（暫時直接回原名或做簡單標準化）"""
    return ori_ingredient_name[-2:]

def query_unit2gram(ori_ingredient_name: str, unit_name: str) -> float | None:
    """查詢該食材 + 單位對應的克重；查不到回 None"""    
    return random.uniform(0,200)

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

def query_coemission(nor_ingredient_name: str) -> dict | None:
    """
    查詢碳排係數，回 dict 或 None。
    範例回傳結構：{'coe_source': 'FAO', 'coe_category': 'Vegetable', 'weight_g2g': 0.25}
    """
    demo = {
        "黑豆": {"coe_source": "FAO", "coe_category": "Legume", "weight_g2g": 0.6},
        "水":   {"coe_source": "FAO", "coe_category": "Water",  "weight_g2g": 0.0},
    }
    return demo.get(nor_ingredient_name)

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
        host,port,user,password,
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
        PASSWORD = "pas4word"
    """
    # === 嘗試連線 ===
    conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            charset="utf8mb4",
            connect_timeout=5,
            cursorclass=pymysql.cursors.DictCursor,
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
):
    """
    Initialize of tables in assigned DB
    - used for init and create some demo records to play with

    TBA
    - protection with user_permission !?
    """

    # 建立資料庫
    cursor = conn.cursor()
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
    conn.select_db(db_name)

    # drop_tables_if_exists
    tables = ["recipe_ingredient", "unit_normalize", "ingredient_normalize", "carbon_emission", "recipe"]
    if drop_tables_if_exists:
        for t in tables:
            cursor.execute(f"DROP TABLE IF EXISTS `{t}`;")
    
    # create tables (DDL)
    # UNIQUE KEY `uniq_ori_ingredient_name` (`ori_ingredient_name`)
    ddls = {
        "recipe": """
            CREATE TABLE IF NOT EXISTS `recipe` (
                `recipe_id`      VARCHAR(64)  NOT NULL,
                `recipe_site`    VARCHAR(100) NOT NULL,
                `recipe_name`    VARCHAR(255),
                `recipe_url`  VARCHAR(255),
                `author`         VARCHAR(100),
                `servings`       INT,
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

    ###
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


### Get Data ------------------------------------------------------------
def get_recipe_data(
    tar_date,
    back_days,
    source='demo',
    if_insert=False,
    kafka_consumer=False
) -> list:
    """
    get recipe data from several available source
        - some basic cleaning: uncapitalize, strip

    INPUTs
    -----------
    tar_date
    back_days
    source
      demo: return demo records
      kafka: get data from a kafka consumer-client [require a kafka producer]
    """
    if source=='demo':
        demo_recipe = [
          {
            "recipe_id": "F05-7777",
            "recipe_site": "icook",
            "recipe_name": "番茄炒蛋C",
            "recipe_url": "https://www.ytower.com.tw/recipe/iframe-recipe.asp?seq=F05-0228",
            "author": "Cathy",
            "servings": 2,
            "cook_time": None,
            "ingredient_type": "食材",
            "ingredient_name": "小番茄",
            "weight_value": 50,
            "weight_unit": "公克",
            "publish_date": "2007-12-13",
            "crawl_time": "2025-10-19 10:11:21",
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
            "publish_date": "2007-12-13",
            "crawl_time": "2025-10-19 10:11:21",
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
            "weight_value": 6,
            "weight_unit": "公克",
            "publish_date": "2007-12-13",
            "crawl_time": "2025-10-19 10:11:21",
          },
        ]
        return demo_recipe
        
    if source=='kafka':
        pass


def get_coemission_data(source='myemission'):
    """
    get and clean data from several carbon emission site
    - this is likely to be an one-time task
    """
    if source=='myemission':

        ### Crawl data
        myemission_df = mec.main_init(ingr_batch_size=20,if_write=False)
        print('Crawl Done')
        
        ### batch translate (EN -> zh-TW)
        # - should I also check normalize?
        size = myemission_df.shape[0]
        batch_size = 20
        batch = math.ceil(size/batch_size)
        trans_name = []
        for b in range(batch):
            qlist = list(myemission_df['name'].iloc[b*batch_size:(b+1)*batch_size])
            res_list = translate(
                qlist=qlist,
                key=TRANS_API_KEY,
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
        myemission_df = myemission_df.rename(columns = {'category':'coe_category','emissions':'weight_g2g'})
        myemission_df = myemission_df[['coe_source','nor_ingredient_name','publish_time','crawl_time','coe_category','weight_g2g']]
        print('Clean Done')
        
        return json.loads(myemission_df.to_json(orient='records'))


def register_recipe(conn, recipe_json: dict):
    """
    1) 取出 recipe 相關的欄位、去重複
    2) 寫入 recipe
        使用 upsert：若已有相同 recipe_id x recipe site (PK)，則更新欄位 (ON DUPLICATE clause)
    """

    ### Get unique recipe level data and relavant columns
    # empty list = False
    if not recipe_json:
        print('Input data not exist')
        return

    # 確保必要欄位存在
    need_cols = [
        "recipe_id", "recipe_site", "recipe_name", "recipe_url",
        "author", "servings", "publish_date", "crawl_time"
    ]
    df = pd.DataFrame(recipe_json)
    for c in need_cols:
        if c not in df.columns:
            df[c] = None
    df_unique = df.drop_duplicates(subset=["recipe_id", "recipe_site"], keep="first")
    recipe_values = [
        (
            rec["recipe_id"],
            rec["recipe_site"],
            rec["recipe_name"],
            rec["recipe_url"],
            rec["author"],
            rec["servings"],
            rec["publish_date"],
            rec["crawl_time"],
        )
        for _, rec in df_unique.iterrows()
    ]
    
    ### Insert
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
        conn.commit()


def register_ingredient(conn, recipe_json: list[dict]):
    """
    1) 針對此次食譜的「所有食材列」：
       - 找出 ingredient_normalize 還不存在的 ori_ingredient_name
       - 呼叫 get_normalize() 取得 nor_ingredient_name
       - insert 進 ingredient_normalize

    Backlog
    - 也可以用 upsert，但就要做多餘的 get_normalize()
    """
    
    ### 收集此次出現的原始食材名（去重）
    ori_names = {row["ingredient_name"] for row in recipe_json if row.get("ingredient_name")}
    if not ori_names:
        print('No valid ingredients received')
        return

    ### 找出 input_data 中還未存在於 ingredient_normalize table
    with conn.cursor() as cur:
        q = "SELECT ori_ingredient_name FROM ingredient_normalize WHERE ori_ingredient_name IN %s"
        cur.execute(q, (tuple(ori_names),))
        exists = {r["ori_ingredient_name"] for r in cur.fetchall()}
        
    to_insert = [name for name in ori_names if name not in exists]
    print(f'{len(to_insert)} (ori) ingredients to be addes')
    if not to_insert:
        print('No unexisting ingridient')
        return

    ### 準備 insert 的資料與模板
    sql = """
    INSERT INTO `ingredient_normalize`
      (`ori_ingredient_name`, `nor_ingredient_name`,`normalize_status`)
    VALUES (%s, %s, %s);
    """
    to_insert = [(ori_ingr, get_normalize(ori_ingr),'done') for ori_ingr in to_insert]
    
    ### Upsert
    with conn.cursor() as cur:
        cur.executemany(sql, to_insert)
        conn.commit()


def register_unit(conn, recipe_json: list[dict]):
    """
    3) unit_normalize upsert
       - 查是否已存在 (ori_ingredient_id, unit_name) 或用 ori_ingredient_name + unit_name 推導
       - 若沒有 weight_grams，呼叫 query_unit2gram()
    """
    
    ### Find unexistinf unit pairs
    ingr_unit_pairs = {(row["ingredient_name"],row['weight_unit']) 
                       for row in recipe_json 
                       if row.get("ingredient_name") and row.get("weight_unit")}
    if not ingr_unit_pairs:
        print('No valid data received')
        return
            
    with conn.cursor() as cur:
        sql = f"""
            SELECT ori_ingredient_name, unit_name
            FROM unit_normalize 
            WHERE (ori_ingredient_name,unit_name) IN ({get_where_in_string(ingr_unit_pairs,warp_with_string=False)})
            """
        cur.execute(sql)
        exists = {(r["ori_ingredient_name"],r['unit_name']) for r in cur.fetchall()}
    insert_ingr_unit_pairs = [p for p in ingr_unit_pairs if p not in exists]
    if not insert_ingr_unit_pairs:
        print('No unexisting ingridient-unit pair')
        return
    
    ### Query ingredient id (from ingredient_normalize)
    names = sorted({name for name, _ in insert_ingr_unit_pairs})
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

    ### Query unit2gram & Insert values
    # - query_unit2gram() - may take time, batch query?
    insert_ingr_unit_pairs = [(names_to_id[p[0]],
                              p[0],
                              p[1],
                              query_unit2gram(p[0],p[1]),
                              'done') for p in insert_ingr_unit_pairs]

    ### Insert to unit_normalize
    sql_ins = """
    INSERT INTO `unit_normalize`
      (`ori_ingredient_id`, `ori_ingredient_name`, `unit_name`, `weight_grams`,`u2g_status`)
    VALUES (%s, %s, %s, %s, %s)
    """
    with conn.cursor() as cur:
        cur.executemany(sql_ins,insert_ingr_unit_pairs)
        conn.commit()


def register_coemission_from_recipe(conn, recipe_json: list[dict]):
    """
    1) carbon_emission upsert：
       - 以正規化名稱為 key 檢查是否存在
       - 若沒有則 translate() + query_coemission() 後寫入
       Schema 調整：使用 id 作 PK + nor_ingredient_name UNIQUE
    """
    
    ### 正規化名稱 from ingredients
    # call get_normalize() or query DB
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
    
    ### check nor_names in carbon_emission
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
        print('No unexisting nor_ingredient in carbon_emission')
        return

    ### query carbon_emission
    # -- delay and deal with this later
    ins_nor_names = [('carboncloud',name,None,None,None,None,'pending') for name in query_nor_names]

    ### insert carbon_emission
    sql_ins = """
    INSERT INTO `carbon_emission`
      (`coe_source`,`nor_ingredient_name`,`publish_time`,`crawl_time`,`coe_category`,`weight_g2g`,`coe_status`)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    with conn.cursor() as cur:                        
        cur.executemany(sql_ins, ins_nor_names)
        conn.commit()


def register_recipe_ingredient(conn, recipe_json: list[dict]):
    """
    5) 將此次食譜的食材寫入 recipe_ingredient
       PK = (recipe_id, ori_ingredient_id)
       這裡同樣用「名稱 hash → ori_ingredient_id」做暫時 id
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
        print(sql)
        cur.execute(sql)
        for row in cur.fetchall():
            # DictCursor: row["ori_ingredient_id"]；Tuple: row[0]
            ori_id = row["ori_ingredient_id"] if isinstance(row, dict) else row[0]
            ori_name = row["ori_ingredient_name"] if isinstance(row, dict) else row[1]
            names_to_id[ori_name] = ori_id
    print(names_to_id)
    
    
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
      (`coe_source`,`nor_ingredient_name`,`publish_time`,`crawl_time`,`coe_category`,`weight_g2g`,`coe_status`)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      `publish_time` = VALUES(`publish_time`),
      `crawl_time`   = VALUES(`crawl_time`),
      `coe_category`  = VALUES(`coe_category`),
      `weight_g2g`  = VALUES(`weight_g2g`),
      `coe_status`  = VALUES(`coe_status`);
    """
    ins_values = [(
        row['coe_source'],
        row['nor_ingredient_name'],
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