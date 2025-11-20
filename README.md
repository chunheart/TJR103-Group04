# TJR103-Group04
第四組專題


## 🐳 一、建立與執行容器(環境設定)

```powershell
# 進入專案資料夾

cd "C:\Users\<你的名字>\TJR103-Group04"

# 建立映像檔
docker build -t icook-crawler-internal .

# 啟動容器（背景執行）
docker run -d --rm \
    --name recipe_coemission \
    -v "$PWD":/app \
    icook-crawler-internal

# 查看日誌
docker logs -f recipe_coemission
git 
```

## 二、Features
### 🍳 iCook Crawler - Docker 自動排程版 (v5A Internal)
本版本會每天 09:00 自動抓取「前一天」的 iCook 食譜資料，並將結果存放於容器內 `/app/data/`。


## 三、mysql-etl 環境設置
* 以下指令，透過 docker-compose 建立 airflow(py), mysql container
* airflow(py) container mount airflow/ 下的目錄
```shell
# Build airflow-python container (try to rebuild when updated)
docker build -f service/mysql_etl/airflow.Dockerfile -t py_airflow:latest .

# set KEYs (for testing purpose)
export MY_GOOGLE_TRANS_API_KEY = {your key}

# start kafka
docker-compose -f src/gina_icook_crawler/kafka/docker-compose.yml up -d

# start containers: mysql, airflow-python
# build image (py_airflow) if not existing
docker-compose -f service/mysql_etl/docker-compose.yaml up -d

# add py_airflow into kafka's network
# (hostname the same as container name: kafka-server)
docker network connect kafka_kafka-net py_airflow

# close containers: mysql, airflow-python
docker-compose -f service/mysql_etl/docker-compose.yaml down

# close kafka
docker-compose -f src/gina_icook_crawler/kafka/docker-compose.yml down
```

## 四、Ytower Crawler (楊桃美食網爬蟲)
本模組負責抓取楊桃美食網的食譜資料，並進行初步欄位清洗。
```shell
# 進入專案根目錄
# 執行爬蟲主程式
poetry run python3 src/kevin_ytower_crawler/main.py

# 輸出結果
# 檔案位於: src/kevin_ytower_crawler/ytower_csv_output/ytower_all_recipes.csv
```

## 五、食材單位正規化 (Food Unit Normalization)
透過規則庫與 Google Gemini AI，將非標準單位（如：1條、少許）轉換為標準公克數 (g)。
```shell
# 前置作業：
# 請確認 src/kevin_food_unit_normalization/main.py 內已填入 API Key

# 執行正規化轉換 (自動讀取上一步驟產生的 CSV)
poetry run python3 src/kevin_food_unit_normalization/main.py

# 輸出結果 (包含 Normalized_Weight_g 欄位)
# 檔案位於: src/kevin_ytower_crawler/ytower_csv_output/ytower_recipes_normalized.csv
```
