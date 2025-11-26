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
* airflow(py) container mount airflow/{dags,data,logs,tasks,utils} and src/
```shell
# set KEYs (for testing purpose, should be accessed via secret-manager)
# note that MYSQL_PASSWORD is likely to be saved persistently in volume
export MY_GOOGLE_TRANS_API_KEY={your key}
export MY_GEMINI_API_KEY={your key}
export MYSQL_PASSWORD={psd}
export AIRFLOW_PASSWORD={psd}

# Build airflow-python container (try to rebuild when updated)
docker build -f service/mysql_etl/airflow.Dockerfile -t py_airflow:latest . --build-arg AIRFLOW_PASSWORD=$AIRFLOW_PASSWORD

# start kafka
docker compose -f src/gina_icook_crawler/kafka/docker-compose.yml up -d

# start containers: mysql, airflow-python
# build image (py_airflow) if not existing
# [to rebuild] add --build
docker compose -f service/mysql_etl/docker-compose.yaml up -d --build

# add py_airflow into kafka's network
# (hostname the same as container name: kafka-server)
docker network connect kafka_kafka-net py_airflow

# close containers: mysql, airflow-python
docker compose -f service/mysql_etl/docker-compose.yaml down

# close kafka
docker compose -f src/gina_icook_crawler/kafka/docker-compose.yml down

# Misc
# run icook crawler
python src/gina_icook_crawler/daily.py --since "2025-11-18" --before "2025-11-18"
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

## 六、VM start-up script
```shell
#!/bin/bash
#set -euo pipefail

# ---------- System basics ----------
apt-get update -y
apt-get install -y ca-certificates curl gnupg git jq

# ---------- Docker (official repo) ----------
UBUNTU_CODENAME="$(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}")"

install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
chmod a+r /etc/apt/keyrings/docker.asc
cat >/etc/apt/sources.list.d/docker.sources <<EOF
Types: deb
URIs: https://download.docker.com/linux/ubuntu
Suites: ${UBUNTU_CODENAME}
Components: stable
Signed-By: /etc/apt/keyrings/docker.asc
EOF

apt-get update -y
apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
systemctl enable --now docker
echo "[DONE] install system and docker tools"

# ---------- App user ----------
# so that tjr103-gcp-user can use docker
# TBA: useradd 會在 user 已存在時失敗，可以改成先檢查：
#   id -u tjr103-gcp-user &>/dev/null || useradd -m tjr103-gcp-user
id -u tjr103-gcp-user &>/dev/null || useradd -m tjr103-gcp-user
usermod -aG docker tjr103-gcp-user
cd /home/tjr103-gcp-user
echo "[DONE] create users"

# ---------- Project checkout ----------
# TBA: not 777 the whole folder (changer owner as <me> and grant min permission)
git clone https://github.com/chunheart/TJR103-Group04.git
git config --global --add safe.directory /home/tjr103-gcp-user/TJR103-Group04
cd TJR103-Group04
git config core.filemode false
mkdir -p airflow/logs airflow/data airflow/utils airflow/tasks
chmod -R 777 .
echo "[DONE] clone project"

# ---------- Secrets from Secret Manager
# TBA: mount secret file instead ENV
SECRET_JSON="$(gcloud secrets versions access latest \
  --secret=coemission \
  --project="velvety-broker-476816-s9")"
export MY_GOOGLE_TRANS_API_KEY="$(echo "${SECRET_JSON}" | jq -r '.google_translate_api_key')"
export MY_GEMINI_API_KEY="$(echo "${SECRET_JSON}"       | jq -r '.gemini_api_key')"
export MYSQL_PASSWORD="$(echo "${SECRET_JSON}"       | jq -r '.mysql_password')"
export AIRFLOW_PASSWORD="$(echo "${SECRET_JSON}"       | jq -r '.airflow_password')"
echo "[DONE] get secrets"

# ---------- Bring up containers ----------
docker compose -f src/gina_icook_crawler/kafka/docker-compose.yml up -d 
docker compose -f service/mysql_etl/docker-compose.yaml up -d --build
docker network connect kafka_kafka-net py_airflow
echo "[DONE] start containers"
``` 