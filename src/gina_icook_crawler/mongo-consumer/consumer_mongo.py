# ========================================
# 📥 mongo-consumer 安全防漏防重複版
# 🌈 含時間戳記 + 彩色輸出 + 每日自動 log 檔
# ========================================

from kafka import KafkaConsumer
from pymongo import MongoClient, errors
import json
import time
import traceback
from datetime import datetime
from pathlib import Path
import os

# ===== 基本設定 =====
KAFKA_BROKER = "kafka-server:9092"
TOPIC = "icook_recipes"
MONGO_URI = "mongodb://root:root123@mongo-server:27017"
MONGO_DB = "icook"
MONGO_COLLECTION = "recipes"

# ===== Log 檔路徑設定 =====
LOG_DIR = Path("/app/logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)

def get_log_file():
    """取得當天的 log 檔案路徑"""
    today = datetime.now().strftime("%Y-%m-%d")
    return LOG_DIR / f"{today}.log"

def write_log_file(msg):
    """將 log 寫入檔案"""
    with open(get_log_file(), "a", encoding="utf-8") as f:
        f.write(msg + "\n")

# ===== 彩色輸出工具 =====
def log(msg, level="INFO"):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    colors = {
        "INFO": "\033[92m",   # 綠
        "WARN": "\033[93m",   # 黃
        "ERROR": "\033[91m",  # 紅
        "RESET": "\033[0m"
    }
    color = colors.get(level, "")
    reset = colors["RESET"]
    formatted = f"[{ts}] {msg}"
    print(f"{color}{formatted}{reset}", flush=True)
    write_log_file(formatted)

# ===== 主執行函式 =====
def main():
    log("🚀 mongo-consumer（安全版）啟動中...", "INFO")
    mongo_client = None
    consumer = None
    collection = None
    unique_field = None  # 🌟 自動偵測唯一索引欄位

    while True:
        try:
            # ---------------------------------
            # 🔌 1️⃣ 連線 MongoDB（含重試）
            # ---------------------------------
            if mongo_client is None:
                try:
                    mongo_client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
                    db = mongo_client[MONGO_DB]
                    collection = db[MONGO_COLLECTION]
                    log(f"🍃 已連線 MongoDB：{MONGO_DB}.{MONGO_COLLECTION}")
                except errors.ServerSelectionTimeoutError:
                    log("⚠️ MongoDB 連線逾時，5 秒後重試...", "WARN")
                    mongo_client = None
                    time.sleep(5)
                    continue

            # ---------------------------------
            # 🔄 2️⃣ 連線 Kafka（含重試）
            # ---------------------------------
            if consumer is None:
                try:
                    consumer = KafkaConsumer(
                        TOPIC,
                        bootstrap_servers=[KAFKA_BROKER],
                        auto_offset_reset='earliest',
                        enable_auto_commit=True,
                        group_id='mongo_consumer_group',
                        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                    )
                    log(f"🧱 已連線 Kafka topic：{TOPIC}")
                except Exception as e:
                    log(f"⚠️ Kafka 連線失敗：{e}", "WARN")
                    consumer = None
                    time.sleep(5)
                    continue

            # ---------------------------------
            # 📥 3️⃣ 監聽 Kafka 並寫入 MongoDB
            # ---------------------------------
            log("📥 開始接收 Kafka 訊息並寫入 MongoDB...\n")

            for message in consumer:
                try:
                    data = message.value

                    # 🌟 自動偵測唯一索引欄位
                    if unique_field is None:
                        if "url" in data:
                            unique_field = "url"
                        elif "title" in data:
                            unique_field = "title"
                        else:
                            unique_field = None

                        if unique_field:
                            try:
                                collection.create_index(unique_field, unique=True)
                                log(f"🔑 已建立唯一索引：{unique_field}")
                            except Exception as e:
                                log(f"⚠️ 建立索引失敗：{e}", "WARN")

                    # 🌱 寫入資料
                    collection.insert_one(data)
                    log(f"✅ 寫入成功：{data.get('title', '未命名食譜')}", "INFO")

                except errors.DuplicateKeyError:
                    log(f"⚠️ 跳過重複資料：{data.get('title', '未命名食譜')}", "WARN")
                except Exception as e:
                    log(f"❌ 寫入 MongoDB 失敗：{e}", "ERROR")
                    traceback.print_exc()

        except KeyboardInterrupt:
            log("🛑 手動中止。結束程式。", "WARN")
            break
        except Exception as e:
            log(f"⚠️ 主迴圈錯誤：{e}", "ERROR")
            traceback.print_exc()
            mongo_client = None
            consumer = None
            log("⏳ 5 秒後嘗試重新連線...", "WARN")
            time.sleep(5)

if __name__ == "__main__":
    main()

