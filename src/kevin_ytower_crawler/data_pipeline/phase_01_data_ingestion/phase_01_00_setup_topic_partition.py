# 檔案路徑: src/kevin_ytower_crawler/data_pipeline/phase_01_00_setup_topic_partition.py

import os
from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions
from confluent_kafka import KafkaError

# --- 1. Kafka Topic 設定 ---
# 
# 在這裡「寫死」 Kafka Topic 設定
# 這是 YTower 爬蟲資料專屬的 Topic 名稱
YTOWER_TOPIC_NAME = "ytower_recipes_raw"
# 這個 Topic 要有幾個 Partition (分割區)，建議 2-4 個
YTOWER_TOPIC_PARTITIONS_NUM = 2
# ---------------------------------


def setup_kafka_topics(admin, desired_topics_config):
    """
    函式用來自動建立或更新 Kafka Topics。
    
    參數:
    - admin: Kafka AdminClient 的實例
    - desired_topics_config: 一個字典 {'topic_name': 想要的 partition 數量}
    """

    # --- A. 取得 "目前" Cluster 上的所有 Topic 狀態 ---
    try:
        cluster_metadata = admin.list_topics(timeout=10)
        current_topics = cluster_metadata.topics
        print("--- 已取得目前 Kafka Cluster 上的 Topic 列表 ---")
    except Exception as err:
        print(f"無法取得 Topic 列表: {err}")
        return

    # --- B. 比較「想要的」和「目前的」，準備好要執行的動作 ---
    topics_to_create = []       # 存放要建立的 Topic
    partitions_to_increase = [] # 存放要增加 Partition 的 Topic

    # 本機測試 (單節點)，副本數(Replication Factor)只能設為 1
    default_replication_factor = 1

    for topic_name, desired_partitions in desired_topics_config.items():

        if topic_name not in current_topics:
            # 情況 1: Topic 不存在 -> 準備建立
            print(f"[*] 偵測到 Topic '{topic_name}' 不存在，將排程建立 (Partitions={desired_partitions})")
            new_topic = NewTopic(
                topic_name,
                num_partitions=desired_partitions,
                replication_factor=default_replication_factor
            )
            topics_to_create.append(new_topic)

        else:
            # 情況 2: Topic 已存在 -> 檢查 Partition 數量
            current_partitions = len(current_topics[topic_name].partitions)

            if current_partitions < desired_partitions:
                # 情況 2a: 數量不足 -> 準備增加
                print(f"[*] 偵測到 Topic '{topic_name}' 數量不足 ({current_partitions} < {desired_partitions})，將排程增加")
                new_parts = NewPartitions(topic_name, desired_partitions)
                partitions_to_increase.append(new_parts)

            elif current_partitions > desired_partitions:
                # 情況 2b: 數量過多
                print(f"[!] 警告: Topic '{topic_name}' 數量 ({current_partitions}) 多於目標 ({desired_partitions})。Kafka 不允許減少 Partition。")

            else:
                # 情況 2c: 數量剛好
                print(f"[✓] Topic '{topic_name}' 狀態正確 (Partitions={current_partitions})")

    # --- C. 實際執行動作 ---
    if topics_to_create:
        print("\n--- 正在執行 建立 Topic(s) ---")
        fs = admin.create_topics(topics_to_create)
        for topic, f in fs.items():
            try:
                f.result() # 等待建立完成
                print(f"  [✓] 成功建立 Topic: {topic}")
            except Exception as err:
                # 處理 race condition (多個程式同時嘗試建立)
                if err.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                    print(f"  [!] Topic {topic} 已存在。")
                else:
                    print(f"  [X] 建立 {topic} 失敗: {err}")

    if partitions_to_increase:
        print("\n--- 正在執行 增加 Partition(s) ---")
        fs = admin.create_partitions(partitions_to_increase)
        for topic, f in fs.items():
            try:
                f.result() # 等待增加完成
                print(f"  [✓] 成功增加 Partition: {topic}")
            except Exception as err:
                print(f"  [X] 增加 {topic} 失敗: {err}")

    print("\n--- Kafka Topic 維護作業完成 ---")


# --- 主程式執行區 ---
if __name__ == "__main__":

    # 1. 最終目標：我們要讓 Kafka 上有這些 Topic
    desired_config = {
        YTOWER_TOPIC_NAME: int(YTOWER_TOPIC_PARTITIONS_NUM)
    }

    # 2. Kafka Admin 連線設定 (本地端通常是 localhost:9092)
    props = {
        'bootstrap.servers': 'localhost:9092'
    }

    try:
        admin_client = AdminClient(props)
        # 3. 執行維護函式
        setup_kafka_topics(admin_client, desired_config)
    except Exception as e:
        print(f"無法連線到 Kafka AdminClient (請確認 Kafka 正在本機 localhost:9092 運行): {e}")