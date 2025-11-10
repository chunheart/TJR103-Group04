import os
from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions
from confluent_kafka import KafkaError
from dotenv import load_dotenv

load_dotenv()
TOPIC_NAME_1 = os.getenv("TOPIC_NAME_1")
TOPIC_NAME_1_PARTITIONS_NUM = os.getenv("TOPIC_NAME_1_PARTITION_NUM")
TOPIC_NAME_2 = os.getenv("TOPIC_NAME_2")
TOPIC_NAME_2_PARTITIONS_NUM = os.getenv("TOPIC_NAME_2_PARTITION_NUM")


def setup_kafka_topics(admin, desired_topics_config):
    """
    一個健壯的函式，用來建立或更新 Topic。
    desired_topics_config = {'topic_name': num_partitions}
    """

    # --- 1. 取得 "目前" Cluster 上的所有 Topic 狀態 ---
    try:
        # get metadata from the cluster of brokers
        cluster_metadata = admin.list_topics(timeout=10)
        current_topics = cluster_metadata.topics
        print("--- 已取得目前 Topic 列表 ---")
    except Exception as err:
        print(f"無法取得 Topic 列表: {err}")
        return

    # --- 2. 準備 "要執行的動作" (建立或增加) ---
    topics_to_create = []
    partitions_to_increase = []

    # (重要!) 本機測試，副本數(Replication)預設為 1
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
                print(
                    f"[*] 偵測到 Topic '{topic_name}' 數量不足 ({current_partitions} < {desired_partitions})，將排程增加")
                new_parts = NewPartitions(topic_name, desired_partitions)
                partitions_to_increase.append(new_parts)

            elif current_partitions > desired_partitions:
                # 情況 2b: 數量過多
                print(
                    f"[!] 警告: Topic '{topic_name}' 數量 ({current_partitions}) 多於目標 ({desired_partitions})。Kafka 不允許減少 Partition。")

            else:
                # 情況 2c: 數量剛好
                print(f"[✓] Topic '{topic_name}' 狀態正確 (Partitions={current_partitions})")

    # --- 3. 執行動作 ---

    # (執行 建立)
    if topics_to_create:
        print("\n--- 正在執行 建立 Topic(s) ---")
        fs = admin.create_topics(topics_to_create)
        for topic, f in fs.items():
            try:
                f.result()
                print(f"  [✓] 成功建立 Topic: {topic}")
            except Exception as err:
                if err.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                    print(f"  [!] Topic {topic} 在檢查後又被建立了，沒關係。")
                else:
                    print(f"  [X] 建立 {topic} 失敗: {err}")

    # (執行 增加)
    if partitions_to_increase:
        print("\n--- 正在執行 增加 Partition(s) ---")
        fs = admin.create_partitions(partitions_to_increase)
        for topic, f in fs.items():
            try:
                f.result()
                print(f"  [✓] S成功增加 Partition: {topic}")
            except Exception as err:
                print(f"  [X] 增加 {topic} 失敗: {err}")

    print("\n--- Kafka Topic 維護作業完成 ---")


if __name__ == "__main__":

    # --- 1. 設定你的「最終目標」 ---
    # (你想讓 Cluster 最終長什麼樣子)
    desired_config = {
        TOPIC_NAME_1: int(TOPIC_NAME_1_PARTITIONS_NUM),  # 63萬筆的 Topic，要有 4 個 partition
        TOPIC_NAME_2: int(TOPIC_NAME_2_PARTITIONS_NUM)  # 22萬筆的 Topic，要有 2 個 partition
    }

    # --- 2. 設定 AdminClient ---
    props = {
        'bootstrap.servers': 'localhost:9092'
    }

    try:
        admin_client = AdminClient(props)
        # --- 3. 執行維護 ---
        setup_kafka_topics(admin_client, desired_config)

    except Exception as e:
        print(f"無法連線到 AdminClient: {e}")