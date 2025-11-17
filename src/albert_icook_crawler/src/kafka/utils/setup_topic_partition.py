from confluent_kafka.admin import AdminClient, NewTopic, NewPartitions
from confluent_kafka import KafkaError
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[3] # root directory
ENV_FILE_PATH = PROJECT_ROOT / "kafka" / ".env"


def setup_kafka_topics(admin, desired_topics_config, replication=1):
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

    # Default number of replication is one
    default_replication_factor = replication

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

    # create topic
    if topics_to_create:
        print("\n--- 正在執行 建立 Topic(s) ---")
        fs = admin.create_topics(topics_to_create)
        for topic, f in fs.items():
            try:
                f.result()
                # ready to write the topic into .env
                value_to_env = [topic]
                start_index = 1
                # check if .env is empty
                if ENV_FILE_PATH.exists() and ENV_FILE_PATH.stat().st_size > 0:
                    lines = ENV_FILE_PATH.read_text(encoding="utf-8").splitlines()
                    lines = [line for line in lines if line.strip()]  # remove the blank lines
                    if lines:
                        last_line = lines[-1]
                        if last_line.startswith("KAFKA_TOPIC_"):
                            last_key = last_line.split("=")[0] # get "TOPIC_3"
                            last_num = int(last_key.split("_")[-1]) # get 3
                            start_index = last_num + 1
                result = []
                with open(file=ENV_FILE_PATH, mode="a", encoding="utf-8") as file:
                    for i, value in enumerate(value_to_env, start=start_index):
                        result.append(f"TOPIC_{i}={value}")

                    result = "\n".join(result)

                    if start_index == 1: # empty file
                        file.write(result)
                    else: # not empty file
                        file.write("\n"+result)
                print(f"  [✓] 成功將 TOPIC_{i}={value} 寫入.env")
                print(f"  [✓] 成功建立 Topic: {topic}")

            except (OSError, ValueError, IndexError) as err:
                print(f"Error: {err}")

            except Exception as err:
                if err.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                    print(f"  [!] Topic {topic} 在檢查後又被建立了，沒關係。")
                else:
                    print(f"  [X] 建立 {topic} 失敗: {err}")


    # create partitions
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

def main():
    ### 1. Create topic & partition
    desired_config = {}
    while True:
        topic = input("Topic name: ")
        if topic == "":
            print("Topic cannot be empty!")
            print("=" * 40)
        elif topic in desired_config:
            print("Topic already exists!")
            print("=" * 40)
        elif topic == "exit":
            break
        partitions = int(input("Partitions(default=1): "))
        desired_config[topic] = partitions
        print(f"{topic} and {partitions} have been listed to be created")
        print("You can add another topic with partition or enter exit to stop the current process")
        print("=" * 40)
    # --- 2. Set up AdminClient ---
    props = {
        'bootstrap.servers': 'localhost:9092'
    }
    print("set up your connect to kafka via the port 9092")
    try:
        admin_client = AdminClient(props)
        # --- 3. 執行維護 ---
        setup_kafka_topics(admin_client, desired_config)

    except Exception as e:
        print(f"AdminClient cannot be accessed to: {e}")


if __name__ == "__main__":
    # main()
    print(PROJECT_ROOT)