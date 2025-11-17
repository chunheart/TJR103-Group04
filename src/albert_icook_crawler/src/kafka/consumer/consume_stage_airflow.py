import json
import logging, os, sys
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from confluent_kafka import Consumer, KafkaException, KafkaError
from src.kafka.utils.mongodb_connection import connect_to_local_mongodb

PROJECT_ROOT = Path(__file__).resolve().parents[3] # root directory
ENV_FILE_PATH = PROJECT_ROOT / "kafka" /".env"
load_dotenv(ENV_FILE_PATH)

LOG_DIR_CONSUME = PROJECT_ROOT / "logs" / "kafka" / "consume"
TOPIC_NAME = os.getenv("KAFKA_TOPIC_1")
DATABASE = os.getenv("DATABASE_1")
COLLECTION = os.getenv("COLLECTION_STAGE_1")
GROUP_ID = os.getenv("GROUP_ID_STAGE_1")
MAX_EMPTY_FETCHES = 20


def get_log_file():
    return LOG_DIR_CONSUME / f"produce_{datetime.today().date()}.log"

def set_log_config(log_file):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8", mode="a"),
            logging.StreamHandler(sys.stdout)  # 同時輸出到 console（方便在 Airflow log 看）
        ],
    )

def create_consumer(group_id, topic_name, logger):
    # 步驟1.設定要連線到Kafka集群的相關設定
    # Consumer configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    props = {
        'bootstrap.servers': 'localhost:9092',  # Kafka集群在那裡?
        'group.id': group_id,  # ConsumerGroup的名稱
        'auto.offset.reset': 'earliest',  # 是否從這個ConsumerGroup尚未讀取的partition/offset開始讀
        'enable.auto.commit': False,  # 是否啟動自動commit
        'on_commit': lambda err, partitions: print_commit_result(err, partitions, logger),  # 設定接收commit訊息的callback函數
        'error_cb': lambda err: error_cb(err, logger)  # 設定接收error訊息的callback函數
    }

    # 步驟2. 產生一個Kafka的Consumer的實例
    consumer = Consumer(props)
    # 步驟3. 指定想要訂閱訊息的topic名稱
    topic_name = topic_name
    # 步驟4. 讓Consumer向Kafka集群訂閱指定的topic
    consumer.subscribe([topic_name])
    return consumer

def error_cb(err, logger):
    logger.info('Error: %s' % err)

def print_commit_result(err, partitions, logger):
    if err:
        logger.info('# Failed to commit offsets: %s: %s' % (err, partitions))
    else:
        for p in partitions:
            logger.info(
                'Committed offsets: topic: {}, partition: {}. offset: {}'.format(
                    p.topic, p.partition, p.offset
                )
            )

def consume_raw_data_to_mongodb(consumer, logger):
    """
    retrieve data from kafka topic and insert into mongodb
    return: list of dict (data to insert)
    """
    empty_fetch_count = 0
    max_empty_fetches = MAX_EMPTY_FETCHES

    try:
        while True:
            records = consumer.consume(num_messages=100, timeout=1)  # read 100 records once
            if not records: # check if records are empty
                empty_fetch_count += 1
                logger.info(f"No messages received. Empty fetch count: {empty_fetch_count}/{MAX_EMPTY_FETCHES}")
                if empty_fetch_count >= max_empty_fetches:
                    logger.info("Reached max empty fetches. Assuming topic is clear. Stopping consumer.")
                    break
                else:
                    continue

            empty_fetch_count = 0

            insert_buffer = []

            for record in records:
                if record is None: # avoid empty records
                    continue
                if record.error():
                    # Error or event
                    if record.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        logger.info(
                            '%% {} [{}] reached end at offset {}\n'.format(
                                record.topic(),
                                record.partition(),
                                record.offset()
                            )
                        )

                    else:
                        raise KafkaException(record.error())
                    continue

                # ** 在這裡進行商業邏輯與訊息處理 **
                # get metadata
                topic = record.topic()
                partition = record.partition()
                offset = record.offset()
                try:
                    msg_value = try_decode_utf8(record.value())
                    document = json.loads(msg_value)
                except Exception as e:
                    print(f"Error : {e}")
                    continue

                document["_id"] = f"{topic}-{partition}-{offset}"
                document["_kafka_meta_topic"] = topic
                document["_kafka_meta_partition"] = partition
                document["_kafka_meta_offset"] = offset

                insert_buffer.append(document)

            if insert_buffer:
                yield insert_buffer
                insert_buffer.clear()

    except Exception as e:
        logger.info(str(e))

def try_decode_utf8(data):
    if data:
        return data.decode('utf-8')
    else:
        return None

def consume_messages():
    f"""
    main function
    Retrieve data from Kafka.
    Topic: {TOPIC_NAME}
    GroupId: {GROUP_ID} 
    """

    ### Create log file
    log_producer_dir = LOG_DIR_CONSUME
    log_producer_dir.mkdir(parents=True, exist_ok=True)
    log_file = get_log_file()

    set_log_config(log_file)

    logger = logging.getLogger(__name__)
    logger.info(f"Start to process consuming from Kafka topic {TOPIC_NAME}")

    ### Create connection to MongoDB
    client = connect_to_local_mongodb()
    db = client[DATABASE]
    collection = db[COLLECTION]

    ### Create a consumer
    consumer = create_consumer(GROUP_ID, TOPIC_NAME, logger)

    ### Retrieve data form Kafka
    data_stream = consume_raw_data_to_mongodb(consumer, logger)

    try:
        print("start to retrieve data from kafka and load into mongodb")
        for data in data_stream:
            if not data:
                continue
            try:
                collection.insert_many(data, ordered=False)
                logger.info(f"{len(data)} documents inserted into mongodb")
                consumer.commit(asynchronous=False)
                logger.info(f"Offsets committed for {len(data)} records.")
            except Exception as e:
                logger.info(f"{e}")
    except KeyboardInterrupt:
        logger.info('Aborted by user\n')
    finally:
        client.close()
        logger.info(f"MongoDB connection has disconnected")
        consumer.close()
        logger.info(f"Kafka consumer connection has disconnected")


if __name__ == "__main__":
    # main()
    print(PROJECT_ROOT)