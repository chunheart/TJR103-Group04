import json
import logging, os, sys
from datetime import datetime
from pathlib import Path
from confluent_kafka import Consumer, KafkaException, KafkaError


PROJECT_ROOT = Path('/opt/airflow/')
LOG_DIR_CONSUME = PROJECT_ROOT / "logs" / "kafka" / "consume"
MAX_EMPTY_FETCHES = 20


### LOG ---------------------------------------------------------------
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

def get_logger():
    """Create log file"""
    log_producer_dir = LOG_DIR_CONSUME
    log_producer_dir.mkdir(parents=True, exist_ok=True)
    log_file = get_log_file()
    set_log_config(log_file)
    logger = logging.getLogger(__name__)
    return logger


### Kafka callbacks ---------------------------------------------------------------
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


### Kafka ---------------------------------------------------------------
def create_consumer(group_id, topic_name, logger):
    # 步驟1.設定要連線到Kafka集群的相關設定
    # Consumer configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    props = {
        'bootstrap.servers': 'kafka-server:29092',  # Kafka集群在那裡?
        'group.id': group_id,  # ConsumerGroup的名稱
        'auto.offset.reset': 'earliest',  # 是否從這個ConsumerGroup尚未讀取的partition/offset開始讀
        'enable.auto.commit': False,  # 是否啟動自動commit
        'on_commit': lambda err, partitions: print_commit_result(err, partitions, logger),  # 設定接收commit訊息的callback函數
        'error_cb': lambda err: error_cb(err, logger)  # 設定接收error訊息的callback函數
    }

    # 步驟2. 產生一個Kafka的Consumer的實例
    consumer = Consumer(props)
    # 步驟3. 指定想要訂閱訊息的topic名稱，讓Consumer向Kafka集群訂閱指定的topic
    consumer.subscribe([topic_name])
    return consumer


def consume_kafka_message(consumer, logger):
    """
    retrieve data from a kafka topic

    input:
        consumer: an kafka consumer
        logger: TBA
    
    return: a generator which yield list of dict
    """
    empty_fetch_count = 0
    max_empty_fetches = MAX_EMPTY_FETCHES

    try:
        while True:
            records = consumer.consume(num_messages=500, timeout=3)  # batch read records
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
                # keynote: use generator to separate ingest and write (by batch)
                yield insert_buffer
                consumer.commit()
                insert_buffer.clear()

    except Exception as e:
        logger.info(str(e))


def try_decode_utf8(data):
    if data:
        return data.decode('utf-8')
    else:
        return None


def ingest_kafka_message(consumer,logger,dump_function=None):
    """
    Retrieve data from kafka and write to staged storage (locally, GCS or else)
    - current dump to local storage: /opt/airflow/data/stage/
    """

    ### Retrieve data form Kafka
    data_stream = consume_kafka_message(consumer, logger)
    try:
        print("start to retrieve data from kafka")
        for data in data_stream:
            if not data:
                continue
            try:
                if not dump_function:
                    print('Show last record of batch data',len(data),data[-1])
                else:
                    print('Show last record of batch data',len(data),data[-1])
                    dump_function(data)
            except Exception as e:
                logger.info(f"{e}")
    except KeyboardInterrupt:
        logger.info('Aborted by user\n')
    finally:
        consumer.close()
        logger.info(f"Kafka consumer connection has disconnected")


def get_dump_to_local_function(
    ref_date=None,
    base_path='/opt/airflow/data/stage/temp',
):
    """
    Get a function to received data and group by [ref_date] field and save under assigned [base_path]
    """
    def dump_to_local(data):

        # 1) separate_data_by_source_date
        grouped = {}
        for item in data:
            dt_str = item.get(ref_date,datetime.now().strftime("%Y-%m-%d"))
            if not dt_str:
                continue
            dt_str_nodash = datetime.strptime(dt_str, "%Y-%m-%d").strftime("%Y%m%d")
            if dt_str_nodash not in grouped:
                grouped[dt_str_nodash] = []
                grouped[dt_str_nodash].append(item)
            else:
                grouped[dt_str_nodash].append(item)

        # 2) 對每個日期生成 1 份 json
        for dt_str_nodash, rows in grouped.items():
            folder_path = os.path.join(base_path, dt_str_nodash)

            # ---- 不覆蓋目錄（目錄不存在才建立）----
            if not os.path.exists(folder_path):
                os.makedirs(folder_path)

            # 3) 檔名使用當下時間的 timestamp (+ random?)
            file_path = os.path.join(folder_path, f"{int(datetime.now().timestamp())}.json")

            # 4) 寫出 JSON
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(rows, f, ensure_ascii=False, indent=2)
            print(f"saved: {file_path}")
    
    return dump_to_local

