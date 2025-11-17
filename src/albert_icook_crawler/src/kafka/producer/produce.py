import csv, json
import logging, os, sys
import time

from confluent_kafka import Producer
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[3] # root directory
ENV_FILE_PATH = PROJECT_ROOT / "kafka" /".env"
load_dotenv(ENV_FILE_PATH)

LOG_DIR_PRODUCE = PROJECT_ROOT / "logs" / "kafka" / "produce"
CSV_DIR = PROJECT_ROOT / "data" / "daily"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
TOPIC_NAME = os.getenv("KAFKA_TOPIC_1")

def find_csv():
    """return: CSV file path"""
    try:
        logging.info("File has been found")
        return CSV_DIR / f"Created_on_{datetime.today().date()}" / f"icook_recipe_{datetime.today().date()}.csv"
    except FileNotFoundError:
        logging.info("File not found")
        return None

def move_to_processed_dir(file,logger):
    """
    move file to processed dir
    Param file: CSV file path
    """
    if file is None:
        logging.info(f"Unfound File")
    else:
        processed_dir = PROCESSED_DIR / f"processed={datetime.today().date()}"
        if not processed_dir.exists():
            logging.info(f"processed={datetime.today().date()} directory is being created")
            processed_dir.mkdir(parents=True, exist_ok=True)
        file = str(file)
        file_name = Path(file.split("/")[-1].strip()) # get file name
        processed_file_path = processed_dir / file_name
        Path(file).rename(processed_file_path)

        logger.info(f"[Generator] Archived finished file: {file_name}")


def convert_csv_to_dict(c, logger):
    """
    param csv: csv file path
    return: list of dict of csv file path
    """
    logger.info(f"Start to convert csv to dict")
    insert_dict_file = []
    # get csv-file
    with open(file=c, mode="r", encoding="utf-8-sig") as f:
        # convert each csv file into dictionary file
        dict_file = csv.DictReader(f)
        for row in dict_file:
            clean_row = {key: value.strip() for key, value in row.items()}
            json_row = json.dumps(clean_row).encode("utf-8")
            insert_dict_file.append(json_row)
    logger.info(f"Converting finished")
    return insert_dict_file

def get_log_file():
    return LOG_DIR_PRODUCE / f"produce_{datetime.today().date()}.log"

def set_log_config(log_file):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8", mode="a"),
            logging.StreamHandler(sys.stdout)  # 同時輸出到 console（方便在 Airflow log 看）
        ],
    )

def activate_producer(logger):
    props = {
        'bootstrap.servers': 'localhost:9092',  # <-- 置換成要連接的Kafka集群
        # 'acks': 'all',
        # 'linger.ms': 5,
        # 'batch.size': 32768,
        'error_cb': lambda err: error_cb(err, logger),
    }
    return Producer(props)

def error_cb(err, logger):
    logger.info('Error: %s' % err)

def delivery_callback(err, msg, logger):
    if err:
        logger.info('%% Message failed delivery: %s\n' % err)
    else:
        pass
        # 為了不讓打印訊息拖慢速度, 我們每1萬打印一筆record Metadata來看
        # if msg.offset % 10000 == 0:
        #     logger.info(
        #         '%% Message delivered to topic: {}, partition: {}, offset: {}\n'.format(
        #             msg.topic(),
        #             msg.partition(),
        #             msg.offset()
        #         )
        #     )

def produce_message(producer, dict_list, topic_name, logger):
    """
    produce message
    """
    msg_count = 0 # also as a start index
    time_start = int(round(time.time() * 1000))
    for row in dict_list:
        try:
            while True:
                producer.produce(
                    topic=topic_name,
                    key=f"{datetime.today().date()}_{str(msg_count)}",
                    value=row,
                    on_delivery=lambda err, msg: delivery_callback(err, msg, logger),
                )
                producer.poll(0)
                msg_count += 1
                break
        except BufferError as err:
            logging.error(err)
            producer.poll(1)  # <-- (重要) 呼叫poll來讓client程式去檢查內部的Buffer
            logger.info(
                '%% Local producer queue is full ({} messages awaiting delivery): try again\n'.format(
                    len(producer)
                ))

    time_spend = int(round(time.time() * 1000)) - time_start
    logger.info(f"Send : {msg_count} messages to Kafka")
    logger.info(f"Time spend : {time_spend} ms")
    logger.info(f"Throughput : {msg_count / time_spend * 1000} msg/sec")

def produce():
    ### Create log file
    log_producer_dir = LOG_DIR_PRODUCE
    log_producer_dir.mkdir(parents=True, exist_ok=True)
    log_file = get_log_file()

    set_log_config(log_file)

    logger = logging.getLogger(__name__)
    logger.info(f"Start to process loading CSV file")

    ### activate producer of Kafka

    # create an instance of producer
    producer = activate_producer(logger)

    topic_name = TOPIC_NAME

    ### find csv that is created daily
    csv_file_path = find_csv()

    ### convert csv to dictionary
    csv_dict_format_list = convert_csv_to_dict(csv_file_path, logger)

    ### start processing producing messages
    try:
        logger.info(f"starting sending messages ...")
        produce_message(producer, csv_dict_format_list,topic_name, logger)
    except Exception as e:
        sys.stderr.write(str(e))

    ### move the processed csv file
    move_to_processed_dir(csv_file_path, logger)


    while True:
        logger.info('Flushing final messages (up to 60 seconds)...')
        remaining_msg = producer.flush(10)
        if remaining_msg:
            pass
        else:
            break
    logger.info('Message sending completed!')


if __name__ == "__main__":
    produce()
