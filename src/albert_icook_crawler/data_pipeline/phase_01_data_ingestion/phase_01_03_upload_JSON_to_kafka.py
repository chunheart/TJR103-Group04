import time, os, sys

from phase_01_02_convert_csv_to_JSON import convert_csv_to_dict
from confluent_kafka import Producer

TOPIC_NAME = os.getenv("TOPIC_NAME_2")

def error_cb(err):
    print('Error: %s' % err)

# Optional per-message delivery callback (triggered by poll() or flush())
# when a message has been successfully delivered or permanently
# failed delivery (after retries).
def delivery_callback(err, msg):
    if err:
        print('%% Message failed delivery: %s\n' % err)
    else:
        # 為了不讓打印訊息拖慢速度, 我們每1萬打印一筆record Metadata來看
        if int(msg.key()) % 10000 == 0:
            print(
                '%% Message delivered to topic: {}, partition: {}, offset: {}\n'.format(
                    msg.topic(),
                    msg.partition(),
                    msg.offset()
                )
            )


if __name__ == "__main__":

    upload_csv_dict_list = convert_csv_to_dict()

    # 步驟1. 設定要連線到Kafka集群的相關設定
    props = {
        # Kafka集群在那裡?
        'bootstrap.servers': 'localhost:9092',  # <-- 置換成要連接的Kafka集群
        'acks': 'all',
        'linger.ms': 5,
        'batch.size': 32768,
        'error_cb': error_cb,  # 設定接收error訊息的callback函數
    }

    # 步驟2. 產生一個Kafka的Producer的實例
    producer = Producer(props)

    # 步驟3. 指定想要發佈訊息的topic名稱
    topic_name = TOPIC_NAME
    msg_count = 0
    time_start = int(round(time.time() * 1000))
    try:
        print('Start sending messages ...')

        for row in upload_csv_dict_list:
            while True:
                try:
                    producer.produce(topic=topic_name,
                                     key=str(msg_count),
                                     value=row,
                                     on_delivery=delivery_callback,
                                     )
                    break
                except BufferError as e:
                    producer.poll(1) # <-- (重要) 呼叫poll來讓client程式去檢查內部的Buffer
                    # sys.stderr.write(
                    #     '%% Local producer queue is full ({} messages awaiting delivery): try again\n'.format(
                    #         len(producer)
                    #     ))
            msg_count += 1

            if msg_count % 10000 == 0:
                print('Send {} messages'.format(msg_count))

        time_spend = int(round(time.time() * 1000)) - time_start

        print('Send : ' + str(msg_count) + ' messages to Kafka')
        print('Total spend : ' + str(time_spend) + ' millis-seconds')
        print('Throughput : ' + str(msg_count / time_spend * 1000) + ' msg/sec')

    except Exception as e:
        sys.stderr.write(str(e))

    # 步驟5. 確認所有在Buffer裡的訊息都己經送出去給Kafka了
    while True:
        print('Flushing final messages (up to 60 seconds)...')
        remaining_msg = producer.flush(60)
        if remaining_msg:
            pass
        else:
            break
    print('Message sending completed!')