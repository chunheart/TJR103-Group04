# 檔案路徑: src/kevin_ytower_crawler/data_pipeline/phase_01_03_upload_JSON_to_kafka.py

import time
import os
import sys

# 匯入您修改過的 phase_02 檔案
from phase_01_02_convert_csv_to_JSON import convert_csv_to_dict
from confluent_kafka import Producer
# (已移除 dotenv)

# --- 1. Kafka Topic 設定 ---
# 
# 在這裡「寫死」您的 Kafka Topic 設定
# (必須和您在 00 檔案 中設定的名稱「完全一樣」)
TOPIC_NAME = "ytower_recipes_raw"
# ---------------------------------

def error_cb(err):
    """(可選) Kafka 發生錯誤時的回呼函式"""
    print('Error: %s' % err)

def delivery_callback(err, msg):
    """
    (可選) 訊息「成功送達」或「失敗」的回呼函式。
    由 producer.poll() 或 producer.flush() 觸發。
    """
    if err:
        print('%% 訊息傳送失敗: %s\n' % err)
    else:
        # 為了不讓打印訊息拖慢速度, 我們每1萬筆才印一次日誌
        if int(msg.key()) % 10000 == 0:
            print(
                '%% 訊息成功送達: Topic: {}, Partition: {}, Offset: {}\n'.format(
                    msg.topic(),
                    msg.partition(),
                    msg.offset()
                )
            )

# --- 主程式執行區 ---
if __name__ == "__main__":

    # 1. 呼叫 phase_02 函式，取得所有要上傳的資料 (JSON bytes 列表)
    upload_csv_dict_list = convert_csv_to_dict()

    if not upload_csv_dict_list:
        print("[Kafka 上傳] 沒有找到任何資料可上傳。")
        sys.exit(0) # 正常結束程式

    # 2. 設定 Kafka Producer 連線
    # (這個 localhost:9092 對「本地端」測試是正確的)
    props = {
        'bootstrap.servers': 'localhost:9092',  # Kafka 伺服器位址
        'acks': 'all',          # 確保所有副本都收到
        'linger.ms': 5,         # (效能) 傳送前等待 5ms 累積成一批 (batch)
        'batch.size': 32768,    # (效能) 累積滿 32KB 就傳送
        'error_cb': error_cb,   # 設定錯誤回呼
    }

    # 3. 建立 Producer 實例
    producer = Producer(props)

    # 4. 指定要發佈的 Topic
    topic_name = TOPIC_NAME
    msg_count = 0
    time_start = int(round(time.time() * 1000))
    
    try:
        print(f'[Kafka 上傳] 開始傳送 {len(upload_csv_dict_list)} 筆訊息至 Topic: {topic_name} ...')

        for row in upload_csv_dict_list:
            while True:
                try:
                    # 5. 執行「發送」 (非同步)
                    producer.produce(topic=topic_name,
                                     key=str(msg_count), # 訊息的 Key (可選)
                                     value=row,          # 訊息的 Value (JSON bytes)
                                     on_delivery=delivery_callback,
                                     )
                    break # 成功進入緩衝區，跳出 while 迴圈
                
                except BufferError as e:
                    # (重要) 本地緩衝區滿了，代表網路來不及消化
                    # 執行 poll() 來觸發 callback 並清空緩衝區
                    print("緩衝區已滿... 執行 poll()...")
                    producer.poll(1) #
                    
            msg_count += 1

            if msg_count % 10000 == 0:
                print(f'已發送 {msg_count} 筆訊息...')

        time_spend = int(round(time.time() * 1000)) - time_start

        print(f'\n--- 傳送統計 ---')
        print(f'總共發送: {msg_count} 筆訊息')
        print(f'總共花費: {time_spend} 毫秒')
        print(f'傳輸速率: {msg_count / (time_spend / 1000):.2f} msg/sec')

    except Exception as e:
        sys.stderr.write(str(e))

    # 6. (最重要) 確保所有訊息都送達
    #    producer.produce() 是非同步的，程式結束前必須呼叫 flush()
    while True:
        print('Flushing 剩餘訊息 (最多等待 60 秒)...')
        remaining_msg = producer.flush(60) # 等待 60 秒
        if remaining_msg > 0:
            print(f"還有 {remaining_msg} 筆訊息在佇列中，繼續 flush...")
        else:
            print("所有訊息皆已送達。")
            break
            
    print('[Kafka 上傳] 任務完成！')