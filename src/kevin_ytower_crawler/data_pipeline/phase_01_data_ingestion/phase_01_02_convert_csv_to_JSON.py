# 檔案路徑: src/kevin_ytower_crawler/data_pipeline/phase_01_02_convert_csv_to_JSON.py

import csv
import json

# 匯入修改過的 phase_01_01 檔案
from phase_01_01_load_CSV import find_csv_file_dir

def convert_csv_to_dict():
    """
    讀取 'phase_01_01' 找到的所有 CSV 檔案，
    並將「每一行」轉換為 Kafka 訊息需要的 JSON (bytes) 格式。
    
    返回: 一個包含所有 JSON (bytes) 訊息的列表。
    """
    insert_dict_file = []
    
    # 取得 CSV 檔案的「生成器」
    csv_generator = find_csv_file_dir()
    
    total_rows = 0
    for csv_data_path in csv_generator:
        # 使用 utf-8-sig 讀取 Scrapy 產生的 CSV
        with open(file=csv_data_path, mode="r", encoding="utf-8-sig") as csv_file:
            
            # 將 CSV 每一行轉為 Python 字典 (dict)
            dict_file = csv.DictReader(csv_file)
            
            for row in dict_file:
                # 1. 清理：去除每個欄位值前後多餘的空白
                clean_row = {key: value.strip() for key, value in row.items()}
                
                # 2. 轉換：將 Python 字典轉為 JSON 字串，再 encode 為 bytes
                #    (Kafka Producer 接收的是 bytes)
                json_row = json.dumps(clean_row).encode("utf-8")
                
                insert_dict_file.append(json_row)
                total_rows += 1
                
    print(f"[CSV 轉換] 轉換完畢，共 {total_rows} 筆資料準備上傳。")
    return insert_dict_file

# (這個檔案通常是被匯入的，直接執行它會自動執行並打印結果)
# if __name__ == "__main__":
#     data = convert_csv_to_dict()
#     print(f"測試執行：找到 {len(data)} 筆資料")