# 檔案路徑: src/kevin_ytower_crawler/data_pipeline/phase_01_01_load_CSV.py

import logging
import os
import sys
from pathlib import Path
# (已移除 dotenv)

# --- 1. CSV 來源設定 ---
# 
# 在這裡「寫死」您的 CSV 來源資料夾
# 這個路徑必須和您 `ytower_crawler/main.py` 中設定的 `output_dir` 一致
# (路徑是相對於 TJR103-Group04... 根目錄)
CSV_DIR = "src/kevin_ytower_crawler/ytower_csv_output"
# ---------------------------------

# 設定日誌 (Logging)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def find_csv_file_dir():
    """
    一個「生成器」(Generator) 函式，用來尋找並處理 CSV 檔案。
    
    它會：
    1. 找到 CSV_DIR 中所有的 .csv 檔案。
    2. 使用 `yield` 一次「交出」一個檔案路徑 (供 phase_02 處理)。
    3. 當 phase_02 處理完畢，程式回到這裡，它會將剛處理完的檔案
       「移動」到 'archive' 資料夾，確保不重複處理。
    """
    parent_dir = Path(CSV_DIR)
    archive_dir = parent_dir / "archive" # 封存資料夾
    
    if not parent_dir.exists():
        print(f"錯誤: 來源資料夾 {parent_dir} 不存在。", file=sys.stderr)
        return

    # 確保 'archive' 資料夾存在
    archive_dir.mkdir(exist_ok=True, parents=True)

    # 尋找所有 .csv 檔案
    csv_file_list = parent_dir.glob("*.csv")
    
    file_count = 0
    for file in csv_file_list:
        file_count += 1
        try:
            print(f"[CSV 讀取] 找到檔案: {file.name}")
            # (1) 交出檔案路徑
            yield file
            
            # (3) 處理完畢，移動檔案
            if file.exists():
                archive_path = archive_dir / file.name
                file.replace(archive_path) # 執行移動
                logger.info(f"[CSV 封存] 已處理並封存: {file.name}")

        except Exception as e:
            logger.error(f"[CSV 處理] 處理 {file.name} 時發生錯誤: {e}")
    
    if file_count == 0:
        print(f"[CSV 讀取] 在 {CSV_DIR} 中沒有找到任何 .csv 檔案。")

# (這個檔案通常是被匯入的，直接執行它不會有太大作用)
if __name__ == "__main__":
    # 測試 generator 是否正常運作
    for f in find_csv_file_dir():
        print(f"主程式測試，找到: {f}")