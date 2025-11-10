import logging
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
CSV_DIR = os.getenv("CSV_DIR_2")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='my_app.log',  # <--- 關鍵：指定檔案名稱
    filemode='a',           # 'a' 表示附加 (append, 預設)，'w' 表示每次覆寫 (overwrite)
    encoding='utf-8'        # 強烈建議加上編碼，避免中文亂碼
)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def find_csv_file_dir():
    """return generator of csv file path"""
    parent_dir = Path(CSV_DIR)
    archive_dir = parent_dir / "archive"
    if not parent_dir.exists():
        print(f"Error: {parent_dir} does not have any csv files", file=sys.stderr)
    archive_dir.mkdir(exist_ok=True, parents=True)  # parents=True 比較保險

    csv_file_list = parent_dir.glob("*.csv")
    for file in csv_file_list:
        try:
            yield file
            if file.exists():
                archive_path = archive_dir / file.name
                file.replace(archive_path)
                logger.info(f"[Generator] Archived finished file: {file.name}")

        except FileNotFoundError as e:
            print(f"{parent_dir} does not have any csv files")
        except Exception as e:
            logger.error(f"[Generator] Failed to archive {file.name}: {e}")


if __name__ == "__main__":
    find_csv_file_dir()