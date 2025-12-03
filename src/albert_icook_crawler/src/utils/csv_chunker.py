import os, sys
import pandas as pd

from albert_icook_crawler.src.utils.get_logger import get_logger
from datetime import datetime
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2] # albert_icook_crawler

FILENAME = os.path.basename(__file__).split(".")[0]
LOG_FILE_DIR = ROOT_DIR / "src" / "logs" / f"logs={datetime.today().date()}"
LOG_FILE_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE_PATH = LOG_FILE_DIR /  f"{FILENAME}_{datetime.today().date()}.log"
LOGGER = get_logger(log_file_path=LOG_FILE_PATH, logger_name=FILENAME)


INPUT_CSV_DIR = ROOT_DIR / "data" / "yotower_ori_ingredient"
INPUT_CSV_FILE_PATH = INPUT_CSV_DIR / "all_ingredient_names_cleaned_ytower.csv"

OUTPUT_CSV_DIR = INPUT_CSV_DIR = ROOT_DIR / "data" / "yotower_ori_ingredient" / "chunks"
OUTPUT_CSV_DIR.mkdir(exist_ok=True, parents=True)

BATCH_SIZE = 30


def slice(input_path=INPUT_CSV_FILE_PATH):
    """
    This script is to chunk massive CSV files into small, manageable files.
    param input_path: Path, CSV file
    return: Path, csv file
    
    # Algorithm:
    1- open the CSV file that needs to be chunked
    2- chunk it in a unit of 100
    3- save the unit as a new csv 
    """
    LOGGER.info(f"Start processing {FILENAME}.")
    try:
        LOGGER.info(f"Opening {input_path}.")
        # Open the target CSV file
        with open (file=input_path, mode="r", encoding="utf-8-sig") as csv:
            LOGGER.info(f"Opened {input_path}.")

            LOGGER.info(f"Reading {input_path} and Converting into dataframe.")
            df = pd.read_csv(csv)
            LOGGER.info(f"Converted {input_path} into dataframe.")
        
        LOGGER.info(f"Ready to chunk {input_path}.")
        buffer = []
        batch_index = 1
        LOGGER.info(f"Start chunking round {batch_index}.")
        start_idx = 0
        df_list = df.iterrows()
        error = 0
        for idx, row in df_list:
            try:
                buffer.append(row)
                if len(buffer) >= BATCH_SIZE:
                    end_idx = idx
                    chunk_df = pd.DataFrame(buffer)
                    chunk_df.to_csv(
                        path_or_buf=OUTPUT_CSV_DIR / f"all_ingredient_names_cleaned_ytower_chunk_{batch_index}.csv", 
                        index=False,
                    )
                    LOGGER.info(f"Finished chunking round {batch_index}.")
                    LOGGER.info(f"Index {start_idx} to {end_idx} has been chunked, total {BATCH_SIZE} data.")
                    LOGGER.info(f"Chunked CSV file has been saved to the path:\n{OUTPUT_CSV_DIR} / all_ingredient_names_cleaned_ytower_chunk_{batch_index}.csv")
                    
                    # Reset the buffer
                    buffer.clear()
                    LOGGER.info(f"Flushed the buffer.")
                    LOGGER.info(f"=" * 80)

                    batch_index += 1
                    LOGGER.info(f"Start chunking round {batch_index}.")
                    start_idx = idx + 1

            except Exception as e:
                LOGGER.error(f"Failed to porocess the data in round {batch_index}:\nIndex:{idx}\nContent:{row}")
                LOGGER.error(f"This row will be skipped and the script will continue.")
                error += 1
                continue
        
        if error > 0:
            LOGGER.error(f"All process has been completed.\nTotal {error} happened, please check and fix.")
        else:
            LOGGER.info(f"All process has been completed. No errors happened.")
            LOGGER.info(f"Well don. Have a nice one!")




    except FileExistsError as e:
        LOGGER.error(f"Failed to open {input_path}.\nSystem error:{e}")

if __name__ == "__main__":
    # input_file_path = sys.argv[1]
    # if input_file_path is None:
    #     slice()
    # else:
    #     slice(input_file_path)
    slice()
    