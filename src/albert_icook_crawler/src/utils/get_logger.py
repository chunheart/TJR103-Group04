import logging
import sys

"""
FILENAME = os.path.basename(__file__).split(".")[0]
LOG_ROOT_DIR = Path(__file__).resolve().parents[1]
LOG_FILE_PATH = LOG_ROOT_DIR / f"{FILENAME}_{datetime.today().date()}.log"
"""

def get_logger(log_file_path, logger_name):
    """
    Create a logger only belonging to python scripts
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO) # only level above INFO will be written

    # Avoid handler from being added repeatedly
    if logger.hasHandlers():
        logger.handlers.clear()

    # Set format
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    # 1. File Handler
    file_handler = logging.FileHandler(log_file_path, encoding="utf-8", mode="a")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # 2. Stream Handler (Console)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    logger.propagate = False

    return logger

if "__main__" == __name__:
    pass
    # print(LOG_FILE_PATH)