import logging
import subprocess, sys

from datetime import datetime
from pathlib import Path

# project root dir: project_footprint_calculation
PROJECT_ROOT = Path(__file__).resolve().parents[4] # find the root directory
# program-running dir
SCRAPY_PROJECT_DIR = PROJECT_ROOT / "src"/ "pipeline" / "extract" / "scrapy_app_icook"
# file-saved dir
DATA_DIR = PROJECT_ROOT / "data" / "daily"

KEYWORD = "latest"


def run_scrapy():
    # === Logging ===
    log_dir = PROJECT_ROOT / "logs" / "scrapy"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"icook_{datetime.today().strftime('%Y%m%d_%H%M%S')}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler()  # 同時輸出到 console（方便在 Airflow log 看）
        ]
    )

    logger = logging.getLogger(__name__)
    logger.info(f"Initialized IcookDailySpider with keyword={KEYWORD}")

    logger.info(f"Processing...")
    # create data dir
    output_dir = DATA_DIR / f"Created_on_{datetime.today().date()}"
    output_dir.mkdir(parents=True, exist_ok=True)

    # output file path
    output_file_path = output_dir / f"icook_recipe_{datetime.today().date()}.csv"

    # execute scrapy
    command = [
        "scrapy", "crawl", "icook_daily",
        "-a", f"keyword={KEYWORD}",
        "-O", str(output_file_path)
    ]

    logger.info(f"[INFO] Start processing...")
    logger.info(f"[INFO] Output: {output_file_path}")

    try:
        result = subprocess.run(
            args=command,
            cwd=SCRAPY_PROJECT_DIR,
            check=True,
            text=True,
            capture_output=True,
        )

        if result.stdout:
            logger.info("[SCRAPY STDOUT]")
            logger.info(result.stdout)

        if result.stderr:
            logger.warning("[SCRAPY STDERR]")
            logger.warning(result.stderr)

        logger.info(f"[SUCCESS] Completed!")
    except subprocess.CalledProcessError as e:
        logger.info(f"[ERROR] Failed!")
        logger.info(e.stderr)


def main():
    """
    Update the latest recipe from icook daily.
    This program will automatically be activated to retrieve the yesterday data
    """
    run_scrapy()


if __name__ == "__main__":
    main()