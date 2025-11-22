import logging
import os
import subprocess, sys

from datetime import datetime
from pathlib import Path


class IcookDailySpider:
    def __init__(self, keyword="latest"):
        # start from the latest part
        self.keyword = keyword
        self.env = os.getenv("AIRFLOW_ENV", "dev")
        self.project_root = Path(__file__).resolve().parents[4] # Root directory : /opt/airflow/src/albert_icook_crawler
        # program-running dir
        self.scapy_project_dir = self.project_root / "src"/ "pipeline" / "extract" / "scrapy_app_icook"
        # file-saved dir
        self.data_dir = self.project_root / "data"/ "daily"
        # create data dir
        self.output_dir = self.data_dir / f"Created_on_{datetime.today().date()}"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        # output file path
        self.output_file_path = self.output_dir / f"icook_recipe_{datetime.today().date()}.csv"

        # === Logging ===
        self.log_dir = self.project_root / "src" / "logs" / "scrapy"
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.log_file = self.log_dir / f"icook_{datetime.today().date()}.log"

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
            handlers=[
                logging.FileHandler(self.log_file, encoding="utf-8", mode="a"),
                logging.StreamHandler(sys.stdout)  # 同時輸出到 console（方便在 Airflow log 看）
            ]
        )

        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Initialized IcookDailySpider with keyword={self.keyword}")


    def run(self):
        command = [
            "scrapy", "crawl", "icook_daily",
            "-a", f"keyword={self.keyword}",
            "-O", str(self.output_file_path),
            "--logfile", str(self.log_file),
            "--loglevel", "DEBUG",
        ]

        self.logger.info(f"[INFO] Start processing...")
        self.logger.info(f"[INFO] Output: {self.output_file_path}")

        try:
            if self.env == "production": # run in the airflow container
                return_code= subprocess.run(
                    args=command,
                    cwd=self.scapy_project_dir,
                    check=False,
                ).returncode


                if return_code == 0:
                    self.logger.info("[SUCCESS] Scrapy completed successfully.")
                else:
                    self.logger.error(f"[ERROR] Scrapy exited with code {return_code}")
            else: # run in the local device
                result = subprocess.run(
                    command,
                    cwd=self.scapy_project_dir,
                    check=True,
                    text=True,
                    capture_output=True
                )
                self.logger.info(result.stdout)
                if result.stderr:
                    self.logger.warning(result.stderr)
                self.logger.info("[SUCCESS] Scrapy completed successfully (dev mode).")

        except subprocess.CalledProcessError as e:
            self.logger.info(f"[ERROR] Failed!")
            self.logger.info(e.stderr)

        finally:
            self.logger.info("Scrapy task finished.")

if __name__ == "__main__":
    IcookDailySpider().run()
