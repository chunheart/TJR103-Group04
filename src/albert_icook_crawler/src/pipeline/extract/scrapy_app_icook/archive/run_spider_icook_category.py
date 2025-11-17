import os, sys

from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent # find the root directory

def main(search_from_outside):
    """
    Call Scrapy to scrap the data of selected websites
    """
    os.chdir(PROJECT_ROOT) # change the current directory as the working directory

    for keyword in search_from_outside:
        print(f"Processing tasks: {keyword}")

        command = (
            f"scrapy crawl icook_category "
            f"-a keyword={keyword} "
            f"-O output_csvs_{datetime.date(datetime.today())}/icook_{keyword}.csv"
        )

        print(f"Executing {command}")

        os.system(command) # execute the command

        print(f"Task completed: {keyword}")

    print("All tasks completed")


if __name__ == "__main__":
    search = sys.argv[1]
    main(search)