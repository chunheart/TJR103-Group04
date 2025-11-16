import os
import sys
from datetime import datetime
from pathlib import Path

# Project root directory (where main.py is located)
PROJECT_ROOT = Path(__file__).resolve().parent

def main():
    """
    Executes the 'ytower_main' Scrapy crawl task.
    """
    
    # Change CWD to project root so Scrapy can find its scrapy.cfg
    os.chdir(PROJECT_ROOT)

    # 1. Create a date-stamped directory for the output CSV
    output_dir = "ytower_csv_output"
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "ytower_all_recipes.csv")

    # 2. Assemble the Scrapy command
    #    -O : Overwrites the specified CSV file with all scraped items
    command = f"scrapy crawl ytower_main -O {output_file}"

    print(f"Executing: {command}")
    os.system(command)
    print(f"Crawl completed. Data saved to {output_file}")


if __name__ == "__main__":
    main()