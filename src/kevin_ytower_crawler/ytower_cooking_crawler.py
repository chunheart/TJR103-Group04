import requests
from bs4 import BeautifulSoup
import time
import csv
from urllib.parse import urljoin
import os
from datetime import datetime

# ====================================================================
# CONFIGURATION
# --------------------------------------------------------------------

# Set to a number to limit pages (e.g., 3) for testing.
# Set to None for a full crawl.
MAX_PAGES_TO_GET_LINKS = None  
MAX_RECIPES_FOR_DETAIL_TEST = None

# Defines the crawl jobs to run
RECIPE_TASKS = [
    {
        'name': '素食',
        'api_template': 'https://www.ytower.com.tw/recipe/pager.asp?VEGETARIA=%AF%C0%AD%B9&IsMobile=0',
        'output_file': 'ytower_all_vegetarian_structured_recipes.csv',
    },
    {
        'name': '葷食',
        'api_template': 'https://www.ytower.com.tw/recipe/pager.asp?VEGETARIA=%B8%A7%AD%B9&IsMobile=0',
        'output_file': 'ytower_all_omnivore_structured_recipes.csv',
    }
]

# HTTP Settings & Delays
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}
BASE_URL = 'https://www.ytower.com.tw/recipe/'
LINK_CRAWL_DELAY = 0.5    # Delay between fetching list pages (sec)
DETAIL_CRAWL_DELAY = 1.5  # Delay between fetching detail pages (sec)

# Defines the columns for the output CSV file
CSV_FIELDNAMES = [
    'ID', 
    'Recipe_Title', 
    'Author', 
    'Recipe_URL', 
    'Servings',       
    'Type', 
    'Ingredient_Name', 
    'Weight', 
    'Unit', 
    'Publish_Date', 
    'Crawl_Time', 
    'site'             
]

# ====================================================================
# CRAWLER CORE
# --------------------------------------------------------------------

# (NOTE: The 'crawl_recipe_details' function is missing from this file.)
# (NOTE: The 'extract_weight_unit' function is missing from this file.)


def crawl_all_recipe_links(api_template: str, max_pages: int | None) -> list[str]:
    """Fetches all recipe URLs by iterating through the pager.asp API."""

    unique_links = set()
    page = 1
    
    print(f" Starting link collection from API...")

    while True:
        if max_pages is not None and page > max_pages:
             break # Stop if page limit is reached
        
        full_api_url = f"{api_template}&page={page}"
        
        if page > 1:
             time.sleep(LINK_CRAWL_DELAY) 
        
        try:
            # Fetch the list page
            response = requests.get(full_api_url, headers=HEADERS, timeout=15)
            response.raise_for_status()
            response.encoding = 'big5' # Target site uses big5 encoding
        except requests.exceptions.RequestException as e:
            print(f" API Page {page} request failed: {e}")
            break # Stop on network error

        if not response.text.strip():
             # Stop if the API returns an empty page
             print(f" Page {page} returned empty content. Stopping link crawl.")
             break

        soup = BeautifulSoup(response.text, 'lxml')
        # Find all links pointing to a recipe page
        new_links = soup.find_all('a', href=lambda href: href and 'iframe-recipe.asp' in href)
        
        if not new_links:
            # Stop if this page contains no recipe links
            print(f" Page {page} has no new links. Stopping loop.")
            break
            
        new_links_count = 0
        for link in new_links:
            href = link['href']
            # Build the full, absolute URL
            full_recipe_url = urljoin(BASE_URL, href) 
            
            if full_recipe_url not in unique_links:
                unique_links.add(full_recipe_url)
                new_links_count += 1
                
        print(f"  > Page {page}: Found {new_links_count} new links. Total: {len(unique_links)}")

        page += 1 # Go to the next page
        
    return list(unique_links)

def execute_crawl_task(task: dict, max_pages_link: int | None, max_details: int | None) -> list[dict]:
    """Main execution flow: get links, then crawl details for each link."""
    
    # Step 1: Get all links
    links_to_crawl_full = crawl_all_recipe_links(task['api_template'], max_pages_link)
    
    # Step 2: Apply limit if MAX_RECIPES_FOR_DETAIL_TEST is set
    links_to_crawl = links_to_crawl_full[:max_details] if max_details is not None else links_to_crawl_full
    
    all_recipes_data = []
    total_count = len(links_to_crawl)
    
    print(f"\n  [INFO] Starting detail crawl for [{task['name']}]: {total_count} recipes.")

    # Step 3: Iterate and crawl each detail page
    for i, url in enumerate(links_to_crawl):
        # Print a progress update every 50 items
        if (i + 1) % 50 == 0 or i < 3 or i == total_count - 1:
            print(f"  > Progress: {i+1}/{total_count}")
        
        # This call will fail, as 'crawl_recipe_details' is not defined in this file.
        recipe_items = crawl_recipe_details(url)
        
        if recipe_items:
            all_recipes_data.extend(recipe_items)

    return all_recipes_data

# ====================================================================
# OUTPUT
# --------------------------------------------------------------------

def export_to_csv(data: list[dict], filename: str):
    """Writes the collected data list into a CSV file."""
    
    if os.path.exists(filename):
        os.remove(filename) # Remove old file if it exists
        print(f"  [File] Removed old file: {filename}")
        
    print(f"  [File] Writing {len(data)} rows to {filename}...")
    
    try:
        # Use 'utf-8-sig' to ensure correct encoding in Excel
        with open(filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=CSV_FIELDNAMES)
            writer.writeheader()
            writer.writerows(data)
            
        print(f"  [File] Successfully wrote to {filename}.")
    except Exception as e:
        print(f"  [ERROR] Failed to write CSV file: {e}")

# ====================================================================
# MAIN EXECUTION
# --------------------------------------------------------------------

def main():
    """Main entry point for the script."""
    
    total_tasks = len(RECIPE_TASKS)
    
    print("\n" + "="*60)
    print("Starting Recipe Data Crawl Task")
    print("="*60)
    
    for idx, task in enumerate(RECIPE_TASKS):
        print("\n" + "#"*50)
        print(f"           [TASK {idx + 1}/{total_tasks}] Running: {task['name']}           ")
        print("#"*50)
        
        # 1. Run the crawl task
        all_recipes = execute_crawl_task(
            task, 
            MAX_PAGES_TO_GET_LINKS, 
            MAX_RECIPES_FOR_DETAIL_TEST
        )

        print(f"\n--- Task [{task['name']}] Complete ---")
        print(f"Total valid data rows collected: {len(all_recipes)}")

        # 2. Export results to CSV
        if all_recipes:
            export_to_csv(all_recipes, filename=task['output_file'])
        else:
            print("No valid recipe data found to export.")
            
    print("\n\n" + "="*60)
    print("All crawl tasks completed")
    print("="*60)


if __name__ == '__main__':
    main()