import re
from bs4 import BeautifulSoup
from datetime import datetime

# Import helper functions from the utils package
from .utils.parser_utils import extract_weight_unit

def parse_ytower_recipe(response):
    """
    Parses a single YTower recipe detail page from a Scrapy response.
    
    This function is a generator that yields one dictionary per
    ingredient found on the page.
    """
    current_crawl_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Use response.body and specify 'big5' encoding, as required by ytower.com.tw
    soup = BeautifulSoup(response.body, 'lxml', from_encoding='big5')
    
    
    # 1. Extract base recipe info (ID, Title, Date)
    recipe_title_tag = soup.select_one('div#recipe_name h2 a')
    recipe_title = recipe_title_tag.text.strip() if recipe_title_tag else "N/A"
    recipe_title = re.sub(r'\(\d+\)$', '', recipe_title).strip() # Clean "(123)" suffix
    
    # Get recipe ID from the URL string
    seq_match = re.search(r'seq=([A-Za-z0-9-]+)', response.url)
    recipe_id = seq_match.group(1) if seq_match else 'N/A'
    
    # Get publish date
    date_tag = soup.select_one('div#recipe_info time')
    publish_date_raw = date_tag.get('datetime', 'N/A') if date_tag else 'N/A'
    
    display_date = publish_date_raw
    if publish_date_raw != 'N/A':
        try:
            # Ensure YYYY-MM-DD format
            dt_obj = datetime.strptime(publish_date_raw, '%Y-%m-%d')
            display_date = dt_obj.strftime('%Y-%m-%d')
        except ValueError:
             pass # Keep original string if format is unexpected

    # 2. Extract ingredient groups
    ingredient_ul_list = soup.select('div#recipe_item ul.ingredient')
    
    for ul in ingredient_ul_list:
        # Identify group type (ingredient vs. seasoning)
        list_type = '食材' # Default type
        header_li = ul.select_one('li')
        if header_li and ('調 味 料' in header_li.text.strip() or '調味料' in header_li.text.strip()):
            list_type = '調味料'

        # Iterate over actual items, skipping the header <li>
        for li in ul.select('li')[1:]:
            name_span = li.select_one('span.ingredient_name')
            amount_span = li.select_one('span.ingredient_amount')

            if name_span and amount_span:
                ingredient_name_full = name_span.text.replace('\r\n', '').strip()
                quantity_str_raw = amount_span.text.strip()
                
                # Clean ingredient name by removing the quantity suffix
                ingredient_name = ingredient_name_full.replace(quantity_str_raw, '').strip()

                if not ingredient_name: 
                    continue # Skip invalid entries
                
                # Call helper function to split quantity
                weight, unit = extract_weight_unit(quantity_str_raw)
                
                # Yield one structured item per ingredient
                yield {
                    'ID': recipe_id,
                    'Recipe_Title': recipe_title,
                    'Author': 'N/A',       # Field exists in schema, but not on this site
                    'Likes': 'N/A',        # Field exists in schema, but not on this site
                    'Recipe_URL': response.url,
                    'Type': list_type,
                    'Ingredient_Name': ingredient_name,
                    'Weight': weight,
                    'Unit': unit,
                    'Publish_Date': display_date,
                    'Crawl_Time': current_crawl_time
                }