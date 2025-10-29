import requests
from bs4 import BeautifulSoup
import time
import csv
from urllib.parse import urljoin
import os
import re
from datetime import datetime

# ====================================================================
# 配置區 (CONFIGURATION)
# --------------------------------------------------------------------

# 爬取模式限制 (設為 None 表示全量爬取)
MAX_PAGES_TO_GET_LINKS = None  
MAX_RECIPES_FOR_DETAIL_TEST = None

# 爬取任務清單
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

# HTTP 設定與延遲常數
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}
BASE_URL = 'https://www.ytower.com.tw/recipe/'
LINK_CRAWL_DELAY = 0.5    # 爬取目錄頁之間的延遲 (秒)
DETAIL_CRAWL_DELAY = 1.5  # 爬取食譜詳情頁之間的延遲 (秒)

# CSV 輸出欄位定義 (Schema)
CSV_FIELDNAMES = [
    'ID', 'Recipe_Title', 'Author', 'Likes', 'Recipe_URL', 'Type', 
    'Ingredient_Name', 'Weight', 'Unit', 'Publish_Date', 'Crawl_Time'
]

# ====================================================================
# 工具函式 (UTILITIES)
# --------------------------------------------------------------------

def extract_weight_unit(quantity_str: str) -> tuple[str, str]:
    """
    將份量字串 (e.g., '600公克', '1大匙') 分割成數字和單位。
    :param quantity_str: 原始的份量字串。
    :return: (重量, 單位)
    """
    quantity_str = quantity_str.strip()
    
    # 匹配開頭的數字/分數/空格，並將剩餘文字作為單位
    match = re.match(r'([\d\./\s]+)(.*)', quantity_str)
    
    if match:
        weight = match.group(1).strip()
        unit = match.group(2).strip()
        
        # 處理純單位或純文字情況
        if not weight and unit:
            return '', unit
        if not unit and re.search(r'[a-zA-Z\u4e00-\u9fa5]', weight):
             return '', weight
             
        return weight, unit
    
    # 若匹配失敗，回傳空重量與原始字串
    return '', quantity_str 

# ====================================================================
# 爬取核心邏輯 (CRAWLER CORE)
# --------------------------------------------------------------------

def crawl_recipe_details(recipe_url: str) -> list[dict] | None:
    """爬取單一食譜頁面的食材與調味料數據。"""
    
    time.sleep(DETAIL_CRAWL_DELAY) 
    current_crawl_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        response = requests.get(recipe_url, headers=HEADERS, timeout=10)
        # 該網站使用 big5 編碼，須明確設定
        response.encoding = 'big5' 
        response.raise_for_status() 
    except requests.exceptions.RequestException as e:
        print(f"  無法存取 {recipe_url}: {e}")
        return None

    soup = BeautifulSoup(response.text, 'lxml') 
    all_recipe_items = []
    
    # 1. 提取食譜基本資訊 (名稱, ID, 日期)
    recipe_title_tag = soup.select_one('div#recipe_name h2 a')
    recipe_title = recipe_title_tag.text.strip() if recipe_title_tag else "N/A"
    recipe_title = re.sub(r'\(\d+\)$', '', recipe_title).strip() # 清理尾部 (數字)
    
    seq_match = re.search(r'seq=([A-Za-z0-9-]+)', recipe_url)
    recipe_id = seq_match.group(1) if seq_match else 'N/A'
    
    date_tag = soup.select_one('div#recipe_info time')
    publish_date_raw = date_tag.get('datetime', 'N/A') if date_tag else 'N/A'
    
    display_date = publish_date_raw
    if publish_date_raw != 'N/A':
        try:
            # 確保日期格式為 YYYY-MM-DD
            dt_obj = datetime.strptime(publish_date_raw, '%Y-%m-%d')
            display_date = dt_obj.strftime('%Y-%m-%d')
        except ValueError:
             pass # 保持原始日期

    # 2. 提取食材與份量
    ingredient_ul_list = soup.select('div#recipe_item ul.ingredient')
    
    for ul in ingredient_ul_list:
        # 判斷類型：'食材' 或 '調味料'
        list_type = '食材' 
        header_li = ul.select_one('li')
        if header_li and ('調 味 料' in header_li.text.strip() or '調味料' in header_li.text.strip()):
            list_type = '調味料'

        for li in ul.select('li')[1:]: # 跳過第一個標題 li
            name_span = li.select_one('span.ingredient_name')
            amount_span = li.select_one('span.ingredient_amount')

            if name_span and amount_span:
                ingredient_name_full = name_span.text.replace('\r\n', '').strip()
                quantity_str_raw = amount_span.text.strip()
                
                # 簡化食材名稱 (去除後綴的份量部分)
                ingredient_name = ingredient_name_full.replace(quantity_str_raw, '').strip()

                if not ingredient_name: 
                    continue # 跳過無效項目
                
                weight, unit = extract_weight_unit(quantity_str_raw)
                
                # 將結果結構化為字典
                all_recipe_items.append({
                    'ID': recipe_id,
                    'Recipe_Title': recipe_title,
                    'Author': 'N/A',       
                    'Likes': 'N/A',        
                    'Recipe_URL': recipe_url,
                    'Type': list_type,
                    'Ingredient_Name': ingredient_name,
                    'Weight': weight,
                    'Unit': unit,
                    'Publish_Date': display_date,
                    'Crawl_Time': current_crawl_time
                })
        
    return all_recipe_items

def crawl_all_recipe_links(api_template: str, max_pages: int | None) -> list[str]:
    """循環呼叫 pager.asp API 接口，收集所有食譜 URL。"""

    unique_links = set()
    page = 1
    
    print(f" 開始從 API 收集連結...")

    while True:
        if max_pages is not None and page > max_pages:
             break
        
        full_api_url = f"{api_template}&page={page}"
        
        if page > 1:
             time.sleep(LINK_CRAWL_DELAY) 
        
        try:
            response = requests.get(full_api_url, headers=HEADERS, timeout=15)
            response.raise_for_status()
            response.encoding = 'big5'
        except requests.exceptions.RequestException as e:
            print(f" API 頁面 {page} 請求失敗: {e}")
            break

        if not response.text.strip():
             # 達到 API 頁面尾部
             print(f" 頁面 {page} 返回空白內容，終止連結爬取。")
             break

        soup = BeautifulSoup(response.text, 'lxml')
        # 過濾出食譜詳情頁連結 (iframe-recipe.asp)
        new_links = soup.find_all('a', href=lambda href: href and 'iframe-recipe.asp' in href)
        
        if not new_links:
            print(f" 頁面 {page} 未找到新連結，終止循環。")
            break
            
        new_links_count = 0
        for link in new_links:
            href = link['href']
            # 使用 urljoin 構建完整的 URL
            full_recipe_url = urljoin(BASE_URL, href) 
            
            if full_recipe_url not in unique_links:
                unique_links.add(full_recipe_url)
                new_links_count += 1
                
        print(f"  > 頁碼 {page}: 收集到 {new_links_count} 個新連結。目前總計: {len(unique_links)}")

        page += 1 
        
    return list(unique_links)

def execute_crawl_task(task: dict, max_pages_link: int | None, max_details: int | None) -> list[dict]:
    """主要執行流程：先獲取連結，再逐一爬取細項數據。"""
    
    # 步驟 1: 獲取所有連結
    links_to_crawl_full = crawl_all_recipe_links(task['api_template'], max_pages_link)
    
    # 步驟 2: 根據限制決定最終爬取數量
    links_to_crawl = links_to_crawl_full[:max_details] if max_details is not None else links_to_crawl_full
    
    all_recipes_data = []
    total_count = len(links_to_crawl)
    
    print(f"\n  [資訊] 開始爬取【{task['name']}】細項數據: 共 {total_count} 筆食譜。")

    # 步驟 3: 遍歷並爬取每個食譜的詳情頁
    for i, url in enumerate(links_to_crawl):
        # 簡潔的進度顯示 (每 50 個顯示一次，或在開始/結束時顯示)
        if (i + 1) % 50 == 0 or i < 3 or i == total_count - 1:
            print(f"  > 進度: {i+1}/{total_count}")
        
        recipe_items = crawl_recipe_details(url)
        
        if recipe_items:
            all_recipes_data.extend(recipe_items)

    return all_recipes_data

# ====================================================================
# 輸出函式 (OUTPUT)
# --------------------------------------------------------------------

def export_to_csv(data: list[dict], filename: str):
    """將爬取到的數據寫入 CSV 檔案。"""
    
    if os.path.exists(filename):
        os.remove(filename)
        print(f"  [檔案] 移除舊檔案: {filename}")
        
    print(f"  [檔案] 正在寫入 {len(data)} 行數據到 {filename}...")
    
    try:
        # 使用 utf-8-sig 確保中文在 Excel 或其他軟體中不會亂碼
        with open(filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=CSV_FIELDNAMES)
            writer.writeheader()
            writer.writerows(data)
            
        print(f"  [檔案] 成功寫入到 {filename}。")
    except Exception as e:
        print(f"  [ERROR] 寫入 CSV 檔案失敗: {e}")

# ====================================================================
# 主程式執行入口 (MAIN EXECUTION)
# --------------------------------------------------------------------

def main():
    """程式的主執行邏輯入口。"""
    
    total_tasks = len(RECIPE_TASKS)
    
    print("\n" + "="*60)
    print("開始執行食譜食材數據爬取任務")
    print("="*60)
    
    for idx, task in enumerate(RECIPE_TASKS):
        print("\n" + "#"*50)
        print(f"           [任務 {idx + 1}/{total_tasks}] 執行中: {task['name']}           ")
        print("#"*50)
        
        # 1. 執行爬取
        all_recipes = execute_crawl_task(
            task, 
            MAX_PAGES_TO_GET_LINKS, 
            MAX_RECIPES_FOR_DETAIL_TEST
        )

        print(f"\n--- 任務【{task['name']}】完成 ---")
        print(f"收集到的有效數據行數: {len(all_recipes)}")

        # 2. 匯出至 CSV
        if all_recipes:
            export_to_csv(all_recipes, filename=task['output_file'])
        else:
            print("沒有找到有效的食譜數據可匯出。")
            
    print("\n\n" + "="*60)
    print("所有爬取任務已全部完成")
    print("="*60)


if __name__ == '__main__':
    main()
