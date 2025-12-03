import time
import re
import math
import random
import json

import pandas as pd
import requests
from bs4 import BeautifulSoup

"""
Query and Crawl from https://apps.carboncloud.com/climatehub/search?q=tofu
Notes
- some query may fail, ex: chicken can't be found
"""

def parse_hits_product(search_api_res_data:list) -> list : 
    """
    Parse and extract data from respons of search API
    """
    parsed_data = []
    for res_prod in search_api_res_data.get("hits", []):
        c0 = res_prod.get("contents", [{}])[0]
        c1 = res_prod.get("contents", [{}, {}])[1]
        rec = {
            "prod_id": c0,
            "prod_name": c1.get("productName"),
            "total_coe": c1.get("totalFootprint"),
            "market": c1.get("market"),
            "org": c1.get("orgInfo", {}).get("displayName"),
            "breakdown_coe": json.dumps(c1.get("footprintBreakdown", {})),
        }
        parsed_data.append(rec)

    return parsed_data


def carboncloud_crawler(query_list:list,max_pages_per_item=2) -> None | pd.DataFrame:
    """
    Crawl co emission from https://apps.carboncloud.com/climatehub/search?q=bagel
        using their search api: https://api.carboncloud.com/v0/search
        also see their prod api: https://api.carboncloud.com/v0/carbondata/{prod_id}
    
    query_list should be a list of string of ingredient name, like tofu, bagel
    return dataframe contains fields as follow:
        prod_id: for their product api or product page
        prod_name: product name (lowered)
        total_coe: total carbon emission (kg CO2e/kg)
        breakdown_coe: detail composition of carbon emission
        query: get from what query word

    Backlog
    1.furthur clean
        1-1 composition products like: beef and vegetable soup
        1-2 variant
    2.more data
        2-1 to check what else can we extract from responsed data
    """
    query_df_collect = []
    for query_item in query_list:
        
        query_item = str(query_item).lower()
        ### initial setting
        page_at = 1
        page_size = 20
        search_api_url = f'https://api.carboncloud.com/v0/search?q={query_item}&limit={page_size}&offset={(page_at-1)*20}'
        print('GET search API:',search_api_url)
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/Chrome/140.0.0.0 Safari/537.36",
            "Accept": "*/*",
            "Referer": "https://apps.carboncloud.com/",
        }

        ### initial request
        try:
            res = requests.get(search_api_url, headers=headers, timeout=20)
            #res.raise_for_status()
            res_data = res.json()
            res_total_hits_count = res_data['totalHitCount']
            total_pages = math.ceil(res_total_hits_count/20)
            total_pages = min(total_pages,max_pages_per_item)
            print(f'Find {res_total_hits_count} products. {total_pages} pages to be collected (limited by max {max_pages_per_item} pages)')
        except:
            print(f'[Warning] Fail to crawl and fetch {query_item}')
            continue
        if res_total_hits_count < 1:
            continue

        ### collect by page
        collected_prod = []
        try:
            for i in range(1,total_pages + 1):
                if i == 1:
                    collected_prod += parse_hits_product(res_data)
                else:
                    page_at = i
                    search_api_url = f'https://api.carboncloud.com/v0/search?q={query_item}&limit={page_size}&offset={(page_at-1)*20}'
                    print('GET search API:',search_api_url)
                    time.sleep(random.choice([0.5,1,3]))
                    res = requests.get(search_api_url, headers=headers, timeout=20)
                    res_data = res.json()
                    collected_prod += parse_hits_product(res_data)
                    print('GET search API done:',search_api_url)

            ### to df & basic clean data
            query_df = pd.DataFrame(collected_prod)
            query_df['query'] = query_item  #lowered
            query_df['prod_name'] = query_df['prod_name'].str.lower()
            query_df_collect.append(query_df)
        except:
            print(f'[Warning] Fail to crawl and fetch {query_item}')

    if not query_df_collect:
        print(f'[Warning] Fail to crawl and fetch {query_item}')
        return
    else:
        return pd.concat(query_df_collect)


def test_main():
    my_df = carboncloud_crawler(query_list=['tofu','bagel'])
    print(my_df.shape)
    print(my_df.head())
    my_df.to_csv('carboncloud_demo.csv',index=False)


if __name__ == "__main__":
    test_main()