import time
import re
import math
import random
import json

import pandas as pd
import requests
from bs4 import BeautifulSoup

"""
TBA
"""

def get_foodlist():
    foodlist_api_url = 'https://api.myemissions.co/v1/calculator/foods/?limit=1000'
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/Chrome/140.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Referer": "https://myemissions.co/",
    }
    res = requests.get(foodlist_api_url, headers=headers, timeout=20)
    print(res.status_code)
    res_data = res.json()
    if res_data['count'] > 1000:
        print('WARNING: more than 1000 data received')
    ingr_df = pd.DataFrame(res_data['results'])
    return ingr_df


def get_unitlist():
    unitlist_api_url = 'https://api.myemissions.co/v1/calculator/units/'
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/Chrome/140.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Referer": "https://myemissions.co/",
    }
    res = requests.get(unitlist_api_url, headers=headers, timeout=20)
    print(res.status_code)
    unit_df = pd.DataFrame(res.json()['results'])
    return unit_df


def collect_co_emission(ingr_id_list:list,unit_id:str,ingr_batch_size:int=10) -> pd.DataFrame:
    """
    collect co emission by ingr_id and calculator api: https://api.myemissions.co/v1/calculator/
    each ingredient is collected at the same unit (1000g)
    
    input
        ingr_batch_size: num of ingredients per request
        unit_id
        ingr_batch_size
    return df contains following columns
        food_id & name
        category: food category
        serving_weight & serving_desc: a typical serving and its weight in gram(g)
            ex: one slice - fruit cake - 120g 
        emissions: KG CO2 / KG
    """

    ### inital setting
    ingr_list_length = len(ingr_id_list)
    batch_times = math.ceil(ingr_list_length/ingr_batch_size)
    print('Batch API times: ',batch_times)
    calculate_api_url = 'https://api.myemissions.co/v1/calculator/'
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/Chrome/140.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Referer": "https://myemissions.co/",
        "Content-Type": "application/json",
    }
    payload_base = {"ingredients":[],"servings":1}
    collected_data = []
    
    ### batch request
    for b in range(batch_times):
        # print(b*ingr_batch_size,(b+1)*ingr_batch_size)
        batch_ingr_list = ingr_id_list[b*ingr_batch_size:(b+1)*ingr_batch_size]
        print('Actual batch size:',len(batch_ingr_list), 'First item:',batch_ingr_list[0])
        for ingr in batch_ingr_list:
            payload_base['ingredients'].append(
                {
                    'food':ingr,
                    'unit':unit_id,
                    'amount':1,
                }
            )
        # print(payload_base)
        time.sleep(random.choice([0.5,1,3]))
        res = requests.post(calculate_api_url,
                            headers=headers,
                            json=payload_base,)
        print(res.status_code)
        collected_data += res.json()['ingredients']
        payload_base['ingredients'] = []
    print('[Done] collecting ingredients co2 emission')
    return pd.DataFrame(collected_data)


def main_init(ingr_batch_size:int=10, if_write:bool=False):
    """
    An all-in-one process to collect ingredien carbon emission from "myemissions.co"
        https://myemissions.co/resources/food-carbon-footprint-calculator/
    """

    ### collect foods list, 
    food_df = get_foodlist()\
        .rename(columns={'id':'food_id'})
    unit_df = get_unitlist()
    kg_id = unit_df['id'][unit_df['short_desc']=='kg'].iloc[0]
    food_id_list = food_df['food_id'].to_list()

    ### batch query emission
    emit_df = collect_co_emission(
        ingr_id_list=food_id_list,
        unit_id=kg_id,
        ingr_batch_size=ingr_batch_size)
    emit_df = emit_df.drop(columns=['food_name'])

    ### combine
    food_emit_df = food_df.merge(emit_df,on='food_id',how='inner')
    food_emit_df.head()
    if if_write:
        food_emit_df.to_csv('my_emission_ingr_co.csv',index=False)
    else:
        return food_emit_df


def main_update():
    """
    (TBA)
    if we have existing ingr emission and we just need to update new ingr
    """
    pass


if __name__ == "__main__":
    main_init(ingr_batch_size=20, if_write=True)