"""
Goal: Crawling the recipe
Name: Albert
"""


import time
import pandas as pd
import random
import requests

from datetime import datetime
from bs4 import BeautifulSoup


# Constant
BASE_URL_ICOOK = "https://icook.tw"

SEARCH_ICOOK = "/search/"

HEADERS = {
    "user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36"
}

DATE_REGEX= "%Y/%m/%d"

MAIN = "main ingredients"

SAUCE = "sauce"

class Food:
    """
    This class is to record how many things that need to be stored
    """
    def __init__(self,
                 recept_id,
                 recipe_name, # str
                 author, # str
                 good, # int
                 recipe_url, # str
                 browsing_num, # int only in icook
                 people,  # int,
                 cooking_time, # int
                 recept_type, # str
                 ingredients, # list
                 quantity,  # int
                 unit, # str
                 recipe_upload_date, # datetime
                 crawl_datetime, # datetime
                 ):

        self.recept_id = recept_id # pk
        self.recipe_name = recipe_name # recipe name
        self.author = author  # recipe creator
        self.good = good  # recipe reputation
        self.recipe_url = recipe_url # recipe url
        self.browsing_num = browsing_num # browsing_num
        self.people = people  # the number of people
        self.cooking_time = cooking_time  # cooking time
        self.recept_type = recept_type
        self.ingredients = ingredients # cooking ingredients
        self.quantity = quantity
        self.unit = unit
        self.recipe_upload_date = recipe_upload_date  # recipe_upload_date
        self.crawl_datetime = crawl_datetime



    def __hash__(self):
        return hash(self.recipe_url) + hash(self.browsing_num) + hash(self.author) + hash(self.good)

    def __eq__(self, other):
        return (self.recipe_url == other.recipe_url
                and self.browsing_num == other.browsing_num
                and self.good == other.good)


def crawl_icook_recept():
    """
    This function is to crawl the icook recipe page
    """
    recept_id = None
    recipe_name = str
    author = str
    recipe_url = "https://icook.tw/recipes/482266"
    upload_date = datetime
    browsing = None
    good = None
    ppl = None # means people
    time_item = None
    recept_type = None
    quantity = 0
    unit = None
    crawl_datetime = datetime.now()

    food_list = []

    response = requests.get(recipe_url, headers=HEADERS) # HTML

    # print(response) # show 200 or not
    try:
        response.raise_for_status() # make sure to receive 200

        soup = BeautifulSoup(response.text, "lxml") # HTML
        # print(soup)

        #### find ID ####
        recept_id = recipe_url.split("/")[-1]
        #### find ID end ####

        #### find author ####
        r_author = soup.find("a", attrs={"class": "author-name-link"})
        if r_author: # if author exist
            for author in r_author:
                author = author.text.strip()
                # print(f"author: {author}", type(author))
        #### find author end ####

        #### find recipe name ####
        r_recipe_name = soup.find_all("h1", attrs={"id": "recipe-name"})
        if r_recipe_name:
            for recipe_name in r_recipe_name:
                recipe_name = recipe_name.text.strip()
                # print(f"recipe: {recipe_name}", type(recipe_name))
        #### find recipe name end ####

        #### find upload date & browsing  ####
        r_upload_browsing = soup.find_all("div", attrs={"class": "recipe-detail-metas"})
        # print(r_upload)
        if r_upload_browsing:
            for upload_browsing in r_upload_browsing:
                # find upload date
                # strip the right, left, inner blank
                upload_date = upload_browsing.find("time").text.strip().replace(" ", "")[:-2]
                # regex datetime
                upload_date = datetime.date(datetime.strptime(upload_date, DATE_REGEX))
                # print(f"upload date: {upload_date}", type(upload_date))
                # find browsing
                browsing = int(upload_browsing.find("div").text.strip().replace(" ", "")[:-2])
                # print(f"browsing num: {browsing}", type(browsing))
        #### find upload  date& browsing end ####

        #### find good reputation ####
        """need to consider good reputation will increase or decrease"""
        r_good = soup.find_all("span", attrs={"class": "stat-content bold"})
        if r_good:
            for good in r_good:
                good = int(good.text.strip())
                # print(f"reputation: {good}", type(good))
        #### find good reputation end ####

        #### find people ####
        r_ppl = soup.find_all("div", attrs={"class": "servings"})
        if r_ppl:
            for ppl in r_ppl:
                ppl = int(ppl.find("span", attrs={"class": "num"}).text.strip())
                # print(f"serving : {ppl}", type(ppl))
        #### find people end ####

        #### find cooking time ####
        r_time = soup.find_all("div", attrs={"class": "time-info info-block"})
        if r_time:
            for time_item in r_time:
                time_item = int(time_item.find("span", attrs={"class": "num"}).text.strip())
                # print(f"time : {time_item}", type(time_item))
        #### find cooking time end ####

        ### find main ingredients ####
        r_ings = soup.find_all("div", attrs={"class": "group group-0"})
        # print(r_ings)
        if r_ings:
            for r_ing in r_ings:
                ings = r_ing.find_all("li", attrs={"class": "ingredient"}) # list
                for ing in ings:
                    ing_name = ing.find("a", attrs={"class": "ingredient-search"}).text.strip()
                    ing_num = ing.find("div", attrs={"class": "ingredient-unit"}).text.strip()
                    print(f"main ingredient: {ing_name}")
                    print(f"main ingredient num: {ing_num}")
                    food_data = Food(
                                    recept_id=recept_id,
                                    recipe_name=recipe_name,
                                    author=author,
                                    good=good,
                                    recipe_url=recipe_url,
                                    browsing_num=browsing,
                                    people=ppl,
                                    cooking_time=time_item,
                                    recept_type=MAIN,
                                    ingredients=ing_name,
                                    quantity=ing_num,
                                    unit=unit,
                                    recipe_upload_date=upload_date,
                                    crawl_datetime=crawl_datetime,
                                    )
                    food_list.append(food_data)
        #### find main ingredients end ####

        #### find sauce ingredients ####
        r_sauces = soup.find_all("div", attrs={"class": "group group-1"})
        # print(r_sauces)
        if r_sauces:
            for r_sauce in r_sauces:
                sauces = r_sauce.find_all("li", attrs={"class": "ingredient"})
                for sauce in sauces:
                    sauce_name = sauce.find("a", attrs={"class": "ingredient-search"}).text.strip()
                    sauce_num = sauce.find("div", attrs={"class": "ingredient-unit"}).text.strip()
                    # print(f"sauce ingredient: {sauce_name}")
                    # print(f"sauce ingredient num: {sauce_num}")
                    food_data = Food(
                        recept_id=recept_id,
                        recipe_name=recipe_name,
                        author=author,
                        good=good,
                        recipe_url=recipe_url,
                        browsing_num=browsing,
                        people=ppl,
                        cooking_time=time_item,
                        recept_type=SAUCE,
                        ingredients=sauce_name,
                        quantity=sauce_num,
                        unit=unit,
                        recipe_upload_date=upload_date,
                        crawl_datetime=crawl_datetime,
                    )
                    food_list.append(food_data)
        #### find sauce ingredients end ####

        ### for converting the above info into pandas ####
        columns = [
            "recept_id",
            "recipe_name",
            "author",
            "recipe_url",
            "browsing_num",
            "good",
            "people",
            "cooking_time",
            "recept_type",
            "ingredients",
            "quantity",
            "unit",
            "recipe_upload_date",
            "crawl_datetime",
        ]

        data = []
        for food in food_list:
            food_obj = [
                    food.recept_id,
                    food.recipe_name,
                    food.author,
                    food.recipe_url,
                    food.browsing_num,
                    food.good,
                    food.people,
                    food.cooking_time,
                    food.recept_type,
                    food.ingredients,
                    food.quantity,
                    food.unit,
                    food.recipe_upload_date,
                    food.crawl_datetime,

                ]
            data.append(food_obj)


        recept_df = pd.DataFrame(data, columns=columns)
        print("=" * 60)
        print(recept_df)
        print("=" * 60)
        filename = "recept_data.csv"
        recept_df.to_csv(filename, index=False, mode="w", encoding="utf-8")
        print(f"{filename} has been saved successfully")
        #### for converting the above info into pandas end ####

        sleeptime = random.randint(2,5)
        time.sleep(sleeptime)  # no need for one-time request

    except requests.exceptions.HTTPError as e:
        print(f"Error: {e}")


def main():
    crawl_icook_recept()


if __name__ == '__main__':
    main()
