"""
Module: recipe_crawler.py
Creator: Albert
This module is to process web crawling for collecting wanted data in selected websites by Beautifulsoup
"""

from datetime import datetime
from bs4 import BeautifulSoup

# Constant
DATE_REGEX= "%Y/%m/%d"
MAIN = "main ingredients"
SAUCE = "sauce"


class Food:
    """
    This class is to record how many things that need to be stored
    """
    def __init__(self,
                 recept_id=None, # str | None
                 recipe_name=None, # str | None
                 author=None, # str | None
                 good=None, # int | None
                 recipe_url=None, # str | None
                 browsing_num=None, # int | None
                 people=None,  # int | None
                 cooking_time=None, # int | None
                 recept_type=None, # str | None
                 ingredients=None, # list | None
                 quantity=None,  # int | None
                 unit=None, # str | None
                 recipe_upload_date=None, # datetime | None
                 crawl_datetime=datetime.now(), # datetime | None
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


def parse_icook_recipe(scrapy_response):
    """
    This function PARSES the icook recipe page from a Scrapy response.
    It is now a GENERATOR (yields data).

    param: scrapy_response (This is Scrapy's response object, NOT a URL string)
    """
    ### define all values ###
    recipe_name = None
    author = None
    upload_date = None
    browsing = None
    good = None
    ppl = None
    time = None
    # ing_name = None
    # quantity = None
    unit = None
    crawl_datetime = datetime.now()

    # Parse HTML
    soup = BeautifulSoup(scrapy_response.text, "lxml")

    #### find ID ####
    recept_id = scrapy_response.url.split("/")[-1]
    #### find ID end ####

    #### find author ####
    r_author = soup.find("a", attrs={"class": "author-name-link"})
    if r_author:
        author = r_author.text.strip()
    #### find author end ####

    #### find recipe name ####
    r_recipe_name = soup.find("h1", attrs={"id": "recipe-name"})
    if r_recipe_name:
        recipe_name = r_recipe_name.text.strip()
    #### find recipe name end ####

    #### find upload date & browsing  ####
    upload_browsing = soup.find("div", attrs={"class": "recipe-detail-metas"})
    if upload_browsing:
        # find upload date
        # strip the right, left, inner blank
        upload_date = upload_browsing.find("time").text.strip().replace(" ", "")[:-2]
        # regex datetime
        upload_date = datetime.date(datetime.strptime(upload_date, DATE_REGEX))
        # find browsing
        browsing = upload_browsing.find("div").text.strip().replace(" ", "")
    #### find upload  date& browsing end ####

    #### find good reputation ####
    """need to consider good reputation will increase or decrease"""
    r_good = soup.find("span", attrs={"class": "stat-content bold"})
    if r_good:
        good = r_good.text.strip()
    #### find good reputation end ####

    #### find people ####
    r_ppl = soup.find("div", attrs={"class": "servings"})
    if r_ppl:
        ppl = r_ppl.text.strip()
    #### find people end ####

    #### find cooking time ####
    r_time = soup.find("div", attrs={"class": "time-info info-block"})
    if r_time:
        time = r_time.text.strip()

    #### find cooking time end ####

    ### find main ingredients ####
    r_ings = soup.find_all("div", attrs={"class": "group group-0"})
    if r_ings:
        for r_ing in r_ings:
            ings = r_ing.find_all("li", attrs={"class": "ingredient"}) # list
            for ing in ings:
                ing_name = ing.find("a", attrs={"class": "ingredient-search"}).text.strip()
                quantity = ing.find("div", attrs={"class": "ingredient-unit"}).text.strip()
                # collect all info into an object, Food class
                food_data = Food(
                    recept_id=recept_id,
                    recipe_name=recipe_name,
                    author=author,
                    good=good,
                    recipe_url=scrapy_response.url,
                    browsing_num=browsing,
                    people=ppl,
                    cooking_time=time,
                    recept_type=MAIN,
                    ingredients=ing_name,
                    quantity=quantity,
                    unit=unit,
                    recipe_upload_date=upload_date,
                    crawl_datetime=crawl_datetime,
                    )
                yield food_data
    #### find main ingredients end ####

    #### find sauce ingredients ####
    r_sauces = soup.find_all("div", attrs={"class": "group group-1"})
    if r_sauces:
        for r_sauce in r_sauces:
            sauces = r_sauce.find_all("li", attrs={"class": "ingredient"})
            for sauce in sauces:
                ing_name = sauce.find("a", attrs={"class": "ingredient-search"}).text.strip()
                quantity = sauce.find("div", attrs={"class": "ingredient-unit"}).text.strip()
                # collect all info into an object, Food, class
                food_data = Food(
                    recept_id=recept_id,
                    recipe_name=recipe_name,
                    author=author,
                    good=good,
                    recipe_url=scrapy_response.url,
                    browsing_num=browsing,
                    people=ppl,
                    cooking_time=time,
                    recept_type=SAUCE,
                    ingredients=ing_name,
                    quantity=quantity,
                    unit=unit,
                    recipe_upload_date=upload_date,
                    crawl_datetime=crawl_datetime,
                )
                yield food_data
    #### find sauce ingredients end ####
