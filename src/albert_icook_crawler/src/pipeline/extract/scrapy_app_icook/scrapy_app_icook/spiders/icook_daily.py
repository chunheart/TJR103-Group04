"""
Module: icook_category_usage.py
Creator: Albert
This module is to process data mining in an asynchronous way by Scrapy, requests
"""
import scrapy, sys

from urllib.parse import  quote

from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError, TimeoutError, TCPTimedOutError
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

# Constant
DATE_REGEX= "%Y/%m/%d"
MAIN = "main ingredients"
SAUCE = "sauce"

class IcookCategorySpider(scrapy.Spider):
    name = "icook_daily" # spider's name
    allowed_domains = ["icook.tw"] # set scraping constraints

    def __init__(self, keyword=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if keyword is None:
            raise ValueError("parameter 'keyword' is must-have, for example, -a keyword=...")

        self.search_keyword = keyword


    def start_requests(self):
        """
        Replace start_url, make users available to mine data in category page
        Users can enter keyword by -a keyword="number", the number can refer to the category page: https://icook.tw/categories
        """

        # convert the keyword into the code that align to url's standard
        encoded_keyword = quote(self.search_keyword)
        # concat
        url = f"https://icook.tw/recipes/{encoded_keyword}"
        # generate the first url that a spider requests
        yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        # find the all recipe links in target url
        print(f"start processing: {response.url}")

        recipe_links = response.xpath('//a[@class="browse-recipe-link"]/@href').getall()
        for link in recipe_links:
            # response.follow will automatically process the relative path, such as /recipes/480984
            # Scrapy will fetch this url and call function, parse_recipe_detail after completed
            yield response.follow(
                link,
                callback=self.parse_recipe_detail,
                errback=self.handle_recipe_error,
                meta={
                    "max_retry_times": 3
                } # try three times the most,
            )

        # next page
        next_page_link = response.xpath('//a[@rel="next nofollow"]/@href').get()
        if next_page_link is not None:
            # Here it will check if we still have next page. If so, will call the current processing functino again
            yield response.follow(
                next_page_link,
                callback=self.parse,
                errback=self.handle_recipe_error,
                meta = {
                "max_retry_times": 1
                },
            )

    def handle_recipe_error(self, failure):
        """
        This is a function will function only when errors happen
        """
        failed_request = failure.request

        if failure.check(HttpError):
            # HTTP errors (4xx, 5xx)
            response = failure.value.response
            self.logger.error(
                f"HTTP Error {response.status} on URL: {response.url}"
            )

        elif failure.check(DNSLookupError):
            # DNS error
            self.logger.error(
                f"DNS Lookup Error on URL: {failed_request.url}"
            )

        elif failure.check(TimeoutError, TCPTimedOutError):
            # Timeout error
            self.logger.error(
                f"Timeout Error on URL: {failed_request.url}"
            )

        else:
            # other errors
            self.logger.error(
                f"Unhandled Error {failure.value} on URL: {failed_request.url}"
            )
        # Just record errors when one of them happen
        pass

    @staticmethod
    def parse_recipe_detail(response):
        """
        Here we parse the recipe detail page and fetch data from it
        """
        items_generator = Food.parse_icook_recipe(response) # yield generator
        none_count = 0
        try:
            for food_object in items_generator:
                if food_object is None:
                    continue
                else:
                    yield food_object.__dict__
        except AttributeError as e:
            print(f"Exception while parsing recipe detail: {e}")
            pass


"""The below is the function for retrieving the data in recipe page"""

class Food:
    """
    This class is to record how many things that need to be stored
    """

    def __init__(self,
                 recept_id=None,  # str | None
                 recipe_name=None,  # str | None
                 author=None,  # str | None
                 good=None,  # int | None
                 recipe_url=None,  # str | None
                 browsing_num=None,  # int | None
                 people=None,  # int | None
                 cooking_time=None,  # int | None
                 recept_type=None,  # str | None
                 ingredients=None,  # list | None
                 quantity=None,  # int | None
                 unit=None,  # str | None
                 recipe_upload_date=None,  # datetime | None
                 crawl_datetime=datetime.now(),  # datetime | None
                 ):
        self.recept_id = recept_id  # pk
        self.recipe_name = recipe_name  # recipe name
        self.author = author  # recipe creator
        self.good = good  # recipe reputation
        self.recipe_url = recipe_url  # recipe url
        self.browsing_num = browsing_num  # browsing_num
        self.people = people  # the number of people
        self.cooking_time = cooking_time  # cooking time
        self.recept_type = recept_type
        self.ingredients = ingredients  # cooking ingredients
        self.quantity = quantity
        self.unit = unit
        self.recipe_upload_date = recipe_upload_date  # recipe_upload_date
        self.crawl_datetime = crawl_datetime

    @staticmethod
    def parse_icook_recipe(scrapy_response):
        """
            param scrapy_response:  Scrapy's response object, NOT a URL string
            return GENERATOR (yields data).

            Here will only get the yesterday data
            """
        ### define all values ###
        recipe_name = None
        author = None
        upload_date = None
        browsing = None
        good = None
        ppl = None
        time = None
        unit = None
        crawl_datetime = datetime.now()

        # set time boundary
        yesterday_date = (datetime.today() - timedelta(days=1)).date()

        # Parse HTML
        soup = BeautifulSoup(scrapy_response.text, "lxml")

        #### find upload date & browsing  ####
        upload_browsing = soup.find("div", attrs={"class": "recipe-detail-metas"})
        if upload_browsing:
            # find upload date
            # strip the right, left, inner blank
            upload_date = upload_browsing.find("time").text.strip().replace(" ", "")[:-2]
            # regex datetime
            upload_date = datetime.strptime(upload_date, DATE_REGEX).date()
            if upload_date != yesterday_date:
                return None
            # find browsing
            browsing = upload_browsing.find("div").text.strip().replace(" ", "")
        #### find upload  date& browsing end ####

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
                ings = r_ing.find_all("li", attrs={"class": "ingredient"})  # list
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