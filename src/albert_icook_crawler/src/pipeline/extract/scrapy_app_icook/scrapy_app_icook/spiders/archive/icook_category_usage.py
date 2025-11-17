"""
Module: icook_category_usage.py
Creator: Albert
This module is to process data mining in an asynchronous way by Scrapy, requests
"""
import scrapy

from spiders.recipe_crawler import parse_icook_recipe
from urllib.parse import  quote

from scrapy.spidermiddlewares.httperror import HttpError
from twisted.internet.error import DNSLookupError, TimeoutError, TCPTimedOutError


class IcookCategorySpider(scrapy.Spider):
    name = "icook_category" # spider's name
    allowed_domains = ["icook.tw"] # set scraping constraints

    def __init__(self, keyword=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if keyword is None:
            raise ValueError("parameter 'keyword' is must-have, for example, -a keyword=...")
        elif not any(key.isdigit() for key in keyword): # check if each value is int
            raise ValueError("Keyword '%s' is not a number." % keyword)

        self.search_keyword = keyword


    def start_requests(self):
        """
        Replace start_url, make users available to mine data in category page
        Users can enter keyword by -a keyword="number", the number can refer to the category page: https://icook.tw/categories
        """

        # convert the keyword into the code that align to url's standard
        encoded_keyword = quote(self.search_keyword)
        # concat
        url = f"https://icook.tw/categories/{encoded_keyword}/"
        # generate the first url that a spider requests
        yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        # find the all recipe links in target url
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
        items_generator = parse_icook_recipe(response) # yield generator

        for food_object in items_generator:
            yield food_object.__dict__




