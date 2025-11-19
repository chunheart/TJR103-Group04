# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy

class YtowerCrawlerItem(scrapy.Item):
    ID = scrapy.Field()
    Recipe_Title = scrapy.Field()
    Author = scrapy.Field()
    Recipe_URL = scrapy.Field()
    Servings = scrapy.Field()      
    Type = scrapy.Field()
    Ingredient_Name = scrapy.Field()
    Weight = scrapy.Field()
    Unit = scrapy.Field()
    Publish_Date = scrapy.Field()
    Crawl_Time = scrapy.Field()
    site = scrapy.Field()          
    