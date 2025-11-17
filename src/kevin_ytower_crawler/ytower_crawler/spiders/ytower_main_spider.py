import scrapy
from ytower_crawler.ytower_recipe_parser import parse_ytower_recipe

class YtowerMainSpider(scrapy.Spider):
    name = "ytower_main"
    allowed_domains = ["ytower.com.tw"]

       
    def start_requests(self):
        """
        Entry point: Crawl the first page of both main categories.
        """
        tasks = [
            {'name': '素食', 'url': 'https://www.ytower.com.tw/recipe/pager.asp?VEGETARIA=%AF%C0%AD%B9&IsMobile=0'},
            {'name': '葷食', 'url': 'https://www.ytower.com.tw/recipe/pager.asp?VEGETARIA=%B8%A7%AD%B9&IsMobile=0'}
        ]

        for task in tasks:
            # # Pass page/base_url for pagination logic
            yield scrapy.Request(
                url=task['url'], 
                callback=self.parse_category_page,
                cb_kwargs={'page': 1, 'base_url': task['url']}
            )

    def parse_category_page(self, response, page, base_url):
        """
        Handles category pages: Scrapes recipe links and handles pagination.
        """
        # 1. Extract recipe links
        recipe_links = response.css('a[href*="iframe-recipe.asp"]::attr(href)').getall()

        if not recipe_links:
            self.logger.info(f'在 {response.url} 找不到新連結，停止翻頁。')
            return

        # 2. Yield requests for recipe details
        for link in recipe_links:
            # response.follow handles relative URLs
            yield response.follow(link, callback=self.parse_recipe_detail)

        # 3. Handle next page
        next_page = page + 1
        next_page_url = f"{base_url}&page={next_page}"

        # Yield next page request, recursively calling this function
        yield scrapy.Request(
            url=next_page_url, 
            callback=self.parse_category_page,
            cb_kwargs={'page': next_page, 'base_url': base_url}
        )

    def parse_recipe_detail(self, response):
        """
        Handles recipe detail pages.
        Offloads all parsing logic to the imported parser function.
        """
        # The parser function yields item dicts
        yield from parse_ytower_recipe(response)