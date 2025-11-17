# Scrapy settings for ytower_crawler project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html

BOT_NAME = "ytower_crawler"

SPIDER_MODULES = ["ytower_crawler.spiders"]
NEWSPIDER_MODULE = "ytower_crawler.spiders"


# ==========================================================
# --- Custom Project Settings ---
# ==========================================================

# Set a realistic browser User-Agent
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Configure AutoThrottle (intelligent crawl delay)
# This replaces manual time.sleep()
AUTOTHROTTLE_ENABLED = True
# Start with an initial download delay of 1.5 seconds
AUTOTHROTTLE_START_DELAY = 1.5
# Aim to send only one concurrent request to the server at a time
AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0

# Set CSV export encoding to 'utf-8-sig' for Excel compatibility
FEED_EXPORT_ENCODING = "utf-8-sig"

# ==========================================================
# --- Scrapy Default Templates (Can be ignored) ---
# ==========================================================

# Configure maximum concurrent requests (per domain)
# Note: AUTOTHROTTLE_TARGET_CONCURRENCY will take precedence
CONCURRENT_REQUESTS_PER_DOMAIN = 1
DOWNLOAD_DELAY = 1

# Disable cookies (enabled by default)
#COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
#TELNETCONSOLE_ENABLED = False

# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
#    "Accept-Language": "en",
#}

# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
#SPIDER_MIDDLEWARES = {
#    "ytower_crawler.middlewares.YtowerCrawlerSpiderMiddleware": 543,
#}

# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#DOWNLOADER_MIDDLEWARES = {
#    "ytower_crawler.middlewares.YtowerCrawlerDownloaderMiddleware": 543,
#}

# See https://docs.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    "scrapy.extensions.telnet.TelnetConsole": None,
#}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
#ITEM_PIPELINES = {
#    "ytower_crawler.pipelines.YtowerCrawlerPipeline": 300,
#}

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = "httpcache"
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = "scrapy.extensions.httpcache.FilesystemCacheStorage"