import scrapy


class ScraperBaseSpider(scrapy.Spider):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not self.start_urls:
            self.start_urls = self.__import_start_urls()

    @staticmethod
    def __import_start_urls() -> list[str]:
        from scraper.hide.start_urls import START_URLS
        return START_URLS
