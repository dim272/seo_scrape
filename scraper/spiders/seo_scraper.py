import json

import scrapy

from scraper.spiders.base_spider import ScraperBaseSpider


class SeoSpider(ScraperBaseSpider):
    name = 'seo_scrape'
    start_urls = []     # Set your start urls here.

    def parse(self, resp: scrapy.http.Response, **kwargs):
        item = {
            'url': resp.url,
            'title': resp.xpath('//head/title/text()').get(),
            'tags': self.parse_tags(resp),
        }

        yield item

    @staticmethod
    def parse_tags(resp: scrapy.http.Response) -> json:
        result = dict()

        for number in range(1, 5):
            tag_name = f'h{number}'
            tags = resp.xpath(f'//{tag_name}')
            tag_values = [tag.xpath('normalize-space(./text())').get() for tag in tags]
            result[tag_name] = [tag_value for tag_value in tag_values if tag_value]

        return json.dumps(result)
