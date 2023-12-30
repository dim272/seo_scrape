import re
import json
import string
import logging
import sqlite3
from collections import defaultdict

from scraper.pipelines import SQLITE_DB_PATH
from scraper.spiders.seo_scraper import SeoSpider


class ScrapeItem:
    """
    DB scrape data metaclass.
    """

    def __init__(self, url, title, tags):
        self.url: str = url
        self.title: str = title
        self.tags: dict[str, list] = self._deserialize_tags(tags)

    def get_tags(self, tag_key: str) -> list[str] | None:
        return self.tags.get(tag_key)

    @staticmethod
    def _deserialize_tags(json_obj: json) -> dict[str, list]:
        return json.loads(json_obj)


class TagsAnalyzer:
    """
    Counts duplicate tags.
    """

    def __init__(self, scrape_items: list[ScrapeItem]):
        self.tags = self._collect_tags(scrape_items)

    def __call__(self, *args, **kwargs) -> dict[str, dict]:
        result: dict[str, dict] = {}
        for tag_name, joined_tags in self.tags.items():
            count_tags: dict[str, int] = self._tag_counter(joined_tags)
            result[tag_name] = count_tags

        sorted_result = dict(sorted(result.items()))
        return sorted_result

    def _collect_tags(self, scrape_items: list[ScrapeItem]) -> dict[str, str]:
        result = defaultdict(str)
        for item in scrape_items:
            for tag_name, tag_list in item.tags.items():
                if joined_tags := self._join_tags(tag_list):
                    result[tag_name] += ' ' + joined_tags

        return dict(result)

    @staticmethod
    def _tag_counter(tags: str) -> dict[str, int]:
        result = defaultdict(int)
        for tag in tags.split():
            result[tag] += 1

        return dict(result)

    def _join_tags(self, tag_list: list[str]) -> str:
        tag_list = [self._clear_tag(tag) for tag in tag_list if tag.strip()]
        return ' '.join(tag_list) if tag_list else None

    @staticmethod
    def _clear_tag(tag: str) -> str:
        tag = tag.translate(str.maketrans('', '', string.punctuation))
        tag = re.sub('\xa0', ' ', tag)
        return tag.lower().strip()


def get_db_data(db_name, table_name):
    con = sqlite3.connect(db_name)
    cur = con.cursor()
    sql_query = cur.execute(f"SELECT url, title, tags FROM {table_name}").fetchall()
    return sql_query


def generate_message(analysis: dict[str, dict]) -> str:
    top_tags = set()
    message = ''
    for tag_name, tag_values in analysis.items():
        message += f'\nTop values for {tag_name!r} tag:\n'
        tag_values = sorted(tag_values.items(), key=lambda item: item[1], reverse=True)
        for index, (word, counter) in enumerate(tag_values):
            message += f'\t{word!r} :: {counter}\n'
            if index < 5 and tag_name not in ('h3', 'h4', 'h5'):
                top_tags.add(word)
            if counter <= 1 or index >= 10:
                break

    message += f'\nTop tags: {", ".join(sorted(top_tags))}'

    return message


def main():
    db_data = get_db_data(db_name=SQLITE_DB_PATH, table_name=SeoSpider.name)
    items = [ScrapeItem(url, title, tags) for url, title, tags in db_data]
    analyzer = TagsAnalyzer(items)
    message = generate_message(analyzer())
    logging.info(message)


if __name__ == '__main__':
    logging.basicConfig(level='INFO')
    main()
