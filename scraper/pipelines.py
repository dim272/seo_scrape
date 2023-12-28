import sqlite3
from pathlib import Path

SQLITE_DB_NAME = 'seo.db'
SQLITE_DB_PATH = (Path(__file__).parent / SQLITE_DB_NAME).resolve()


class SqlitePipeline:
    """Saves items to sqlite db."""

    con: sqlite3.Connection
    cur: sqlite3.Cursor
    table_name: str

    def open_spider(self, spider):
        self.con = sqlite3.connect(SQLITE_DB_PATH)
        self.cur = self.con.cursor()
        self.table_name = spider.name
        self.cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS '{self.table_name}'
            (
                id INTEGER PRIMARY KEY,
                url TEXT,
                title TEXT,
                tags JSON,
                created DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def process_item(self, item, spider):
        self.cur.execute(
            f"""
                INSERT INTO '{self.table_name}' 
                (url, title, tags) 
                VALUES (?, ?, ?)
            """,
            (
                item.get('url'),
                item.get('title'),
                item.get('tags'),
            )
        )

        self.con.commit()
        return item

    def close_spider(self, spider):
        self.con.close()
