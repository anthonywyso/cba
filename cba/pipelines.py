# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pandas as pd
import numpy as np
import sqlite3 as sql
import settings


class CbaPipeline(object):
    def process_item(self, item, spider):
        return item


class SQLitePipeline(object):
    """
    Pipeline for storing scraped items in a SQLite database
    """
    def __init__(self):
        """
        Declares sqlite database.
        """
        self.db = settings.DATABASE['database']

    def process_item(self, item, spider):
        """
        Processes scrapy item and inserts it into existing db table.
        This method is called for every item pipeline component.
        """
        features = item.keys()
        data = {feature: item[feature] for feature in features}
        if '#' in features:
            data.pop('#')
        df = pd.DataFrame(data=data, index=np.arange(1))

        with sql.connect(self.db) as connection:
            df.to_sql(item.__class__.__name__, connection, if_exists='append')

        return item