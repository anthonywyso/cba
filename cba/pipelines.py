# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pandas as pd
import numpy as np
import sqlite3


class CbaPipeline(object):
    def process_item(self, item, spider):
        return item


class SQLitePipeline(object):
    """
    Pipeline for storing scraped items in a SQLite database
    """
    def __init__(self):
        """
        Initializes database connection and sessionmaker. Creates table.
        """
        self.db = 'data/cba.sqlite'

    def process_item(self, item, spider):
        """
        Save scraped rows in database based on orm_model.
        This method is called for every item pipeline component.
        """
        features = item.keys()
        features.remove('#')
        data = {feature: item[feature] for feature in features}
        df = pd.DataFrame(data=data, index=np.arange(1))
        # create_table = "CREATE TABLE IF NOT EXISTS {0} (sys_id INTEGER PRIMARY KEY ASC, {1})".format(item.__class__.__name__, ", ".join(features))
        # create_table = "CREATE TABLE IF NOT EXISTS {0} (sys_id INTEGER PRIMARY KEY ASC, ".format(item.__class__.__name__) + ("? " * len(features)) + ")"
        # print create_table

        with sqlite3.connect(self.db) as connection:
            # c = connection.cursor()
            # c.execute(create_table)
            df.to_sql(item.__class__.__name__, connection, if_exists='append')

        return item