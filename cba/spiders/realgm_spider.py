from scrapy.spider import Spider
from scrapy.selector import Selector

# TODO explore adding the crawl
from scrapy.contrib.linkextractors.lxmlhtml import LxmlParserLinkExtractor
from scrapy.contrib.spiders import CrawlSpider, Rule
import sqlite3 as sql

from cba.items import create_item_class, BasicItemLoader
import cba.settings as settings

stat_class = ['player', 'team']
stat_types = ['Totals', 'Misc_Stats', 'Advanced_Stats']
cba_years = range(2012, 2015)
page_sets = range(1, 10)


def create_table(name, features):
    '''
    INPUT: string, list
    OUTPUT: None
    Dynamically create db table with schema. db specified in settings.
    '''
    # features still has '#'
    features.remove('#')
    sql_query = "CREATE TABLE IF NOT EXISTS {0} (sys_id INTEGER PRIMARY KEY ASC, {1})".format(name, ", ".join(features))
    with sql.connect(settings.DATABASE['database']) as connection:
        c = connection.cursor()
        c.execute(sql_query)


class RealGMSpider(Spider):
    name = "realgm"
    allowed_domains = ["basketball.realgm.com"]

    # manually generate urls -- currently unable to crawl due to broken links
    start_urls = ["".join(["http://basketball.realgm.com/international/league/40/Chinese-CBA/stats/", str(year), "/Totals/All/All/player/All/asc/", str(page), "/Regular_Season"]) for year in cba_years for page in page_sets]

    def parse(self, response):
        sel = Selector(response)
        table_header = sel.xpath('//thead/tr/th')
        table_rows = sel.xpath("//table[@class='filtered']/tbody/tr")

        # extract table features
        features = ['season']
        for col in table_header:
            if col.xpath('a'):
                feature = col.xpath('a/text()').extract()
            else:
                feature = col.xpath('text()').extract()
            feature = str(feature[0]).lower().replace('%', 'pct') #takes 1st value out of list, format style
            feature = feature.replace('3p', 'fg3')
            features.append(feature)

        # TODO add dynamic db_table names
        db_table = 'player_seasonlog_totals'

        # build dynamic sql table
        create_table(db_table, features)

        # build dynamic item container
        DynamicDummyItem = create_item_class(db_table, features)

        # iterate through rows to extract values and assign them to item
        for ir, row in enumerate(table_rows):
            # declare item container to hold all the values
            loader = BasicItemLoader(DynamicDummyItem())

            for i, feature in enumerate(features):
                if feature == 'season':
                    val = table_header.xpath('a/@href').re('\d{4}')
                elif row.xpath('td[%i]/a' % int(i)):
                    val = row.xpath('td[%i]/a/text()' % int(i)).extract() # names only
                else:
                    val = row.xpath('td[%i]/text()' % int(i)).extract()
                loader.add_value(str(feature), val)

            yield loader.load_item()

