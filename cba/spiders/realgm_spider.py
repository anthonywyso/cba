from scrapy.spider import Spider
from scrapy.selector import Selector

# TODO explore adding the crawl
from scrapy.contrib.linkextractors.lxmlhtml import LxmlParserLinkExtractor
from scrapy.contrib.spiders import CrawlSpider, Rule
import sqlite3 as sql
import re

from cba.items import create_item_class, BasicItemLoader
import cba.settings as settings


def clean_values(feature):
    '''
    INPUT: string
    OUTPUT: string
    Formats feature values so they can be used in sqlite schema
    '''
    feature = feature.replace('%', 'pct') # cleans feature formats
    feature = feature.replace('3p', 'fg3')
    feature = feature.replace(' ', '_')
    feature = feature.replace('/', '_')
    feature = feature.replace('&', 'and')
    feature = feature.replace("'s", '')
    if re.match('\d', feature):
        feature = "_" + feature
    return feature


def create_table(name, features):
    '''
    INPUT: string, list
    OUTPUT: None
    Dynamically create db table with schema. db attributes specified in settings.
    '''
    table_features = list(features)
    if '#' in table_features:
        table_features.remove('#')
    sql_query = "CREATE TABLE IF NOT EXISTS {0} (sys_id INTEGER PRIMARY KEY ASC, {1})".format(name, ", ".join(table_features))
    with sql.connect(settings.DATABASE['database']) as connection:
        c = connection.cursor()
        c.execute(sql_query)


def create_crawl_urls():
    '''
    INPUT: None
    OUTPUT: list
    Generates a brute-force list of urls to be scraped
    '''
    stat_class = ['player', 'team']
    stat_types = ['Totals', 'Misc_Stats', 'Advanced_Stats']
    team_pages = ['home', 'rosters', 'news', 'schedule', 'stats', 'depth_charts']
    cba_years = range(2012, 2015)
    page_sets = range(1, 10)
    player_urls = ["".join(["http://basketball.realgm.com/international/league/40/Chinese-CBA/stats/", str(year), "/", stat_type,"/All/All/player/All/asc/", str(page), "/Regular_Season"]) for stat_type in stat_types for year in cba_years for page in page_sets]
    team_urls = ["".join(["http://basketball.realgm.com/international/league/40/Chinese-CBA/stats_team/", str(year), "/", stat_type, "/Team_Totals/team_name/asc/"]) for stat_type in stat_types for year in cba_years]
    return player_urls + team_urls


def create_player_urls():
    sql_query = "SELECT league_season_num from league_info_realgm WHERE league_num == 40"
    with sql.connect(settings.DATABASE['database_read']) as connection:
        c = connection.cursor()
        cba_codes = c.execute(sql_query).fetchall()
    cba_codes = [str(code[0]) for code in cba_codes]
    root = 'http://basketball.realgm.com/international/league/40/Chinese-CBA/players'
    urls = ["/".join([root, str(code)]) for code in cba_codes]
    return urls


def create_team_details_urls():
    sql_query = "SELECT url_stem from team_info_scraped WHERE url_stem like '%roster%'"
    with sql.connect(settings.DATABASE['database_read']) as connection:
        c = connection.cursor()
        stems = c.execute(sql_query).fetchall()
    root = 'http://basketball.realgm.com'
    stems = [str(stem[0]).strip('/') for stem in stems]
    cba_years = range(2012, 2015)
    complete_urls = ['/'.join([root, stem, str(year)]) for stem in stems for year in cba_years]
    return complete_urls


def extract_table_name(url):
    '''
    INPUT: Selector object
    OUTPUT: string
    Generates a db table name based on the stats being scraped, as described by the url
    '''
    keywords = set(url.split('/'))
    stat_class = keywords.intersection(set(['player', 'team_totals'])).pop()
    stat_class = stat_class.split('_')[0]
    stat_type = keywords.intersection(set(['totals', 'misc_stats', 'advanced_stats'])).pop()
    stat_type = stat_type.split('_')[0]
    return "_".join([stat_class, 'season', stat_type])


def extract_table_name2(url):
    '''
    INPUT: Selector object
    OUTPUT: string
    Generates a db table name based on the stats being scraped, as described by the url
    '''
    keywords = set(url.split('/'))

    class_check = ['player', 'players', 'team_name', 'team']
    if keywords.intersection(set(class_check)):
        stat_class = keywords.intersection(class_check).pop()
        stat_class = stat_class.strip('s').split('_')[0]
    else:
        stat_class = 'unk'
    # else:
    #     with sql.connect(settings.DATABASE['database_read']) as connection:
    #         c = connection.cursor()
    #         c.execute('''SELECT team_stem FROM team_info_realgm''')
    #         for team in c.fetchall():
    #             if team[0] in url:
    #                 stat_class = 'team'


    type_check = ['totals', 'misc_stats', 'advanced_stats', 'rosters', 'news', 'schedule', 'stats', 'depth_chart']
    if keywords.intersection(set(type_check)):
        stat_type = keywords.intersection(set(type_check)).pop()
        stat_type = stat_type.split('_')[0]
    else:
        stat_type = 'unk'
    return "_".join([stat_class, 'season', stat_type])


def extract_table_name_from_header(table):
    '''
    INPUT: Selector object
    OUTPUT: return string
    Generates a db table name from the <h2> tag above the table
    '''
    results_total = len(table.xpath("preceding-sibling::h2[@class]/text()"))
    header_tag = table.xpath("preceding-sibling::h2[@class]/text()")[results_total - 1].lower()
    with sql.connect(settings.DATABASE['database_read']) as connection:
        c = connection.cursor()
        c.execute('''SELECT team FROM team_info_realgm''')
        teams = [str(x[0]).lower() for x in c.fetchall()]
        c.execute('''SELECT team_name FROM team_info''')
        teams_full = [str(x[0]).lower() for x in c.fetchall()]
    for i, team in enumerate(teams_full):
        header_tag = header_tag.replace(team, '')
        header_tag = header_tag.replace(teams[i], '') # TODO build more robust team name removal
    table_name = clean_values(header_tag)
    # return "_".join([stat_class, 'season', stat_type])


def extract_table_features(table_header, features=[]):
    '''
    INPUT: Selector object, list
    OUTPUT: list
    Builds list of features from a table's header
    '''
    for col in table_header:
            if col.xpath('a'):
                feature = col.xpath('a/text()').extract()
            else:
                feature = col.xpath('text()').extract()
            feature = str(feature[0].strip().lower()) # takes 1st value out of xpath list
            # TODO exceptions.UnicodeEncodeError: 'ascii' codec can't encode character u'\xa0' in position 0: ordinal not in range(128)
            feature = clean_values(feature)
            features.append(feature)

    return features


def find_season_mapping(url):
    keywords = set(url.split('/'))
    sql_query = "SELECT league_season_num, season from league_info_realgm WHERE league_num == 40"
    with sql.connect(settings.DATABASE['database_read']) as connection:
        c = connection.cursor()
        cba_mappings = c.execute(sql_query).fetchall()
    cba_mappings = {str(code): str(season) for code, season in cba_mappings}
    keywords = keywords.intersection(cba_mappings.keys())
    season = cba_mappings[keywords.pop()]
    return season


class RealGMSpider(Spider):
    '''
    Scrapes single table sites
    '''
    name = "realgm"
    allowed_domains = ["basketball.realgm.com"]

    # manually generate urls -- currently unable to crawl due to broken links
    start_urls = create_crawl_urls()

    def parse(self, response):
        sel = Selector(response)
        table_header = sel.xpath("//thead/tr/th")
        table_rows = sel.xpath("//table[@class='filtered']/tbody/tr")

        # extract and clean table features
        features = extract_table_features(table_header, features=['season'])
        # build dynamic sql table name
        db_table = extract_table_name(response.url)
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
                    val = row.xpath('td[%i]/a/text()' % int(i)).extract()
                else:
                    val = row.xpath('td[%i]/text()' % int(i)).extract()
                loader.add_value(str(feature), val)
            yield loader.load_item()


class RealGMMultiTableSpider(Spider):
    '''
    Scrapes multi table sites
    '''
    name = "realgm_multi"
    allowed_domains = ["basketball.realgm.com"]

    # manually generate urls -- currently unable to crawl due to broken links
    # http://basketball.realgm.com/international/league/40/Chinese-CBA/team/86/Bayi-Fubang/rosters/2012
    # start_urls = create_player_urls()
    start_urls = create_crawl_urls()

    def parse(self, response):
        sel = Selector(response)
        tables = sel.xpath("//table[contains(@class,'filtered')]")
        url = response.url

        # iterate through all tables
        for i, table in enumerate(tables):
            # iterate through table header and build features, db table, scrapy item
            table_headers = table.xpath("thead/tr/th")
            features = extract_table_features(table_headers, features=['season'])

            # TODO build new table namer DONE NEEDS TEST extract_table_name2
            # build dynamic sql table name
            db_table = extract_table_name2(url) + str(i)

            # build dynamic sql table
            create_table(db_table, features)
            # build dynamic item container
            DynamicDummyItem = create_item_class(db_table, features)

            # iterate through rows to extract values and assign them to item
            table_rows = table.xpath("tbody/tr")

            for ir, row in enumerate(table_rows):
                # declare item container to hold all the values
                loader = BasicItemLoader(DynamicDummyItem())
                for j, feature in enumerate(features):
                    # print j, feature, row.xpath('td[%i]/a' % int(j))
                    # raw_input('STOPPED')
                    # print row.xpath('td[%i]/a/text()' % int(j)).extract()
                    if feature == 'season':
                        if re.findall('\d{4}', url):
                            val = re.findall('\d{4}', url)[0] # TODO increase robustness for urls without year DONE NEEDS TEST
                        else: # TODO match it up with season in league_info_realgm DONE NEEDS TEST
                            val = find_season_mapping(url)
                    elif row.xpath('td[%i]/a' % int(j)):
                        val = row.xpath('td[%i]/a/text()' % int(j)).extract()
                    else:
                        val = row.xpath('td[%i]/text()' % int(j)).extract()
                    loader.add_value(str(feature), val)
                yield loader.load_item()