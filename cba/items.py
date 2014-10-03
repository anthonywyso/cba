# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html
import string
from scrapy.item import Item, Field
from scrapy.contrib.loader import ItemLoader
from scrapy.contrib.loader.processor import TakeFirst, MapCompose, Join
from scrapy.utils.markup import remove_entities


class CbaItem(Item):
    # define the fields for your item here like:
    # name = Field()
    pass


def create_item_class(class_name, field_list):
    field_dict = {}
    for field_name in field_list:
        field_dict[field_name] = Field()
    return type(class_name, (Item,), field_dict)


def cast_string(x):
    return str(x)


class BasicItemLoader(ItemLoader):
    default_input_processor = MapCompose(remove_entities, string.strip, cast_string)
    default_output_processor = TakeFirst()
