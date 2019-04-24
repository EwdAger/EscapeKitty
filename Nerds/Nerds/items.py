# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose, TakeFirst, Join

class NerdsItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


class HnustJobModelItemLoader(ItemLoader):
    default_output_processor = TakeFirst()


class HnustJobModelItem(scrapy.Item):
    job_name = scrapy.Field()
    url = scrapy.Field()
    publish_id = scrapy.Field()
    salary = scrapy.Field()
    city_name = scrapy.Field()
    about_major = scrapy.Field()
    degree_require = scrapy.Field()
    company_name = scrapy.Field()
    tianyan_company_url = scrapy.Field()
    publish_time = scrapy.Field()
    end_time = scrapy.Field()
    crawl_time = scrapy.Field()
    scale = scrapy.Field()
    industry_category = scrapy.Field()
    keywords = scrapy.Field()
    is_practice = scrapy.Field()


