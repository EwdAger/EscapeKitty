# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose, TakeFirst, Join
from datetime import datetime
from settings import SQL_DATETIME_FORMAT


def trans_time(value):
    return datetime.strptime(value, '%Y-%m-%d')


def trans_int(value):
    return int(value)


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
    publish_time = scrapy.Field(
        input_processor=MapCompose(trans_time)
    )
    end_time = scrapy.Field(
        input_processor=MapCompose(trans_time)
    )
    crawl_time = scrapy.Field()
    scale = scrapy.Field()
    industry_category = scrapy.Field()
    keywords = scrapy.Field()
    is_practice = scrapy.Field(
        input_processor=MapCompose(trans_int)
    )

    def get_insert_sql(self):
        # 插入职位信息的sql语句
        insert_sql = """
            INSERT INTO hnust_jobs(publish_id, company_name, job_name, end_time, publish_time,
            salary, city_name, about_major, degree_require, scale, industry_category, keywords,
            is_practice, url, tianyan_company_url, crawl_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE industry_category=VALUES(industry_category), salary=VALUES(salary), 
            city_name=VALUES(city_name)
        """
        params = (
            self['publish_id'], self['company_name'], self['job_name'], self['end_time'],
            self['publish_time'], self['salary'], self['city_name'], self['about_major'],
            self['degree_require'], self['scale'], self['industry_category'], self['keywords'],
            self['is_practice'], self['url'], self['tianyan_company_url'], self['crawl_time'].strftime(SQL_DATETIME_FORMAT)
        )
        return insert_sql, params

