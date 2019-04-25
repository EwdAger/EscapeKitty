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
from Nerds.utils.common import get_md5


def trans_datetime(value):
    if value:
        return datetime.strptime(value, '%Y-%m-%d %H:%M')
    else:
        return value


def trans_date(value):
    if value:
        return datetime.strptime(value, '%Y-%m-%d')
    else:
        return value


def trans_int(value):
    return int(value)


def edit_school(value):
    if value:
        return value
    else:
        return "湖南科技大学"


def is_blacklist(value):
    # 为兼容不同数据库此处不用Python中的bool类型
    if value == "blacklist":
        return 1
    else:
        return 0


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
        input_processor=MapCompose(trans_date)
    )
    end_time = scrapy.Field(
        input_processor=MapCompose(trans_date)
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
            self['is_practice'], self['url'], self['tianyan_company_url'],
            self['crawl_time'].strftime(SQL_DATETIME_FORMAT)
        )
        return insert_sql, params


class HnustCareersItemLoader(ItemLoader):
    default_output_processor = TakeFirst()


class HnustCareersItem(scrapy.Item):
    career_talk_id = scrapy.Field()
    url = scrapy.Field()
    tianyan_company_url = scrapy.Field()
    company_name = scrapy.Field()
    professionals = scrapy.Field()
    company_property = scrapy.Field()
    industry_category = scrapy.Field()
    city_name = scrapy.Field()
    meet_name = scrapy.Field()
    meet_time = scrapy.Field(
        input_processor=MapCompose(trans_datetime)
    )
    school_name = scrapy.Field(
        input_processor=MapCompose(edit_school)
    )
    address = scrapy.Field()
    crawl_time = scrapy.Field()

    def get_insert_sql(self):
        insert_sql = """
            INSERT INTO hnust_careers(career_talk_id, url, tianyan_company_url, company_name,
            professionals, company_property, industry_category, city_name, meet_name, 
            meet_time, school_name, address, crawl_time) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE meet_time=VALUES(meet_time), address=VALUES(address)
        """

        params = (
            self["career_talk_id"], self["url"], self["tianyan_company_url"], self["company_name"],
            self["professionals"], self["company_property"], self["industry_category"], self["city_name"],
            self["meet_name"], self["meet_time"], self["school_name"], self["address"],
            self["crawl_time"].strftime(SQL_DATETIME_FORMAT)
        )

        return insert_sql, params


class HnustJobfairsItemLoader(ItemLoader):
    default_output_processor = TakeFirst()


class HnustJobfairsItem(scrapy.Item):
    fair_id = scrapy.Field()
    url = scrapy.Field()
    title = scrapy.Field()
    organisers = scrapy.Field()
    school_name = scrapy.Field()
    address = scrapy.Field()
    meet_time = scrapy.Field(
        input_processor=MapCompose(trans_datetime)
    )
    # 参与企业数
    plan_c_count = scrapy.Field(
        input_processor=MapCompose(trans_int)
    )
    crawl_time = scrapy.Field()

    def get_insert_sql(self):
        insert_sql = """
            INSERT INTO hnust_jobfairs(fair_id, url, title, organisers, school_name, address, meet_time, plan_c_count,
            crawl_time)VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
            address=VALUES(address), meet_time=VALUES(meet_time), plan_c_count=VALUES(plan_c_count)
        """
        params = (
            self["fair_id"], self["url"], self["title"], self["organisers"], self["school_name"], self["address"],
            self["meet_time"], self["plan_c_count"], self["crawl_time"].strftime(SQL_DATETIME_FORMAT)
        )

        return insert_sql, params


class ICUItemLoader(ItemLoader):
    default_output_processor = TakeFirst()


class ICUItem(scrapy.Item):
    company_id = scrapy.Field(
        input_processor=MapCompose(get_md5)
    )
    company_name = scrapy.Field()
    time_desc = scrapy.Field()
    is_blacklist = scrapy.Field(
        input_processor=MapCompose(is_blacklist)
    )
    crawl_time = scrapy.Field()

    def get_insert_sql(self):
        insert_sql = """
            INSERT INTO ICU_list(company_id, company_name, time_desc, is_blacklist, crawl_time)
            VALUES(%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE is_blacklist=VALUES(is_blacklist), time_desc=VALUES(time_desc)
        """
        params = (
            self["company_id"], self["company_name"], self["time_desc"], self["is_blacklist"], self["crawl_time"].strftime(SQL_DATETIME_FORMAT)
        )

        return insert_sql, params
