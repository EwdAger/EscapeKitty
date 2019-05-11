# -*- coding: utf-8 -*-
import scrapy
import json
from Nerds.items import HnustJobModelItem, HnustJobModelItemLoader, HnustCareersItem, HnustCareersItemLoader, \
    HnustJobfairsItem, HnustJobfairsItemLoader, ICUItem, ICUItemLoader
from datetime import datetime
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals


class HnustSpider(scrapy.Spider):
    def __init__(self):
        pass

    name = 'hnust'
    allowed_domains = ['jy.hnust.edu.cn']
    start_urls = ['http://jy.hnust.edu.cn/']


    def start_requests(self):
        # 爬取前两万条岗位信息
        for i in range(2):
            yield scrapy.Request("http://jy.hnust.edu.cn/module/getjobs?start_page=1&type_id=-1&k=&is_practice={0}&count=20000&start=1".format(i),
                                 callback=self.handle_jobs)
        # 爬取宣讲会信息
        for i in ["inner", "outer"]:
            yield scrapy.Request("http://jy.hnust.edu.cn/module/getcareers?start_page=1&k=&type={0}&day=&count=10000&start=1".format(i),
                                 callback=self.handle_careers)
        # 爬取双选会信息
        yield scrapy.Request("http://jy.hnust.edu.cn/module/getjobfairs?start_page=1&keyword=&count=300&start=1", callback=self.handle_jobfairs)
        for i in ["blacklist", "whitelist"]:
            yield scrapy.Request("https://github.com/996icu/996.ICU/tree/master/{0}".format(i), meta={"type": i},
                                 callback=self.get_996ICU)

    def handle_jobs(self, response):
        jobs = json.loads(response.body_as_unicode())["data"]
        for job in jobs:
            # 不爬取过期信息
            # end_time = job["end_time"]
            # end_time = datetime.strptime(end_time, '%Y-%m-%d')
            # now = datetime.now()
            # if end_time < now:
            #     continue
            item_loader = HnustJobModelItemLoader(item=HnustJobModelItem())
            item_loader.add_value("job_name", job['job_name'])
            item_loader.add_value("url", "http://jy.hnust.edu.cn/detail/job?id={0}&menu_id=".format(job["publish_id"]))
            item_loader.add_value("publish_id", job["publish_id"])
            item_loader.add_value("salary", job["salary"])
            item_loader.add_value("city_name", job["city_name"])
            item_loader.add_value("about_major", job["about_major"])
            item_loader.add_value("degree_require", job["degree_require"])
            item_loader.add_value("scale", job["scale"])
            item_loader.add_value("industry_category", job["industry_category"])
            item_loader.add_value("keywords", job["keywords"])
            item_loader.add_value("is_practice", job["is_practice"])
            item_loader.add_value("company_name", job["company_name"])
            item_loader.add_value("publish_time", job["publish_time"])
            item_loader.add_value("end_time", job["end_time"])
            item_loader.add_value("tianyan_company_url", "https://www.tianyancha.com/search?key="+job["company_name"])
            item_loader.add_value("crawl_time", datetime.now())
            job_item = item_loader.load_item()
            yield job_item

    def handle_careers(self, response):
        careers = json.loads(response.body_as_unicode())["data"]
        for career in careers:
            # 不爬取过期信息
            # end_time = career["meet_time"]
            # end_time = datetime.strptime(meet_time, '%Y-%m-%d')
            # now = datetime.now()
            # if end_time < now:
            #     continue
            item_loader = HnustCareersItemLoader(item=HnustCareersItem())
            item_loader.add_value("career_talk_id", career["career_talk_id"])
            item_loader.add_value("url", "http://jy.hnust.edu.cn/detail/career?id="+career["career_talk_id"])
            item_loader.add_value("tianyan_company_url", "https://www.tianyancha.com/search?key="+career["company_name"])
            item_loader.add_value("company_name", career["company_name"])
            item_loader.add_value("professionals", career["professionals"])
            item_loader.add_value("company_property", career["company_property"])
            item_loader.add_value("industry_category", career["industry_category"])
            item_loader.add_value("city_name", career["city_name"])
            item_loader.add_value("meet_name", career["meet_name"])
            item_loader.add_value("meet_time", " ".join([career["meet_day"], career["meet_time"]]))
            item_loader.add_value("school_name", career["school_name"])
            item_loader.add_value("address", career["address"])
            item_loader.add_value("crawl_time", datetime.now())
            career_loader = item_loader.load_item()
            yield career_loader

    def handle_jobfairs(self, response):
        jobfairs = json.loads(response.body_as_unicode())["data"]
        for jobfair in jobfairs:
            # 不爬取过期信息
            #     end_time = jobfair["meet_time"]
            #     end_time = datetime.strptime(end_time, '%Y-%m-%d')
            #     now = datetime.now()
            #     if end_time < now:
            #         continue
            item_loader = HnustJobfairsItemLoader(item=HnustJobfairsItem())
            item_loader.add_value("fair_id", jobfair["fair_id"])
            item_loader.add_value("url", "http://jy.hnust.edu.cn/detail/jobfair?id="+jobfair["fair_id"])
            item_loader.add_value("title", jobfair["title"])
            item_loader.add_value("organisers", jobfair["organisers"])
            item_loader.add_value("school_name", jobfair["school_name"])
            item_loader.add_value("address", jobfair["address"])
            item_loader.add_value("meet_time", " ".join([jobfair["meet_day"], jobfair["meet_time"]]))
            item_loader.add_value("plan_c_count", jobfair["plan_c_count"])
            item_loader.add_value("crawl_time", datetime.now())
            jobfair_loader = item_loader.load_item()
            yield jobfair_loader

    def get_996ICU(self, response):
        list_type = response.meta.get("type")
        table_num = 2 if list_type == "blacklist" else 1
        count_tr = len(response.xpath("//*[@id='readme']/div[2]/article/table[{0}]/tbody/tr".format(table_num)))
        for i in range(1, count_tr+1):
            item_loader = ICUItemLoader(item=ICUItem(), response=response)
            item_loader.add_xpath("company_id", "//*[@id='readme']/div[2]/article/table[{0}]/tbody/tr[{1}]/td[2]/a/text()".format(table_num, i))
            item_loader.add_xpath("company_name", "//*[@id='readme']/div[2]/article/table[{0}]/tbody/tr[{1}]/td[2]/a/text()".format(table_num, i))
            item_loader.add_xpath("time_desc", "//*[@id='readme']/div[2]/article/table[{0}]/tbody/tr[{1}]/td[4]/text()".format(table_num, i))
            item_loader.add_value("is_blacklist", list_type)
            item_loader.add_value("crawl_time", datetime.now())
            ICU_loader = item_loader.load_item()
            yield ICU_loader