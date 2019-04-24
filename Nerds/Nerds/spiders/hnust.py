# -*- coding: utf-8 -*-
import scrapy
import json
from Nerds.items import HnustJobModelItem, HnustJobModelItemLoader


class HnustSpider(scrapy.Spider):
    name = 'hnust'
    allowed_domains = ['jy.hnust.edu.cn']
    start_urls = ['http://jy.hnust.edu.cn/']

    def start_requests(self):
        # 爬取前两万条正式岗位信息
        yield scrapy.Request("http://jy.hnust.edu.cn/module/getjobs?start_page=1&type_id=-1&k=&is_practice=0&count=20000&start=1",
                             callback=self.handle_jobs)
        yield scrapy.Request("http://jy.hnust.edu.cn/module/getjobs?start_page=1&type_id=-1&k=&is_practice=1&count=20000&start=1",
                             callback=self.handle_jobs)

    def handle_jobs(self, response):
        jobs = json.loads(response.body_as_unicode())["data"]
        itemloader = HnustJobModelItemLoader(item=HnustJobModelItem)
        for job in jobs:
            itemloader.add_value("job_name", job['job_name'])
            itemloader.add_value("url", "http://jy.hnust.edu.cn/detail/job?id={0}&menu_id=".format(job["publish_id"]))
            itemloader.add_value("publish_id", job["publish_id"])
            itemloader.add_value("salary", job["salary"])
            itemloader.add_value("city_name", job["city_name"])
            itemloader.add_value("about_major", job["about_major"])
            itemloader.add_value("degree_require", job["degree_require"])
            itemloader.add_value("scale", job["scale"])
            itemloader.add_value("industry_category", job["industry_category"])
            itemloader.add_value("keywords", job["keywords"])
            itemloader.add_value("is_practice", job["is_practice"])
            itemloader.add_value("company_name", job["company_name"])
            itemloader.add_value("publish_time", job["publish_time"])
            itemloader.add_value("end_time", job["end_time"])
            itemloader.add_value("tianyan_company_url", "https://www.tianyancha.com/search?key="+job["company_name"])
        print()