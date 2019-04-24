# -*- coding: utf-8 -*-
import scrapy
import json
from Nerds.items import HnustJobModelItem, HnustJobModelItemLoader
from datetime import datetime


class HnustSpider(scrapy.Spider):
    name = 'hnust'
    allowed_domains = ['jy.hnust.edu.cn']
    start_urls = ['http://jy.hnust.edu.cn/']

    def start_requests(self):
        # 爬取前两万条岗位信息
        for i in range(2):
            yield scrapy.Request("http://jy.hnust.edu.cn/module/getjobs?start_page=1&type_id=-1&k=&is_practice={0}&count=5&start=1".format(i),
                                 callback=self.handle_jobs)

    def handle_jobs(self, response):
        jobs = json.loads(response.body_as_unicode())["data"]
        for job in jobs:
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

