from django.shortcuts import render
from django.views.generic.base import View
from Kitty.models import JobsType, JobfairsType, CareersType
from django.http import HttpResponse
import json
from elasticsearch import Elasticsearch
from datetime import datetime
import MySQLdb
import MySQLdb.cursors
from Nerds.main import run_spider
from pyecharts import Bar, Pie
from django_echarts.views.backend import EChartsBackendView
from .charts import charts

db = MySQLdb.connect("localhost", "root", "33Miss77###", "hnjy", charset='utf8')
client = Elasticsearch(hosts=["127.0.0.1"])


def select_type(s_type):
    value = {"s_type": "", "key": ""}
    if s_type == "careers":
        value["s_type"] = CareersType.search()
        value["key"] = "company_name"
    elif s_type == "jobfairs":
        value["s_type"] = JobfairsType.search()
        value["key"] = "title"
    else:
        value["s_type"] = JobsType.search()
        value["key"] = "job_name"
    return value


# Create your views here.
class SearchSuggest(View):
    def get(self, request):
        key_words = request.GET.get("s", "")
        s_type = request.GET.get("s_type", "")
        value = select_type(s_type)
        re_datas = []
        if key_words:
            s = value["s_type"]
            s = s.suggest('my_suggest', key_words, completion={
                "field": "suggest", "fuzzy": {
                    "fuzziness": 2
                },
                "size": 10
            })
            suggestions = s.execute_suggest()
            for match in suggestions.my_suggest[0].options:
                sourse = match._source
                re_datas.append(sourse[value["key"]])
        return HttpResponse(json.dumps(re_datas), content_type="application/json")


class SearchView(View):
    def get(self, request):
        key_words = request.GET.get("q", "")
        s_type = request.GET.get("s_type", "jobs")
        page = request.GET.get("p", "1")
        try:
            page = int(page)
        except:
            page = 1
        start_time = datetime.now()
        if s_type == "careers":
            response = client.search(
                index="careers",
                body={
                    "query": {
                        "multi_match": {
                            "query": key_words,
                            "fields": ["industry_category", "school_name", "company_name", "professionals"]
                        }
                    },
                    "from": (page - 1) * 10,
                    "size": 10,
                    "highlight": {
                        "pre_tags": ['<span class="keyWord">'],
                        "post_tags": ['</span>'],
                        "fields": {
                            "industry_category": {},
                            "school_name": {},
                            "company_name": {},
                            "professionals": {},
                        }
                    }
                }
            )
            end_time = datetime.now()
            last_seconds = (end_time - start_time).total_seconds()
            total_nums = response["hits"]["total"]
            if (page % 10) > 0:
                page_nums = int(total_nums / 10) + 1
            else:
                page_nums = int(total_nums / 10)
            hit_list = []
            for hit in response["hits"]["hits"]:
                hit_dict = {}
                if "industry_category" in hit["highlight"]:
                    hit_dict["industry_category"] = "".join(hit["highlight"]["industry_category"])
                else:
                    hit_dict["industry_category"] = hit["_source"]["industry_category"]

                if "school_name" in hit["highlight"]:
                    hit_dict["school_name"] = "".join(hit["highlight"]["school_name"])
                else:
                    hit_dict["school_name"] = hit["_source"]["school_name"]

                if "company_name" in hit["highlight"]:
                    hit_dict["company_name"] = "".join(hit["highlight"]["company_name"])
                else:
                    hit_dict["company_name"] = hit["_source"]["company_name"]

                if "professionals" in hit["highlight"]:
                    hit_dict["professionals"] = "".join(hit["highlight"]["professionals"])
                else:
                    hit_dict["professionals"] = hit["_source"]["professionals"]

                hit_dict["meet_time"] = hit["_source"]["meet_time"]
                hit_dict["url"] = hit["_source"]["url"]
                hit_dict["tianyan_company_url"] = hit["_source"]["tianyan_company_url"]
                hit_dict["company_property"] = hit["_source"]["company_property"]
                hit_dict["city_name"] = hit["_source"]["city_name"]
                hit_dict["meet_name"] = hit["_source"]["meet_name"]
                hit_dict["address"] = hit["_source"]["address"]
                hit_dict["score"] = hit["_score"]

                cursor = db.cursor()
                try:
                    sql = """
                            SELECT is_blacklist FROM ICU_list
                            WHERE company_name = "{0}"
                                """.format(hit["_source"]["company_name"].strip())
                    cursor.execute(sql)
                    result = cursor.fetchall()[0][0]
                    if result == 1:
                        hit_dict["css"] = "btn btn-warning"
                        hit_dict["c_value"] = "此为996公司"
                    elif result == 0:
                        hit_dict["css"] = "btn btn-success"
                        hit_dict["c_value"] = "此为965公司"
                except:
                    hit_dict["css"] = "btn btn-info"
                    hit_dict["c_value"] = "未收录工作时长"
                cursor.close()
                hit_list.append(hit_dict)

            return render(request, "careers_result.html", {"all_hits": hit_list, "key_words": key_words,
                                                        "page": page, "total_nums": total_nums,
                                                        "page_nums": page_nums, "last_seconds": last_seconds,
                                                        "info_nums": self.get_info_nums()})
        elif s_type == "jobfairs":
            response = client.search(
                index="jobfairs",
                body={
                    "query": {
                        "multi_match": {
                            "query": key_words,
                            "fields": ["title", "school_name", "address"]
                        }
                    },
                    "from": (page - 1) * 10,
                    "size": 10,
                    "highlight": {
                        "pre_tags": ['<span class="keyWord">'],
                        "post_tags": ['</span>'],
                        "fields": {
                            "title": {},
                            "school_name": {},
                            "address": {},
                        }
                    }
                }
            )
            end_time = datetime.now()
            last_seconds = (end_time - start_time).total_seconds()
            total_nums = response["hits"]["total"]
            if (page % 10) > 0:
                page_nums = int(total_nums / 10) + 1
            else:
                page_nums = int(total_nums / 10)
            hit_list = []
            for hit in response["hits"]["hits"]:
                hit_dict = {}
                if "title" in hit["highlight"]:
                    hit_dict["title"] = "".join(hit["highlight"]["title"])
                else:
                    hit_dict["title"] = hit["_source"]["title"]

                if "school_name" in hit["highlight"]:
                    hit_dict["school_name"] = "".join(hit["highlight"]["school_name"])
                else:
                    hit_dict["school_name"] = hit["_source"]["school_name"]

                if "address" in hit["highlight"]:
                    hit_dict["address"] = "".join(hit["highlight"]["address"])
                else:
                    hit_dict["address"] = hit["_source"]["address"]

                hit_dict["meet_time"] = hit["_source"]["meet_time"]
                hit_dict["url"] = hit["_source"]["url"]
                # hit_dict["organisers"] = hit["_source"]["organisers"]
                hit_dict["plan_c_count"] = hit["_source"]["plan_c_count"]
                hit_dict["score"] = hit["_score"]

                hit_list.append(hit_dict)

            return render(request, "jobfairs_result.html", {"all_hits": hit_list, "key_words": key_words,
                                                           "page": page, "total_nums": total_nums,
                                                           "page_nums": page_nums, "last_seconds": last_seconds,
                                                           "info_nums": self.get_info_nums()})
        else:
            response = client.search(
                index="jobs",
                body={
                    "query": {
                        "multi_match": {
                            "query": key_words,
                            "fields": ["job_name", "about_major", "company_name", "keywords"]
                        }
                    },
                    "from": (page-1)*10,
                    "size": 10,
                    "highlight": {
                        "pre_tags": ['<span class="keyWord">'],
                        "post_tags": ['</span>'],
                        "fields": {
                            "job_name": {},
                            "keywords": {},
                            "about_major": {},
                            "company_name": {},
                        }
                    }
                }
            )
            end_time = datetime.now()
            last_seconds = (end_time-start_time).total_seconds()
            total_nums = response["hits"]["total"]
            if (page % 10) > 0:
                page_nums = int(total_nums/10) + 1
            else:
                page_nums = int(total_nums/10)
            hit_list = []
            for hit in response["hits"]["hits"]:
                hit_dict = {}
                if "job_name" in hit["highlight"]:
                    hit_dict["job_name"] = "".join(hit["highlight"]["job_name"])
                else:
                    hit_dict["job_name"] = hit["_source"]["job_name"]

                if "about_major" in hit["highlight"]:
                    hit_dict["about_major"] = "".join(hit["highlight"]["about_major"])
                else:
                    hit_dict["about_major"] = hit["_source"]["about_major"]

                if "company_name" in hit["highlight"]:
                    hit_dict["company_name"] = "".join(hit["highlight"]["company_name"])
                else:
                    hit_dict["company_name"] = hit["_source"]["company_name"]

                if "keywords" in hit["highlight"]:
                    hit_dict["keywords"] = "".join(hit["highlight"]["keywords"])
                else:
                    hit_dict["keywords"] = hit["_source"]["keywords"]

                hit_dict["publish_time"] = hit["_source"]["publish_time"]
                hit_dict["url"] = hit["_source"]["url"]
                hit_dict["tianyan_company_url"] = hit["_source"]["tianyan_company_url"]
                hit_dict["salary"] = hit["_source"]["salary"]
                hit_dict["city_name"] = hit["_source"]["city_name"]
                hit_dict["degree_require"] = hit["_source"]["degree_require"]
                hit_dict["end_time"] = hit["_source"]["end_time"]
                hit_dict["scale"] = hit["_source"]["scale"]
                hit_dict["industry_category"] = hit["_source"]["industry_category"]
                hit_dict["is_practice"] = hit["_source"]["is_practice"]
                hit_dict["score"] = hit["_score"]

                is_practice = "正式岗位" if hit_dict["is_practice"] == 0 else "实习岗位"

                cursor = db.cursor()
                try:
                    sql = """
                        SELECT is_blacklist FROM ICU_list
                        WHERE company_name = "{0}"
                    """.format(hit["_source"]["company_name"].strip())
                    cursor.execute(sql)
                    result = cursor.fetchall()[0][0]
                    if result == 1:
                        hit_dict["css"] = "btn btn-warning"
                        hit_dict["c_value"] = "此为996公司"
                    elif result == 0:
                        hit_dict["css"] = "btn btn-success"
                        hit_dict["c_value"] = "此为965公司"
                except:
                    hit_dict["css"] = "btn btn-info"
                    hit_dict["c_value"] = "未收录工作时长"
                cursor.close()
                hit_list.append(hit_dict)

            return render(request, "jobs_result.html", {"all_hits": hit_list, "key_words": key_words,
                                                   "page": page, "total_nums": total_nums,
                                                   "page_nums": page_nums, "last_seconds": last_seconds,
                                                   "is_practice": is_practice, "info_nums": self.get_info_nums()})

    def get_info_nums(self):
        cursor = db.cursor()
        job_sql = "SELECT COUNT(*) FROM hnust_jobs"
        career_sql = "SELECT COUNT(*) FROM hnust_careers"
        jobfair_sql = "SELECT COUNT(*) FROM hnust_jobfairs"
        results = []
        try:
            cursor.execute(job_sql)
            results.append(cursor.fetchone()[0])
            cursor.execute(career_sql)
            results.append(cursor.fetchone()[0])
            cursor.execute(jobfair_sql)
            results.append(cursor.fetchone()[0])
        except:
            results = [0, 0, 0]
        cursor.close()
        return results


class AdminView(View):
    def get(self, request):
        return render(request, "admin.html", {"is_disabled": ""})


class CrawlView(View):
    def get(self, request):
        # run_spider()
        return render(request, "admin.html", {"is_disabled": 'disabled="disabled"',
                                              "crawling": 1})


class EChartsTemplate(EChartsBackendView):
    template_name = 'charts.html'

    def get_echarts_instance(self, *args, **kwargs):
        name = self.request.GET.get('name', 'bar')
        return charts(name)


if __name__ == "__main__":
    a = EChartsTemplate()
    print(a.get_city_nums())