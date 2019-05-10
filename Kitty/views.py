from django.shortcuts import render
from django.views.generic.base import View
from Kitty.models import JobsType, JobfairsType, CareersType
from django.http import HttpResponse
import json
from elasticsearch import Elasticsearch


client = Elasticsearch(hosts=["127.0.0.1"])


# Create your views here.
class SearchSuggest(View):
    def get(self, request):
        key_words = request.GET.get("s", "")
        re_datas = []
        if key_words:
            s = JobsType.search()
            s = s.suggest('my_suggest', key_words, completion={
                "field": "suggest", "fuzzy": {
                    "fuzziness": 2
                },
                "size": 10
            })
            suggestions = s.execute_suggest()
            for match in suggestions.my_suggest[0].options:
                sourse = match._source
                re_datas.append(sourse["job_name"])
        return HttpResponse(json.dumps(re_datas), content_type="application/json")


class SearchView(View):
    def get(self, request):
        key_words = request.GET.get("q", "")
        response = client.search(
            index="jobs",
            body={
                "query": {
                    "multi_match": {
                        "query": key_words,
                        "fields": ["job_name", "url", "about_major", "company_name", "keywords"]
                    }
                },
                "from": 0,
                "size": 10,
                "highlight": {
                    "pre_tags": ['<span class="keyWord">'],
                    "post_tags": ['</span>'],
                    "fields": {
                        "job_name": {},
                        "keywords": {}
                    }
                }
            }
        )

        total_nums = response["hits"]["total"]
        hit_list = []
        for hit in response["hits"]["hits"]:
            hit_dict = {}
            if "job_name" in hit["highlight"]:
                hit_dict["job_name"] = "".join(hit["highlight"]["job_name"])
            else:
                hit_dict["job_name"] = hit["_source"]["job_name"]

            hit_dict["publish_time"] = hit["_source"]["publish_time"]
            hit_dict["url"] = hit["_source"]["url"]
            hit_dict["score"] = hit["_score"]

            hit_list.append(hit_dict)
        return render(request, "result.html", {"all_hits": hit_list, "key_words": key_words})
