from datetime import datetime
from elasticsearch_dsl import DocType, Date, Nested, Boolean, \
    analyzer, InnerObjectWrapper, Completion, Keyword, Text, Integer

from elasticsearch_dsl.analysis import CustomAnalyzer as _CustomAnalyzer

from elasticsearch_dsl.connections import connections
connections.create_connection(hosts=["localhost"])

class CustomAnalyzer(_CustomAnalyzer):
    def get_analysis_definition(self):
        return {}


ik_analyzer = CustomAnalyzer("ik_max_word", filter=["lowercase"])


class JobsType(DocType):
    # 岗位信息类型
    suggest = Completion(analyzer=ik_analyzer)
    job_name = Text(analyzer="ik_max_word")
    url = Keyword()
    salary = Keyword()
    city_name = Text(analyzer="ik_max_word")
    about_major = Text(analyzer="ik_max_word")
    degree_require = Text(analyzer="ik_max_word")
    company_name = Keyword()
    tianyan_company_url = Keyword()
    publish_time = Date()
    end_time = Date()
    scale = Keyword()
    industry_category = Text(analyzer="ik_max_word")
    keywords = Text(analyzer="ik_max_word")
    is_practice = Integer()

    class Meta:
        index = "jobs"
        doc_type = "jobs_type"


class JobfairsType(DocType):
    # 双选会
    suggest = Completion(analyzer=ik_analyzer)
    url = Keyword()
    title = Keyword()
    school_name = Text(analyzer="ik_max_word")
    address = Text(analyzer="ik_max_word")
    meet_time = Date()
    # 参与企业数
    plan_c_count = Keyword()

    class Meta:
        index = "jobfairs"
        doc_type = "jobfairs_type"


class CareersType(DocType):
    # 招聘会
    suggest = Completion(analyzer=ik_analyzer)
    url = Keyword()
    tianyan_company_url = Keyword()
    company_name = Keyword()
    professionals = Text(analyzer="ik_max_word")
    company_property = Text(analyzer="ik_max_word")
    industry_category = Text(analyzer="ik_max_word")
    city_name = Text(analyzer="ik_max_word")
    meet_name = Keyword()
    school_name = Keyword()
    meet_time = Date()
    address = Keyword()

    class Meta:
        index = "careers"
        doc_type = "careers_type"

if __name__ == "__main__":
    JobsType.init()
    JobfairsType.init()
    CareersType.init()
