import MySQLdb
import MySQLdb.cursors
from pyecharts import Bar, Pie, Line, WordCloud
from django_echarts.views.backend import EChartsBackendView

db = MySQLdb.connect("localhost", "root", "33Miss77###", "hnjy", charset='utf8')

def get_city_tops():
    city_name = []
    nums = []
    cursor = db.cursor()
    sql = """
        SELECT city_name , COUNT(*) as nums FROM hnust_jobs GROUP BY city_name ORDER BY nums  DESC LIMIT 11
    """
    try:
        cursor.execute(sql)
        result = cursor.fetchall()
        for i in result:
            if i[0] == "全国":
                continue
            city_name.append(i[0])
            nums.append(i[1])
    except:
        pass
    cursor.close()
    return city_name, nums


def get_city_nums():
    city_name = []
    nums = []
    cursor = db.cursor()
    sql = """
        SELECT city_name , COUNT(*) as nums FROM hnust_jobs GROUP BY city_name ORDER BY nums  DESC LIMIT 51
    """
    try:
        cursor.execute(sql)
        result = cursor.fetchall()
        for i in result:
            if i[0] == "全国":
                continue
            city_name.append(i[0].strip("市"))
            nums.append(i[1])
    except:
        pass
    cursor.close()
    return city_name, nums


def get_timelist():
    timelist = []
    nums = []
    cursor = db.cursor()
    sql = """
        SELECT DATE_FORMAT(publish_time, "%Y-%m")  as date, COUNT(*) as nums 
        FROM hnust_jobs 
        GROUP BY date
        ORDER BY date
        DESC
        LIMIT 10
    """
    try:
        cursor.execute(sql)
        result = cursor.fetchall()
        for i in result:
            timelist.append(i[0])
            nums.append(int(i[1]))
    except:
        pass
    cursor.close()
    return timelist, nums


def get_wordcloud():
    major = []
    nums = []
    cursor = db.cursor()
    sql = """
            SELECT about_major, COUNT(*) as nums 
            FROM hnust_jobs 
            GROUP BY about_major
            ORDER BY nums
             DESC LIMIT 100
        """
    try:
        cursor.execute(sql)
        result = cursor.fetchall()
        for i in result:
            if i[0] == "不限专业":
                continue
            major.append(i[0])
            nums.append(int(i[1]))
    except:
        pass
    cursor.close()
    return major[::-1], nums[::-1]


def charts(name):
    if name == "bar":
        bar = Bar("热门招聘城市排行", "招聘职位数量")
        city_name, city_nums = get_city_tops()
        bar.add("数量", city_name, city_nums)
        return bar

    elif name == "pie":
        pie = Pie("", "")
        city_name, city_nums = get_city_nums()
        pie.add("数量", city_name, city_nums)
        return pie

    elif name == "line":
        line = Line("月度招聘信息发布统计", "发布时间")
        timelist, nums = get_timelist()
        line.add("数量", timelist, nums)
        return line

    elif name == "word":
        word = WordCloud(width=800, height=400)
        major, nums = get_wordcloud()
        word.add("数量", major, nums, shape="diamond")
        return word
