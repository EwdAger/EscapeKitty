import MySQLdb
import MySQLdb.cursors
from pyecharts import Bar, Pie, Line, WordCloud
from django_echarts.views.backend import EChartsBackendView
import collections

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
    return timelist[::-1], nums[::-1]


def get_wordcloud():
    data = {"计算机类": 0}
    major = []
    nums = []
    cursor = db.cursor()
    sql = """
            SELECT about_major, COUNT(*) as nums 
            FROM hnust_jobs 
            GROUP BY about_major
            ORDER BY nums
             DESC LIMIT 140
        """
    try:
        cursor.execute(sql)
        result = cursor.fetchall()
        for i in result:
            if "不限" in i[0]:
                continue
            if "计算" in i[0] or "软件" in i[0]:
                data["计算机类"] += i[1]
            else:
                buff = i[0].split("，")
                for j in buff:
                    if j not in data:
                        data[j] = i[1]
                    else:
                        data[j] += i[1]
        for k, v in data.items():
            major.append(k)
            nums.append(v)
    except:
        pass
    cursor.close()
    return major, nums


def do_salary(name):
    sql = """
        SELECT job_name,  salary ,  COUNT(*)
        FROM hnust_jobs 
        GROUP BY job_name,  salary
        HAVING job_name LIKE "%{0}%"
    """.format(name)
    data = collections.OrderedDict()
    cursor = db.cursor()
    try:
        cursor.execute(sql)
        result = cursor.fetchall()
        for i in result:
            temp = i[1]
            temp = temp.replace("/月", "").replace("K", "")
            temp = temp.split("-")
            temp = [int(float(i)) for i in temp]
            for j in range(2):
                if temp[j] > 50:
                    continue
                if str(temp[j]) in data:
                    data[str(temp[j])] += i[2]
                else:
                    data[str(temp[j])] = i[2]
    except:
        pass
    cursor.close()
    data = sorted(data.items(), key=lambda item: int(item[0]))
    salary = [str(k)+"K/月" for k, v in data]
    nums = [v for k, v in data]

    return salary, nums


def do_nums(name):
    nums = 0
    sql = """
        SELECT job_name, COUNT(*)
        FROM hnust_jobs 
        GROUP BY job_name
        HAVING job_name LIKE "%{0}%"
    """.format(name)
    data = collections.OrderedDict()
    cursor = db.cursor()
    try:
        cursor.execute(sql)
        result = cursor.fetchall()
        for i in result:
            nums += int(i[1])
    except:
        pass
    cursor.close()

    return nums




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

    elif name == "com":
        com = Line("", "")
        data = []
        for name in ["python", "php", "java", "c++", "前端", "后端", "安卓"]:
            salary, nums = do_salary(name)
            data.append(salary)
            com.add(name, salary, nums)
        return com

    elif name == "jobs":
        jobs = Bar("", "")
        names = []
        nums = []
        for name in ["python", "php", "java", "c++", "前端", "后端", "安卓"]:
            num = do_nums(name)
            names.append(name)
            nums.append(num)
        jobs.add("数量", names, nums)

        return jobs
