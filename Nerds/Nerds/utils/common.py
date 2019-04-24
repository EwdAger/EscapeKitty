import hashlib
from fake_useragent import UserAgent


def get_md5(url):
    if isinstance(url, str):
        url = url.encode("utf-8")
    m = hashlib.md5()
    m.update(url)
    return m.hexdigest()

def random_user_agent():
    ua = UserAgent()
    return ua.random