from scrapy.cmdline import execute

import sys
import os

def run_spider():
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    execute(["scrapy", "crawl", "hnust"])

if __name__ == "__main__":
    run_spider()
