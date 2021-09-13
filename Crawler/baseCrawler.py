import pandas as pd
import requests
from Database.dbmanager import DBManager
from lxml import html
import datetime
from dateutil.parser import parse


class CrawlerBase:

    dbManager = DBManager()
    s = requests.session()
    headers = [
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
                          " Chrome/89.0.4389.82 Safari/537.36"
        },
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.85 Safari/537.36 Edg/90.0.818.49"
        },
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:86.0) Gecko/20100101 Firefox/86.0"
        }
    ]

    param = {}
    url = ""
    def __init__(self):
        self.s.headers.update(self.headers[0])
        self.setup()

    def setup(self):
        self.param = {}
        self.url = ""

    def test(self):
        print(f"param : {self.param} url : {self.url}")

    def run(self):
        pass