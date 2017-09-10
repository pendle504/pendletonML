import requests
import bs4
from models import Connection
conn,c = Connection()
import sqlite3
from datetime import datetime
import pandas as pd
def get_earning_data(date):
    html = requests.get("https://biz.yahoo.com/research/earncal/{}.html".format(date), headers={"User-Agent": "Mozilla/5.0 (Windows NT 6.3; rv:36.0) Gecko/20100101 Firefox/36.0"}).text
    soup = bs4.BeautifulSoup(html,'lxml')
    quotes = []
    for tr in soup.find_all("tr"):
        if len(tr.contents) > 3:
            if len(tr.contents[1].contents) > 0:
                if tr.contents[1].contents[0].name == "a":
                    if tr.contents[1].contents[0]["href"].startswith("http://finance.yahoo.com/q?s="):
                        quotes.append({     "name"  : tr.contents[0].text
                                           ,"symbol": tr.contents[1].contents[0].text
                                           ,"url"   : tr.contents[1].contents[0]["href"]
                                           ,"eps"   : tr.contents[2].text if len(tr.contents) == 6 else u'N/A'
                                           ,"time"  : tr.contents[3].text if len(tr.contents) == 6 else tr.contents[2].text
                                       })
    return quotes
# for i in get_earning_data("20120109"):
#     if i['symbol'] == 'MG':
#         print(i)
