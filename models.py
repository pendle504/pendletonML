### models ####
import sqlite3
from datetime import date, datetime, timedelta
import pandas as pd
import random
from pandas_datareader import data,wb
import pandas_datareader.data as web
from pandas_datareader.data import Options
from scipy.stats import mstats
import os
import sys
import re
from urllib.request import urlopen
import time
from bs4 import BeautifulSoup
def Filebasket():
    filebasket = list()
    for fn in os.listdir():
        try:
            if fn.split('.')[1] == 'csv':
                filebasket.append(fn)
        except:
            pass
    return filebasket
def Connection():
    conn = sqlite3.connect('pend.db')
    c = conn.cursor()
    return conn,c
conn,c = Connection()

def dict_builder(key_list,value_list):
    my_dict = {}
    for k,v in zip(key_list,value_list):
        my_dict[k] = v
    return my_dict

def ndr(row):
    bogey = 0.01
    y = row['dayreturn']/2
    if y > 0:
        if row['nextdayreturn'] >= bogey:
            return 2
        elif row['nextdayreturn'] < -bogey:
            return 1
        else:
            return 0
    else:
        if row['nextdayreturn'] > bogey:
            return 1
        elif row['nextdayreturn'] <= -bogey:
            return 2
        else:
            return 0

def wins_df(df):
    return df.apply(using_mstats,axis=1)

def using_mstats(s):
    return mstats.winsorize(s, limits=[0.05, 0.05])

def fetchOpen(row):
    symbol = row['symbol']
    try:
        link = "https://www.google.com/finance?q="
        url = link+"%s" % (symbol)
        u = urlopen(url)
        content = u.read()
        content = content.decode('UTF-8')
        soup = BeautifulSoup(content,'lxml')
        open_px = soup.find('td',{'data-snapfield':'open'})
        return float(open_px.next_sibling.next_sibling.get_text().strip())
    except:
        return None
        # link = "https://www.google.com/finance?q="
        # url = link+"%s" % (symbol)
        # u = urlopen(url)
        # content = u.read()
        # content = content.decode('UTF-8')
        # soup = BeautifulSoup(content,'lxml')
        # print(soup)
def fetchExtra(symbol):
    try:
        link = "https://www.google.com/finance?q="
        url = link+"%s" % (symbol)
        u = urlopen(url)
        content = u.read()
        content = content.decode('UTF-8')
        soup = BeautifulSoup(content,'lxml')
        mkt_cap = soup.find('td',{'data-snapfield':'market_cap'}).next_sibling.next_sibling.get_text().strip()
        pe = soup.find('td',{'data-snapfield':'pe_ratio'}).next_sibling.next_sibling.get_text().strip()
        beta = soup.find('td',{'data-snapfield':'beta'}).next_sibling.next_sibling.get_text().strip()
        inst_own = soup.find('td',{'data-snapfield':'inst_own'}).next_sibling.next_sibling.get_text().strip()
        return mkt_cap,pe,beta,inst_own
    except:
        return None,None,None,None
