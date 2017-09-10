import sqlite3
import pandas as pd
import numpy as np
from models import Connection
import pandas_datareader.data as web
from datetime import datetime,date
from pandas.tseries.offsets import BDay
from googlefinance import getQuotes
import json
import re
conn,c = Connection()

class DJFear:
    def __init__(self,df_join):
        df = pd.read_sql('''SELECT * FROM djTable''',conn)
        df.Date = pd.DatetimeIndex(df.Date)
        if df['Date'].max() < datetime.now().date()-BDay(1):
            y = DJUpdate()
            df = pd.read_sql('''SELECT * FROM djTable''',conn)
        # df['Date'] = pd.to_datetime(df['Date'],format='%m/%d/%Y')
        self.df_join = df_join
        self.df = df
        self._fear()
    def _fear(self):
        self.df['5DayMom']=self.df['DJTMNMO'] / self.df['DJTMNMO'].shift(5) -1
        self.df['5DayFear']= ((self.df['DJTMNAB'] / self.df['DJTMNAB'].shift(5) - 1) + (self.df['DJTMNQU'] / self.df['DJTMNQU'].shift(5) - 1)) - ((self.df['DJTMNSV'] / self.df['DJTMNSV'].shift(5) - 1) + (self.df['DJTMNSS'] / self.df['DJTMNSS'].shift(5) - 1))
        self.df['3MoMom'] = self.df['DJTMNMO'] / self.df['DJTMNMO'].shift(63) -1
        self.df['3MoFear'] = ((self.df['DJTMNAB'] / self.df['DJTMNAB'].shift(63) - 1) + (self.df['DJTMNQU'] / self.df['DJTMNQU'].shift(63) - 1)) - ((self.df['DJTMNSV'] / self.df['DJTMNSV'].shift(63) - 1) + (self.df['DJTMNSS'] / self.df['DJTMNSS'].shift(63) - 1))
        self.df = self.df.set_index('Date')
        self.junk_spread()
        self.df = self.df[['3MoMom','3MoFear','5DayMom','5DayFear','JSpread','5DayJunk','3MoJunk']]
        self.join()
    def join(self):
        self.df = self.df_join.merge(self.df,how='left',left_index=True,right_index=True)
    def junk_spread(self):
        self.junkdf = web.DataReader('BAMLH0A0HYM2','fred',datetime(2000,1,1),datetime.today())
        self.junkdf['JSpread'] = self.junkdf['BAMLH0A0HYM2']
        self.junkdf['5DayJunk'] = self.junkdf['BAMLH0A0HYM2'] / self.junkdf['BAMLH0A0HYM2'].shift(5) -1
        self.junkdf['3MoJunk'] = self.junkdf['BAMLH0A0HYM2'] / self.junkdf['BAMLH0A0HYM2'].shift(63) -1
        self.df = self.df.merge(self.junkdf,how='left',left_index=True,right_index=True)

class DJUpdate:
    def __init__ (self):
        self.df = pd.read_sql('''SELECT * FROM djTable''',conn,index_col='Date')
        self.ticker_list = self.df.columns
        self.db_update()
    def db_(self):
        self.df.to_sql('djTable',conn,if_exists='append',index=True,index_label='Date')
    def db_update(self):
        self.date = datetime.today()-BDay(1)
        self.date = self.date.date()
        self.columns = ['Date']
        self.data = [self.date]
        for i in self.ticker_list:
            a = json.dumps(getQuotes(i),indent=2)
            b = json.loads(a[1:len(a)-1])
            px = float(re.sub("[^\d\.]","",b['LastTradePrice']))
            self.columns.append(i)
            self.data.append(px)
        self.data = [self.data]
        df = pd.DataFrame(self.data,columns=self.columns).set_index('Date')
        df.to_sql('djTable',conn,if_exists='append',index=True,index_label='Date')
