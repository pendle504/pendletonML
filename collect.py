import pandas_datareader.data as web
import sqlite3
from datetime import date, datetime, timedelta
import pandas as pd
from models import Connection
import numpy as np
conn,c = Connection()
from feeder import PreviousHigh
class Collect:
    def __init__(self,stockid,symbol,d1,thresh,n):
        self.today = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        self.id = stockid
        self.symbol = symbol
        self.d1 = datetime.strptime(d1,'%Y-%m-%d %H:%M:%S') - timedelta(days=1)
        self.date_list = []
        self.df = pd.DataFrame()
        self.df_sub = pd.DataFrame()
        self.thresh = thresh
        self.n = n
        self.period = None
        self.input = ()
        self.get_price()
    def over_reactions(self):
        self.df_sub = self.df[(self.df['ODR']>self.thresh) | (self.df['ODR']<-self.thresh)].reset_index()
        self.date_list = self.create_date_list()
        if len(self.date_list)>0:
            self.df_sub['stockid'],self.df_sub['price'],self.df_sub['dayreturn'] = self.id,self.df_sub['Close'],self.df_sub['ODR']
            self.df_sub = self.df_sub[['stockid','price','dayreturn','Date','Volume']]
            a = PreviousHigh(self.date_list,self.symbol)
            self.df_sub = self.df_sub.set_index('Date')
            self.df_sub = self.df_sub.merge(a.df_total,how='inner',left_index=True,right_index=True)
    def get_price(self):
        self.df = web.DataReader(self.symbol,'yahoo',self.d1,self.today)
        self.calc_close()
        self.over_reactions()
    def create_date_list(self):
        return self.df_sub['Date'].tolist()
    def calc_close(self):
        self.df['ODR'] = (self.df['Close'] / self.df['Close'].shift()) -1
    # def calc_hilo(self):
    #     self.df['HILO'] = (self.df['High']/self.df['Low'])-1
    # def calc_hi_from_lo(self):
    #     self.df['HifLo'] = (self.df['High']/self.df['Low'].shift())-1
    # def calc_lo_from_hi(self):
    #     self.df['LofHi'] = (self.df['Low']/self.df['High'].shift())-1
    def _append_price(self):
        if len(self.df) < self.n:
            min_date = min(self.date_list) - timedelta(days=self.n*2)
            self.df = web.DataReader(self.symbol,'yahoo',min_date,self.today)
            self.calc_close()
    # def to_database(self):
    #     self.df['Rank'] = np.array(np.arange(1,len(self.df)+1))
    #     for i in self.date_list:
    #         z = self.df.ix[i]['Rank']
    #         volumeratio = self.df.ix[i]['Volume']/self.df.iloc[int(z-self.n-1):int(z-1),]['Volume'].mean()
    #         priorvol = self.df.iloc[int(z-self.n-1):int(z-2),]['ODR'].std()
    #         try:
    #             nextdayreturn = self.df.iloc[int(z)]['ODR']
    #         except:
    #             nextdayreturn = None
    #         priorreturn = (self.df.iloc[int(z-2)]['Close']/self.df.iloc[int(z-self.n-1)]['Close'])-1
    #         dayreturn = self.df.ix[i]['ODR']
    #         price = self.df.ix[i]['Close']
    #         vol = self.df.ix[i]['Volume']
    #         self.input = (self.id,price,vol,dayreturn,nextdayreturn,priorreturn,priorvol,volumeratio,self.n,datetime.date(i))
    #         if self.id == 1:
    #             c.execute('''CREATE TABLE IF NOT EXISTS marketTable (stockid INTEGER,price REAL,volume REAL,dayreturn REAL,nextdayreturn REAL,priorreturn REAL,priorvol REAL,volumeratio REAL,days INTEGER,date TIMESTAMP)''')
    #             c.execute('''INSERT INTO marketTable VALUES (?,?,?,?,?,?,?,?,?,?)''',self.input)
    #             conn.commit()
    #         else:
    #             c.execute('''CREATE TABLE IF NOT EXISTS stockDataTable (stockid INTEGER,price REAL,volume REAL,dayreturn REAL,nextdayreturn REAL,priorreturn REAL,priorvol REAL,volumeratio REAL,days INTEGER,date TIMESTAMP)''')
    #             c.execute('''INSERT INTO stockDataTable VALUES (?,?,?,?,?,?,?,?,?,?)''',self.input)
    #             conn.commit()
    def update_d1Table(self):
        param = (self.today,self.id)
        c.execute("UPDATE d1Table SET d1 = (?) WHERE stockid = (?)",param)
        conn.commit()

class Collect_Error:
    def __init__(self,stockid):
        self.id = stockid
        self.today = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        self._remove()
    def _remove(self):
        param = (self.id,)
        c.execute('''DELETE FROM d1Table WHERE stockid = (?)''',(param))
        conn.commit()
        self._replace()
    def _replace(self):
        param = (self.id,self.today,)
        c.execute('''CREATE TABLE IF NOT EXISTS errorTable (stockid INTEGER,date TIMESTAMP)''')
        c.execute('''INSERT INTO errorTable VALUES (?,?)''',param)
        conn.commit()
