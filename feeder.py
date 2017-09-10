
import sqlite3
import pandas as pd
import numpy as np
from models import Connection
from datetime import datetime,timedelta
from pandas.tseries.offsets import BDay
import pandas_datareader.data as web
from models import dict_builder
conn,c = Connection()

class PreviousHigh:
    def __init__(self,date_list,symbol):
        self.date_list = date_list
        self.symbol_dict = {}
        self._symbol_dict()
        self.date = None
        self.beg = None
        self.symbol = symbol
        self.id = int(self.symbol_dict[self.symbol])
        self.px = None
        self.df_total = pd.DataFrame()
        self.px_df = pd.DataFrame()
        self._unpack_list()
    def _symbol_dict(self):
        dict_df = pd.read_sql('''SELECT stockid,symbol FROM stockTable''',conn)
        self.symbol_dict = dict_builder(dict_df['symbol'].tolist(),dict_df['stockid'].tolist())
    def _unpack_list(self):
        for i in self.date_list:
            try:
                self.date = datetime.strptime(i,'%Y-%m-%d')
            except:
                self.date = i
            self.beg = self.date - BDay(260)
            self.get_price()
        self.df_total = self.df_total.set_index('Date')
    def get_price(self):
        df = web.DataReader(self.symbol,'yahoo',self.beg,self.date)
        self.px = df.ix[self.date]['Close']
        if len(df) >=249:
            self.yrHi,self.yrLo = self.px/df['Close'].max(),self.px/df['Close'].min()
        else:
            self.yrHi,self.yrLo = None,None
        if len(df) >=65:
            self.qHi,self.qLo = self.px/df.iloc[len(df)-65:,:]['Close'].max(),self.px/df.iloc[len(df)-65:,:]['Close'].min()
        else:
            self.qHi,self.qLo = None,None
        if len(df) >=22:
            self.mHi,self.mLo = self.px/df.iloc[len(df)-22:,:]['Close'].max(),self.px/df.iloc[len(df)-22:,:]['Close'].min()
        else:
            self.mHi,self.mLo = None,None
        self.df_total = self.df_total.append(pd.DataFrame({'Date':[self.date],'yrHi':[self.yrHi],'yrLo':[self.yrLo],'qHi':[self.qHi],'qLo':[self.qLo],'mHi':[self.mHi],'mLo':[self.mLo]}))
        # self._to_db()
    def _to_db(self):
        c.execute('''CREATE TABLE IF NOT EXISTS stockData2Table (stockid INTEGER,date TIMESTAMP,yrHi REAL,yrLo REAL,qHi REAL,qLo REAL,mHi REAL,mLo REAL)''')
        self.input = (self.id,self.date,self.yrHi,self.yrLo,self.qHi,self.qLo,self.mHi,self.mLo)
        c.execute('''INSERT INTO stockData2Table VALUES (?,?,?,?,?,?,?,?)''',self.input )
        conn.commit()
        # self.px_df = self.px_df.append(pd.DataFrame({'stockid':[self.id],'date':[self.date],'yrHi':[yrHi],'yrLo':[yrLo],'qHi':[qHi],'qLo':[qLo],'mHi':[mHi],'mLo':[mLo]}))

# df = pd.read_sql('''SELECT symbol,date FROM stockTable JOIN stockDataTable ON stockTable.stockid = stockDataTable.stockid''',conn,index_col='date')
# error_list = []
# for i in df['symbol'].unique():
#     try:
#         y = df.symbol.loc[lambda s: s == i].reset_index()
#         a = PreviousHigh(y['date'].unique().tolist(),i)
#     except:
#         error_list = error_list.append(i)

# df = pd.read_sql('''SELECT * FROM stockDataTable ORDER BY date DESC,stockid DESC''',conn)
# df1 = pd.read_sql('''SELECT * FROM stockData2Table ORDER BY date DESC,stockid DESC''',conn)
# df1['date'] = pd.DatetimeIndex(df1.date).normalize()
# df1['date'] = df1['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
# df1 = df1.set_index('date')
# # print(df1.head())
# df1.to_sql('stockData2Table',conn,if_exists='replace',index=True,index_label='date')
# df.to_csv('df.csv'),df1.to_csv('df1.csv')
