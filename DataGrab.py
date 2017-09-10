from earnings_grab import get_earning_data
from models import Connection
from datetime import date, datetime, timedelta
import pandas as pd
from pandas.tseries.offsets import BDay
import sqlite3
from models import Connection
import pandas_datareader.data as web
conn,c = Connection()

def get_data(df,minpx,maxpx,maxvol):
    # df = pd.read_sql('''SELECT stockid,date from stockDataTable''',conn,index_col='stockid')
    df = df[(df['price']>minpx) & (df['price']<maxpx) & (df['volume']<maxvol)]
    df2 = pd.read_sql('''SELECT stockid,earningsdate FROM earningsTable''',conn)
    df3 = pd.read_sql('''SELECT date FROM marketTable''',conn)
    df3['date'],df2['earningsdate'],df['date'] = pd.DatetimeIndex(df3.date),pd.DatetimeIndex(df2.earningsdate).normalize(),pd.DatetimeIndex(df.date).normalize()
    df = df.set_index('stockid')
    fail_list = []
    df_final = pd.DataFrame()
    for i in df2['stockid'].unique().tolist():
        try:
            df_sub,df2_sub = df.ix[i],df2[df2['stockid']==i]
            df_sub['Earning'] = df_sub['date'].isin(df2_sub['earningsdate'])
            df_final = df_final.append(df_sub)
        except:
            continue
    df_final['market'] = df_final['date'].isin(df3['date'])
    df_final = df_final.merge()
    return df_final

class GetVix:
    def __init__(self,beg,end):
        self.beg = beg
        self.end = end
        self.df = pd.DataFrame()
        self._get_px()
    def _get_px(self):
        self.df = web.DataReader('^VIX','yahoo',self.beg,self.end)
        self.df = self.df[['High','Low','Close']]

class GetFamaFrench:
    def __init__(self,df):
        self.df = df.reset_index()
        self.df['date'] = self.df['date'].apply(lambda x: pd.to_datetime(str(x),format='%Y-%m-%d'))
        self.df = self.df.set_index('date')
        self.df2 = pd.read_csv('F-F_Research_Data_5_Factors_2x3_daily.CSV',skiprows=3)
        # self.df2['Date'] = self.df2['Date'].astype(str)
        self.df2['Date'] = self.df2['Date'].apply(lambda x: pd.to_datetime(str(x),format='%Y-%m-%d'))
        # self.df2['Date'] = pd.DatetimeIndex(self.df2.Date).normalize()
        # self.df2['Date'] = self.df2['Date'].apply(lambda x: x.strftime('%Y-%m-%d'))
        self.df2 = self.df2.set_index('Date')
    def _merge(self):
        return self.df.merge(self.df2,how='left',left_index=True,right_index=True)

# x = pd.read_csv('trial.csv')
# y = GetFamaFrench(x)
# # print(y.df2.head())
# a = y._merge()
# a.to_csv('lk.csv')



# y = GetVix(datetime(1990,2,14),datetime(2017,2,14))
# y.df.to_sql('vixTable',conn,if_exists='replace',index=True,index_label='Date')
