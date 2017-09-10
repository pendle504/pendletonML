import sqlite3
import pandas as pd
from models import Connection
from datetime import datetime,date
from pandas.tseries.offsets import BDay
import pandas_datareader.data as web
conn,c = Connection()
class Vix:
    def __init__(self,df_join):
        self.df = pd.read_sql('''SELECT Date,Close FROM vixTable''',conn)
        self.df.Date = pd.DatetimeIndex(self.df.Date)
        self.max_date = self.df['Date'].max()
        if self.max_date < datetime.now().date()-BDay(1):
            self.update_table()
            self.df = pd.read_sql('''SELECT Date,Close FROM vixTable''',conn)
        self.df_join = df_join
        self._VixAvg()
    def _vix_jump(self):
        self.df_join = self.df_join.reset_index()
        try:
            self.df_join = self.df_join.rename(columns={'index':'date'})
        except:
            pass
        self.df_join.date = pd.DatetimeIndex(self.df_join.date)
        datemin,datemax = self.df_join['date'].min()-BDay(1),self.df_join['date'].max() + BDay(1)
        df = web.DataReader('^VIX','yahoo',datemin,datemax)
        df = df.reset_index()
        df['VixOpen'] = df['Open']
        df['VixDelta'] = (df['Open'] / df['Close'].shift(1))-1
        df.to_csv('vixcheck.csv')
        self.df_join,df = self.df_join.set_index('date'),df.set_index('Date')
        df = df[['VixOpen','VixDelta']]
        self.df_join = self.df_join.merge(df,how='left',left_index=True,right_index=True)
        self._join()
    def _VixAvg(self):
        self.df['VWkAvg'] = (self.df['Close'] + self.df['Close'].shift(1) + self.df['Close'].shift(2) + self.df['Close'].shift(3) + self.df['Close'].shift(4) + self.df['Close'].shift(5))/6
        self._vix_jump()
    def _join(self):
        self.df = self.df.set_index('Date')
        self.df = self.df[['VWkAvg']]
        self.df = self.df_join.merge(self.df,how='left',left_index=True,right_index=True)
    def update_table(self):
        df = web.DataReader('^VIX','yahoo',self.max_date+BDay(1),datetime.today())
        df = df[['High','Low','Close']]
        df.to_sql('vixTable',conn,if_exists='append',index=True,index_label='Date')


##### CONTINGENCY IF NEED BULK UPLOAD OF VIX DATA
# df = pd.read_sql('''SELECT * FROM sdt''',conn)
# df.date = pd.DatetimeIndex(df.date)
# cols = [c for c in df.columns if c.startswith('Vix') is False]
# df = df[cols]
# datemin,datemax = df['date'].min(),df['date'].max() + BDay(1)
# df2 = web.DataReader('^VIX','yahoo',datemin,datemax)
# df2 = df2.reset_index()
# df2['VixOpen'] = df2.Open.shift(-1)
# df2['VixDelta'] = df2.VixOpen/df2.Close-1
# df,df2 = df.set_index('date'),df2.set_index('Date')
# df2 = df2[['VixOpen','VixDelta']]
# df = df.merge(df2,how='left',left_index=True,right_index=True)
# df.to_sql('sdt',conn,if_exists='replace',index=True,index_label='date')
