import googlefinance
import json
import sqlite3
from emotionomics import EmoTrader,EmoPatch
from DJ import DJFear
from vix import Vix
from pandas.tseries.offsets import BDay
from datetime import datetime
import pandas as pd
from models import Connection
import pandas_datareader.data as web
conn,c = Connection()
# df = pd.read_sql('''SELECT * FROM stockDT JOIN stockTable ON stockDT.stockid = stockTable.stockid ORDER BY date DESC''',conn)
# usid_list = pd.read_sql('''SELECT USID FROM sdt''',conn)['USID'].tolist()
# columns = ['stockid','price','dayreturn','date','yrHi','yrLo','qHi','qLo','mHi','mLo','sector','symbol','USID']
# df = df[columns]
# df['date'] = pd.DatetimeIndex(df.date)
# df = df[(df['date']> datetime(2000,1,1))]
# print(len(df))
# df = df[~df.USID.isin(usid_list)]
# print(len(df))
# df = df.dropna(how='any')
# x = df.iloc[:5000,:]
# z = EmoTrader(x,columns)
# a = z.df.iloc[:,1:]
# cols = [c for c in a.columns if c.lower()!='symbol']
# a = a[cols]
# a.to_sql('sdt',conn,if_exists='append',index=True,index_label='date')

# df = pd.read_sql('''SELECT usid,stockid,price,volume,dayreturn,date,yrHi,yrLo,qHi,qLo,mHi,mLo FROM stockDT''',conn)
# df = df.dropna(how='any')
# df['valuetraded'] = df['volume'] * df['price']
# df.date = pd.DatetimeIndex(df.date)
# df = df[(df['price']>3)&(df['valuetraded']>750000)&(df['yrLo']<5)&(df['qLo']<5)&(df['mLo']<5)&(df['date']>datetime(2000,1,1))]
# df = df.sort_values(by='date',ascending=False)
# df1 = df.iloc[:20000,:]
# a = EmoPatch(df1)
# columns = ['stockid','price','dayreturn','volume','date','yrHi','yrLo','qHi','qLo','mHi','mLo','sector','symbol','USID']
# c = EmoTrader(a.df,columns,today=False)
# # c = Vix(b.df)
# cols = [c for c in c.df.columns if c.lower()!='symbol']
# fin = c.df[cols]
# fin.to_sql('sdt',conn,if_exists='replace',index=True,index_label='date')
# df = pd.read_sql('''SELECT * FROM sdt''',conn,index_col='date')
# b = DJFear(df)
# df = b.df
# a = Vix(df)
# a.df.to_csv('final.csv')
df = pd.read_csv('final.csv',index_col='USID')
df2 = pd.read_csv('bloomdata_.csv',index_col='USID')
df2 = df2.rename(columns={'Beta (as of Date)':'Beta','MarketCap (as of Date)':'MktCap'})
df2 = df2[['Beta','MktCap']]
df2.Beta,df2.MktCap = pd.to_numeric(df2['Beta']),pd.to_numeric(df2.MktCap)
df = df.merge(df2,how='left',left_index=True,right_index=True)
df = df.reset_index()
df = df.set_index('date')
df = df.dropna(how='any')
df.to_csv('final2.csv')
