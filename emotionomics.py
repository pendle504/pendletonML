import sqlite3
import pandas as pd
from models import Connection,dict_builder
from pandas.tseries.offsets import BDay
import pandas_datareader.data as web
from datetime import datetime
from yahoo_finance import Share
from googlefinance import getQuotes
import re
import json
import numpy as np
conn,c = Connection()
from models import dict_builder,fetchOpen,fetchExtra
class EmoPatch:
    def __init__(self,df):
        self.df = df.reset_index()
        dict_df = pd.read_sql('''SELECT stockid,symbol,sector FROM stockTable''',conn)
        self.symbol_dict = dict_builder(dict_df['stockid'].tolist(),dict_df['symbol'].tolist())
        self.sector_dict = dict_builder(dict_df['stockid'].tolist(),dict_df['sector'].tolist())
        self.map_()
    def map_(self):
        self.df['symbol'] = self.df['stockid'].map(self.symbol_dict)
        self.df['sector'] = self.df['stockid'].map(self.sector_dict)
        # self.df['date'] = self.df['Date']
        # self.max_usid = pd.read_sql('''SELECT USID FROM sdt ORDER BY USID DESC LIMIT 1''',conn)['USID'].max() +1
        # self.df['USID'] = np.array(np.arange(self.max_usid,self.max_usid+len(self.df)))

class EmoTrader:
    def __init__(self,df,columns=[],today=True):
        self.today = today
        self.df = df
        self.columns = columns
        self.columns = self.columns + ['Gap','DayDelta','MktDelta','SectorDelta']
        self.df['DateID'] = np.array(np.arange(1,len(self.df)+1))
        self.df = self.df[(self.df['sector']!='n/a') | (self.df['sector']!='Miscellaneous')]
        self.df['date'] =pd.DatetimeIndex(self.df.date)
        self.df_sub = pd.DataFrame()
        self.sector_map = {
        "Consumer Services":'XLY',
"Technology":'XLK',
"Health Care":'XLV',
"n/a":None,
"Miscellaneous":None,
"Consumer Non-Durables":'XLY',
"Public Utilities":'XLU',
"Capital Goods":'XLB',
"Basic Industries":'XLI',
"Energy":'XLE',
"Finance":'XLF',
"Transportation":'XLI',
"Consumer Durables":'XLP'
        }
        self.ndr()
    def ndr(self):
        self.df['sector'] = self.df['sector'].map(self.sector_map)
        if self.today == False:
            self.df['ndate'] = self.df['date'] + BDay(1)
            self.df['ndate'] = pd.DatetimeIndex(self.df.ndate)
            self.prices()
        else:
            self.day_of()
    def prices(self):
        def _price_grab(row):
            x,y,z = row['symbol'],row['date'],row['ndate']
            try:
                df = web.DataReader(x,'yahoo',z,z)
                a,b = df['Open'][0],df['Close'][0]
                return pd.Series({'ID':row['DateID'],'NDRClose':b,'NDROpen':a})
            except:
                return pd.Series({'ID':row['DateID'],'NDRClose':None,'NDROpen':None})
        def _mkt_grab(row):
            x,y,z = '^GSPC',row['date'],row['ndate']
            try:
                df = web.DataReader(x,'yahoo',z,z)
                a,b = df['Open'][0],df['Close'][0]
                return pd.Series({'ID':row['DateID'],'MktClose':b,'MktOpen':a})
            except:
                return pd.Series({'ID':row['DateID'],'MktClose':None,'MktOpen':None})
        def _sector_grab(row):
            x,y,z = row['sector'],row['date'],row['ndate']
            try:
                df = web.DataReader(x,'yahoo',z,z)
                a,b = df['Open'][0],df['Close'][0]
                return pd.Series({'ID':row['DateID'],'SectorClose':b,'SectorOpen':a})
            except:
                return pd.Series({'ID':row['DateID'],'SectorClose':None,'SectorOpen':None})
        df_px = self.df.apply(_price_grab,axis=1).set_index('ID')
        df_mkt = self.df.apply(_mkt_grab,axis=1).set_index('ID')
        df_sector = self.df.apply(_sector_grab,axis=1).set_index('ID')
        self.emo_df = df_px.merge(df_mkt,how='inner',left_index=True,right_index=True)
        self.emo_df = self.emo_df.merge(df_sector,how='inner',left_index=True,right_index=True)
        self._rejoin()
    def _rejoin(self):
        self.df = self.df.set_index('DateID')
        self.df = self.df.merge(self.emo_df,how='inner',left_index=True,right_index=True)
        self._reclass()
    def _reclass(self):
        self.df['Gap'] = self.df['NDROpen'] / self.df['price'] -1
        self.df['DayDelta'] = self.df['NDRClose'] / self.df['NDROpen'] - 1
        self.df['MktDelta'] = self.df['MktClose'] / self.df['MktOpen'] - 1
        self.df['SectorDelta'] = self.df['SectorClose'] / self.df['SectorOpen'] - 1
        self.df = self.df[self.columns]
        self.df = self.df.set_index('date')
        # self.df.to_csv('emo.csv')
    def _vix_jump(self):
        self.df_join = self.df_join.reset_index()
        datemin,datemax = self.df['date'].min(),self.df['date'].max() + BDay(1)
        df = web.DataReader('^VIX','yahoo',datemin,datemax)
        df = df.reset_index()
        df['symbol']  = 'INDEXCBOE:VIX'
        df['VixOpen'] = df.apply(fetchOpen,axis=1)
        df['VixDelta'] = df.VixOpen/df.Close-1
        self.df,df = self.df.set_index('date'),df.set_index('Date')
        df = df[['VixOpen','VixDelta']]
        self.df = self.df.merge(df,how='left',left_index=True,right_index=True)    
    def _beat(self):
        self.df['MktBeat'] = self.df.DayDelta > self.df.MktDelta
        self.df['SectorBeat'] = self.df.DayDelta > self.df.SectorDelta
        tfdict = {True:1,False:0}
        self.df['MktBeat'],self.df['SectorBeat'] = self.df['MktBeat'].map(tfdict), self.df['SectorBeat'].map(tfdict)
    def day_of(self):
        self.df_trade = pd.DataFrame()
        for i in self.df['symbol'].unique():
            mkt_cap,pe,beta,inst_own = fetchExtra(i)
            try:
                if str(mkt_cap).find('B') >0:
                    self.df_trade = self.df_trade.append(pd.DataFrame({'symbol':[i],'mktcap':[mkt_cap],'pe':[pe],'beta':[beta],'inst_own':[inst_own]}))
                elif str(mkt_cap).find('B') <0:
                    if int(re.findall(r'\d+',str(mkt_cap))[0]) > 500:
                        self.df_trade = self.df_trade.append(pd.DataFrame({'symbol':[i],'mktcap':[mkt_cap],'pe':[pe],'beta':[beta],'inst_own':[inst_own]}))
                    else:
                        continue
            except:
                continue
        self.symbol_usid_dict = dict_builder(self.df['symbol'].tolist(),self.df['USID'].tolist())
        self.df_trade['USID'] = self.df_trade['symbol'].map(self.symbol_usid_dict)
        self.df_trade = self.df_trade.set_index('USID')
        # self.df_trade.to_sql('preTrade',conn,if_exists='append',index=True,index_label='USID')
        self.df = self.df[self.df['symbol'].isin(self.df_trade['symbol'].tolist())]
        self.df['Open'] = self.df.apply(fetchOpen,axis=1)
        self.df['Gap'] = (self.df['Open'] / self.df['price']) - 1
        self.df['DayDelta'],self.df['SectorDelta'],self.df['MktDelta'] = None,None,None
        self.df = self.df[self.columns]
# a = Share('FB')
# print(a.get_open())
# print(a.get_market_cap())
# print(a.get_price_earnings_ratio())
# a = json.dumps(getQuotes('FB'),indent=2)
# b = json.loads(a[1:len(a)-1])
# px = float(re.sub("[^\d\.]","",b['LastTradePrice']))
# print(px)
