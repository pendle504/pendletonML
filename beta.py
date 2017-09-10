import pandas as pd
import pandas_datareader.data as web
import sqlite3
from datetime import datetime
from models import Connection,dict_builder
conn,c = Connection()
class Beta:
    def __init__(self,beta=250,batch=3000):
        df = pd.read_sql('''SELECT DISTINCT symbol,ipoyear FROM sdt JOIN stockTable ON sdt.stockid = stockTable.stockid''',conn)
        self.ticker_list,self.ipoyear = df['symbol'].tolist(),df['ipoyear'].tolist()
        self.dict = dict_builder(self.ticker_list,self.ipoyear)
        self.spx = web.DataReader('^GSPC','yahoo',datetime(1975,1,1),datetime.today())
        self.spx = self.spx.rename(columns={'Close':'SPXClose'})
        self.spx = self.spx[['SPXClose']]
        self.beta = beta
        self.batch = batch
        self._beta()
    def _beta(self):
        self.total = pd.DataFrame()
        count = 0
        for i in self.ticker_list:
            count += 1
            if count < self.batch:
                yr = int(self.dict[i])+1
                if yr < 2012:
                    yr = 2012
                df = web.DataReader(i,'yahoo',datetime(yr,1,1),datetime.today())
                df = df[['Close']]
                df = df.merge(self.spx,how='left',left_index=True,right_index=True)
                df['Return'],df['MktReturn'] = (df['Close']/df['Close'].shift(1))-1,(df['SPXClose']/df['SPXClose'].shift(1))-1
                df = df.reset_index()
                df_total = pd.DataFrame()
                for e in range(0,len(df)):
                    try:
                        a = df.iloc[e:e+self.beta,:]
                        if len(a) == self.beta:
                            df_total = df_total.append(pd.DataFrame({'Date':df.ix[e+self.beta]['Date'],'Beta':[a['Return'].cov(a.MktReturn)/a['MktReturn'].var()],'Ticker':[i]}))
                    except:
                        pass
                self.total = self.total.append(df_total)
        self.total = self.total.set_index('Date')
a = Beta()
a.total.to_sql('betaTable',conn,if_exists='replace',index=True,index_label='Date')
