import sqlite3
from datetime import date, datetime, timedelta
import pandas as pd
import random
import numpy as np
from pandas_datareader import data,wb
import pandas_datareader.data as web
from pandas_datareader.data import Options
import os
from pandas.tseries.offsets import BDay
import sys
from models import Filebasket,Connection,dict_builder


fb = Filebasket()
conn,c = Connection()


# self.df = pd.read_sql('''SELECT symbol,date,USID from stockDT JOIN stockTable ON stockDT.stockid = stockTable.stockid WHERE date > "2012-01-01" ORDER BY USID DESC''',conn)
# USID_list = pd.read_sql('''SELECT USID from n3rTable''',conn)['USID'].tolist()
# print(len(self.df))
# print(len(USID_list))
# self.df = self.df[~self.df['USID'].isin(USID_list)]
# print(len(self.df))
class N3R:
    def __init__(self,df):
        self.df = df.reset_index()
        df2 = pd.read_sql('''SELECT stockid,symbol from stockTable''',conn)
        symbol_dict = dict_builder(key_list=df2['stockid'].tolist(),value_list=df2['symbol'].tolist())
        self.df['symbol'] = self.df['stockid'].map(symbol_dict)
        self.date_list = self.df['Date'].unique()
        self._run()
    def _run(self):
        final_df = pd.DataFrame()
        count = 0
        for z in self.date_list:
            count +=1
            df = self.df[self.df['Date']==z]
            z = str(z)
            z = z.split('T')[0]
            z = datetime.strptime(z,'%Y-%m-%d').date()
            z2 = z + BDay(6)
            usid_symbol_dict = dict_builder(key_list=df['symbol'].tolist(),value_list=df['USID'].tolist())
            date_df = pd.DataFrame()
            for i in df['symbol'].unique():
                try:
                    pxdf = web.DataReader(i,'yahoo',z,z2)
                    pxdf['NDR'] = pxdf['Close']/pxdf['Close'].shift()-1
                    pxdf['N3R'] = pxdf['Close']/pxdf['Close'].shift(3)-1
                    pxdf['N5R'] = pxdf['Close']/pxdf['Close'].shift(5)-1
                    y = pd.DataFrame({'symbol':[i],'ndr':[pxdf.ix[1]['NDR']],'n3r':[pxdf.ix[4]['N3R']],'n5r':[pxdf.ix[6]['N5R']],'ndv':[pxdf.ix[1]['Volume']]})
                    y['USID'] = y.symbol.map(usid_symbol_dict)
                    y = y.set_index('USID')
                    date_df = date_df.append(y)
                except:
                    pass
            final_df = final_df.append(date_df)
        final_df = final_df[['ndr','n3r','n5r','ndv']]
        # final_df = self.df.merge(final_df,how='left',left_index=True,right_index=True)
        final_df.to_sql('n3rTable',conn,if_exists='append',index=True,index_label='USID')
