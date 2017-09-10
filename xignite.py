from datetime import datetime,timedelta
from urllib.request import urlopen
import urllib
import json
import re
import sqlite3
from models import Connection,dict_builder
import pandas as pd
import numpy as np
token = '385724FD7912453C9B67989B735E6D53'
conn,c = Connection()
# self.df = pd.read_sql('''SELECT symbol,date,USID,sector from stockDT JOIN stockTable ON stockDT.stockid = stockTable.stockid WHERE date > "2010-01-01" ORDER BY USID DESC''',conn)
# USID_list = pd.read_sql('''SELECT USID FROM fundTable''',conn)['USID'].tolist()
# self.df = self.df[~self.df['USID'].isin(USID_list)]
class Fundamentals:
    def __init__(self,df):
        self.df =df.reset_index()
        df2 = pd.read_sql('SELECT symbol,stockid,sector from stockTable',conn)
        sector_dict = dict_builder(df2['stockid'].tolist(),df2['sector'].tolist())
        self.df['symbol'] = self.df['stockid'].map(dict_builder(df2['stockid'].tolist(),df2['symbol'].tolist()))
        self.df['sector'] = self.df['stockid'].map(sector_dict)
        self.df = self.df[self.df['sector']!='n/a']
        try:
            self.date_list = self.df['Date'].unique()
        except:
            self.date_list = self.df['date'].unique()
        self._run()
    def _run(self):
        final_df = pd.DataFrame()
        for z in self.date_list:
            try:
                df = self.df[self.df['Date']==z]
            except:
                df = self.df[self.df['date']==z]
            z = str(z)
            z = z.split('T')[0]
            z = datetime.strptime(z,'%Y-%m-%d').date()
            date = '{dt.month}/{dt.day}/{dt.year}'.format(dt =z)
            symbol_list = df['symbol'].tolist()
            usid_list = df['USID'].tolist()
            usid_symbol_dict = dict_builder(key_list=symbol_list,value_list=usid_list)
            symbol = str(symbol_list).strip('[]').strip(' ').replace(' ','').replace("'","")
            Fundamentals = 'MarketCapitalization,Beta,PriceToBook'
            url = '''https://factsetfundamentals.xignite.com/xFactSetFundamentals.json/GetFundamentals?IdentifierType=Symbol&Identifiers=%s&FundamentalTypes=%s&AsOfDate=%s&ReportType=Quarterly&ExcludeRestated=false&UpdatedSince=&_token=''' % (symbol,Fundamentals,date)
            r = urllib.request.urlopen(url+token)
            pre_data = json.loads(r.read().decode(r.info().get_param('charset') or 'utf-8'))[0:]
            big_df = pd.DataFrame()
            for j in range(0,len(pre_data)):
                try:
                    data = pre_data[j]
                    a = {'symbol':[data['Company']['Symbol']]}
                    a['Date'] = data['FundamentalsSets'][0]['AsOfDate']
                    for i in range(0,len(data['FundamentalsSets'][0]['Fundamentals'])):
                        a[data['FundamentalsSets'][0]['Fundamentals'][i]['Type']]=[data['FundamentalsSets'][0]['Fundamentals'][i]['Value']]
                    df = pd.DataFrame.from_dict(a)
                    df['USID'] = df['symbol'].map(usid_symbol_dict)
                    df = df.set_index('USID')
                    df = df[['MarketCapitalization','PriceToBook','Beta']]
                    df = df.apply(pd.to_numeric)
                    df = df.replace('',np.nan)
                    big_df = big_df.append(df)
                except:
                    pass
            final_df = final_df.append(big_df)
        self.final_df = final_df.dropna(how='any')
    def _update(self):
        self.final_df.to_sql('fundTable',conn,if_exists='append',index=True,index_label='USID')
        # final_df.to_csv('check_31617_fund.csv')
df = pd.read_sql('''SELECT * FROM stockDT LIMIT 100''',conn,index_col='USID')
y = Fundamentals(df)
print(y.final_df.head())
