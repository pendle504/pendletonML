from earnings_grab import get_earning_data
from models import Connection,dict_builder
from datetime import date, datetime, timedelta
from pandas.tseries.offsets import BDay
import pandas as pd
conn,c = Connection()
class EarnGrab:
    def __init__(self):
        self.date_list = []
        self.date_dict = {'pre':[0],'during':[0],'post':[1],None:[None]}
        self.df1 = pd.DataFrame()
        self.df = pd.DataFrame()

    def _alldates(self):
        self.df1 = pd.read_sql('''SELECT DISTINCT symbol,stockTable.stockid,date FROM stockDataTable JOIN stockTable ON stockTable.stockid = stockDataTable.stockid''',conn)
        self.date_list,self.symol_list = self.df1['date'].unique().tolist(),self.df1['symbol'].unique().tolist()
        for a in self.date_list:
            self.date = datetime.strptime(a,'%Y-%m-%d')
            for i in get_earning_data(datetime.strptime(a,'%Y-%m-%d').strftime('%Y%m%d')):
                if i['symbol'] in self.symol_list:
                    date_ = self._date_change(i['time'])
                    if date_ is not None:
                        self.df = self.df.append(pd.DataFrame({'symbol':[i['symbol']],'earningsdate':[self.date+BDay(self.date_dict[date_][0])]}))
        self._to_db()
    def _date_change(self,date):
        if date.startswith('B'):
            return 'pre'
        elif date.startswith('A'):
            return 'post'
        elif date.startswith('T'):
            return None
        else:
            try:
                time,meridiem,zone = date.split(' ')
                # return float(float(time.split(':')[0])+(float(time.split(':')[1])/60)),date
                if meridiem == 'am':
                    if float(float(time.split(':')[0])+(float(time.split(':')[1])/60)) >= 9.5:
                        return 'during'
                    else:
                        return 'pre'
                elif meridiem == 'pm':
                    if float(float(time.split(':')[0])+(float(time.split(':')[1])/60)) > 4:
                        return 'post'
                    else:
                        return 'during'
            except:
                return None

    def _to_db(self):
        self.id_dict = dict_builder(self.df1['symbol'].tolist(),self.df1['stockid'].tolist())
        self.df['stockid'] = self.df['symbol'].map(self.id_dict)
        self.df = self.df.set_index('stockid')
        self.df = self.df[['earningsdate']]
        c.execute('''CREATE TABLE IF NOT EXISTS earningsTable (stockid INTEGER,earningsdate TIMESTAMP)''')
        self.df.to_sql('earningsTable',conn,if_exists='replace',index=True,index_label='stockid')
y = EarnGrab()
y._alldates()
