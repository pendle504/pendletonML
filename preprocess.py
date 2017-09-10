import pandas_datareader.data as web
import sqlite3
from datetime import date, datetime, timedelta
import pandas as pd
from models import Connection
conn,c = Connection()

class Prep:
    def __init__(self):
        self.today = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        self.yesterday = datetime.today() - timedelta(days=2)
        self.df = pd.DataFrame()
        self.tuple_list = []
        self.names_left = None
        self.names_complete = None
        self.names_error = None
        self._print_updates()
    def get_df(self,batch=100):
        self.batch = batch
        self.df = pd.read_sql('''SELECT stockTable.stockid AS stockid,symbol,d1 FROM stockTable JOIN d1Table ON stockTable.stockid = d1Table.stockid WHERE d1 < (?) ORDER BY stockid DESC,d1 DESC LIMIT (?)''',conn,params=(self.today,self.batch,))
        self.make_tuple()
    def make_tuple(self):
        self.tuple_list = self.df.apply(tuple,axis=1).tolist()
    def _print_updates(self):
        self._not_updated()
        self._updated()
        print('Names Completed: ',self.names_complete,'\nNames To Be Completed: ',self.names_left)
    def _not_updated(self):
        df = pd.read_sql('''SELECT stockTable.stockid AS stockid,symbol,d1 FROM stockTable JOIN d1Table ON stockTable.stockid = d1Table.stockid WHERE d1 < (?) ORDER BY stockid DESC,d1 DESC''',conn,params=(self.today,))
        self.names_left = len(df)
        return df
    def _updated(self):
        df = pd.read_sql('''SELECT stockTable.stockid AS stockid,symbol,d1 FROM stockTable JOIN d1Table ON stockTable.stockid = d1Table.stockid WHERE d1 >= (?) ORDER BY stockid DESC,d1 DESC''',conn,params=(self.today,))
        self.names_complete = len(df)
        return df
    def _updated_errors(self):
        df = pd.read_sql('''SELECT stockid FROM errorTable''',conn)
        self.names_error = len(df)
        return df
