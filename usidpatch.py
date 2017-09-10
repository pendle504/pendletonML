import sqlite3
from models import Connection
conn,c = Connection()
import numpy as np
import pandas as pd
class usidpatch:
    def __init__(self,df):
        self.df = df
        self.mu = pd.read_sql('''SELECT USID FROM stockDT''',conn)['USID'].max()
        self.df['USID'] = np.array(np.arange(self.mu+1,self.mu+1+len(self.df)))
        self.dfr()
    def dfr(self):
        return self.df


# df = pd.read_sql('''SELECT * FROM stockDT''',conn)
# columns = ['USID','stockid','price']
# print(df.head())
