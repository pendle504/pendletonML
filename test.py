import pandas as pd
import random
import numpy as np
import pandas_datareader.data as web
from datetime import date, datetime, timedelta
from pandas.tseries.offsets import BDay
import sqlite3
from models import Connection
conn,c = Connection()
# mkt = '^GSPC'
# tik = 'GILD'
# df = web.DataReader(mkt,'yahoo',datetime(2016,3,1),datetime.today())
# df2 = web.DataReader(tik,'yahoo',datetime(2016,3,1),datetime.today())
# df['return'+str(mkt)] = df['Close']/df['Close'].shift()-1
# df2['return'+str(tik)] = df2['Close']/df2['Close'].shift()-1
# df = df.merge(df2,how='left',left_index=True,right_index=True)
# columns = ['return'+str(mkt),'return'+str(tik)]
# df = df[columns]
# print(df.corr())
