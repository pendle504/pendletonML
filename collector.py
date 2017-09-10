from collect import Collect,Collect_Error
from preprocess import Prep
from DJ import DJFear,DJUpdate
from vix import Vix
import pandas as pd
from emotionomics import EmoPatch,EmoTrader
from usidpatch import usidpatch
from nextday import N3R
from models import Connection
from xignite import Fundamentals
conn,c = Connection()
import sqlite3
z = Prep()
z.get_df(3000)
master_df = pd.DataFrame()
for i in z.tuple_list:
    a,b,c = i[0],i[1],i[2]
    # print(a,b,c)
    try:
        y = Collect(a,b,c,thresh=.075,n=22)
        if len(y.df_sub) > 0:
            print('Ticker: ',a,'\n Updating: ',len(y.df_sub),' records')
            master_df = master_df.append(y.df_sub)
        else:
            pass
        y.update_d1Table()
    except:
        y = Collect_Error(a)
        print('Deleted: ',a)
usid = usidpatch(master_df)
usid.df = usid.df.reset_index()
usid.df = usid.df.set_index('USID')
usid.df.to_sql('stockDT',conn,if_exists='append',index=True,index_label='USID')
fund = Fundamentals(master_df)
n3r = N3R(master_df)
# master_df.to_csv('check_3317.csv')
# master_df = pd.read_sql('''SELECT * FROM sdt''',conn)
# y = EmoPatch(master_df)
# columns = ['stockid','price','dayreturn','date','yrHi','yrLo','qHi','qLo','mHi','mLo','sector','symbol','USID']
# fin = EmoTrader(y.df,columns,today=False)
# a = fin._vix_jump()
# cols = [c for c in fin.df.columns if c.lower()!='symbol']
# a = fin.df[cols]
# ### IMPORT INTO SDT!!!!!
# dj = DJFear(a)
# vdj = Vix(dj.df)
# df = vdj.df.iloc[:,1:]
# df.to_csv('final.csv')
# # vdj.df.to_csv('check3.csv')
