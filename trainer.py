import sqlite3
import pandas as pd
import numpy as np
from prep import TrainPrep
from emotionomics import EmoTrader
from models import Connection
from datetime import datetime
from DJ import DJFear
from vix import Vix
conn,c = Connection()
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier
from sklearn.metrics import accuracy_score
from sklearn.cross_validation import cross_val_score
from sklearn.pipeline import Pipeline
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler
from sklearn.learning_curve import learning_curve
from MajorityClassVoting import MajorityVoteClassifier
df = pd.read_sql('''SELECT stockDT.date AS Date,stockDT.stockid,symbol,price,dayreturn,volume,
yrHi,yrLo,qHi,qLo,mHi,mLo,stockTable.sector,
stockDT.USID,Beta,MarketCapitalization,PriceToBook,n3r,n5r,ndr,ndv,ndv/volume AS ndvratio
FROM stockDT JOIN stockTable ON stockTable.stockid = stockDT.stockid
JOIN fundTable ON stockDT.USID = fundTable.USID
JOIN n3rTable ON stockDT.USID = n3rTable.USID
ORDER BY stockDT.date DESC''',conn,index_col='USID')
sector_map = {
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
df['sector'] = df['sector'].map(sector_map)
df = df[(df['MarketCapitalization']>200)]
df = df.reset_index()
df = df.set_index('Date')
columns = ['dayreturn','symbol','date','price','sector','volume','MarketCapitalization','n3r','n5r','ndr','PriceToBook','yrHi','yrLo','qHi','qLo','mHi','mLo','ndvratio','Beta']
dj = DJFear(df)
djv = Vix(dj.df)
djv_down = djv.df[djv.df['dayreturn']<0]
djv_up = djv.df[djv.df['dayreturn']>0]
# ,'yrHi','yrLo','qHi','qLo','mHi','mLo','Gap','VWkAvg','VixDelta','5DayFear','3MoFear','JSpread','sector','Beta','MarketCapitalization','PriceToBook'
train_columns = ['dayreturn','price','sector','Beta','3MoFear','MarketCapitalization','JSpread','VWkAvg','ndr','yrHi','yrLo','qHi','qLo','mHi','mLo','PriceToBook','ndvratio']
train = TrainPrep(djv_up,bogey='n5r',scale=False,csv=True,columns=train_columns,test_size=0.05,random_state=1,dummies=True)
X_train,X_test,y_train,y_test = train._variables()
tree = DecisionTreeClassifier(criterion='entropy',max_depth=1)
ada = AdaBoostClassifier(base_estimator = tree,n_estimators=2000,learning_rate =0.1,random_state=0)
ada = ada.fit(X_train,y_train)
# from sklearn.externals import joblib
# joblib.dump(ada,'model.pkl')
y_train_pred = ada.predict(X_train)
y_test_pred = ada.predict(X_test)
ada_train = accuracy_score(y_train,y_train_pred)
ada_test = accuracy_score(y_test,y_test_pred)
print('AdaBoost train/test accucaries %.3f/%.3f' % (ada_train,ada_test))
