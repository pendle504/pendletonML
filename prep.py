import numpy as np
import pandas as pd
from scipy import stats
from sklearn.cross_validation import train_test_split
from sklearn.preprocessing import StandardScaler
class TrainPrep:
    def __init__(self,df,bogey='mkt',columns=[],csv=False,scale=True,test_size=0.1,random_state=0,dummies=False):
        df = df.reset_index()
        self.df = df
        self.type = bogey
        self.dummies = dummies
        self.scale = scale
        self.csv = csv
        self.columns = columns
        self.test_size = test_size
        self.random_state = random_state
        self._prep()
    def _prep(self):
        print('Total Samples (prep0): ',len(self.df))
        self.df = self.df.replace([np.inf,-np.inf],np.nan)
        print('Total Samples (inf): ',len(self.df))
        self.df['valuetraded'] = self.df['price'] * self.df['volume']
        self.df = self.df[(self.df['yrLo']<5)&(self.df['qLo']<4)&(self.df['mLo']<2)]
        print('Total Samples (outliers): ',len(self.df))
        self.df = self.df.dropna(axis=0,how='any')
        print('Total Samples (nan): ',len(self.df))
        if self.dummies:
            self.df_dum = pd.get_dummies(self.df['sector'])
            self.df = self.df.merge(self.df_dum,how='inner',left_index=True,right_index=True)
            self.columns = self.columns + ['XLB','XLE','XLF','XLI','XLK','XLP','XLU','XLV','XLY']
        self._ndr()
    def _ndr(self):
        def MktBeat(row):
            y = row['dayreturn']
            if y > 0:
                if row['DayDelta'] - row['MktDelta'] >= 0:
                    return 0
                else:
                    return 1
            else:
                if row['DayDelta'] - row['MktDelta'] >= 0:
                    return 1
                else:
                    return 0
        def SectorBeat(row):
            y = row['dayreturn']
            if y > 0:
                if row['DayDelta'] - row['SectorDelta'] >= 0:
                    return 0
                else:
                    return 1
            else:
                if row['DayDelta'] - row['SectorDelta'] >= 0:
                    return 1
                else:
                    return 0
        def nxr(row):
            y = row['Y']
            if y > 0:
                return 1
            else:
                return 0
        if self.type == 'mkt':
            self.df['MktBeat'] = self.df.apply(MktBeat,axis=1)
            self.columns.append('MktBeat')
        elif self.type == 'sector':
            self.df['SectorBeat'] = self.df.apply(SectorBeat,axis=1)
            self.columns.append('SectorBeat')
        elif self.type.startswith('n')==True:
            self.df['Y'] = self.df[self.type]
            self.df['Y'] = self.df.apply(nxr,axis=1)
            self.columns.append('Y')
        if self.dummies:
            self.columns.remove('sector')
        self.df = self.df[self.columns]
        # self.df = self.df[(np.abs(stats.zscore(self.df))<3).all(axis=1)]
        if self.csv is True:
            self.df.to_csv('trial_.csv')
        return self.df
    def _variables(self):
        y = self.df.iloc[:,len(self.columns)-1].values
        for i in self.df.iloc[:,len(self.columns)-1].unique():
            print(i,self.df.iloc[:,len(self.columns)-1].value_counts()[i]/len(self.df.iloc[:,len(self.columns)-1]))
        print('Total Samples: ',len(self.df))
        X = self.df.iloc[:,:len(self.columns)-1].values
        self.X_train,self.X_test,self.y_train,self.y_test = train_test_split(X,y,test_size=self.test_size,random_state=self.random_state)
        if self.scale is True:
            self._scale()
            return self.X_train_std,self.X_test_std,self.y_train,self.y_test
        else:
            return self.X_train,self.X_test,self.y_train,self.y_test
    def _scale(self):
        sc = StandardScaler()
        sc.fit(self.X_train)
        self.X_train_std = sc.transform(self.X_train)
        self.X_test_std = sc.transform(self.X_test)
