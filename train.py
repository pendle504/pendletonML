import sqlite3
import pandas as pd
import numpy as np
from models import Connection,ndr,wins_df,using_mstats
from sklearn.cross_validation import train_test_split,StratifiedKFold, cross_val_score
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.learning_curve import learning_curve
from sklearn.linear_model import Perceptron,LogisticRegression
from sklearn.svm import SVC
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.metrics import accuracy_score
from sklearn.decomposition import PCA
from sklearn.pipeline import Pipeline
from scipy.stats import mstats
from scipy import stats
import matplotlib.pyplot as plt
from DataGrab import get_data,GetFamaFrench
from sbs import SBS
conn,c = Connection()
minpx = 2
maxpx = 100
maxvol = 200000000
tf_dict = {True:1,False:0}
df = pd.read_sql('''SELECT stockTable.stockid AS stockid,price,volume,dayreturn,yrHi,yrLo,qHi,qLo,mHi,mLo,priorreturn,priorvol,volumeratio,nextdayreturn,sector,stockDataTable.date AS date,
        vixTable.Close as VixClose,vixTable.High as VixHigh,vixTable.Low as VixLow
        FROM stockDataTable JOIN stockTable ON stockTable.stockid = stockDataTable.stockid
        JOIN stockData2Table ON stockDataTable.stockid = stockData2Table.stockid AND stockDataTable.date = stockData2Table.date
        JOIN vixTable ON stockDataTable.date = vixTable.Date
        WHERE price > (?) AND volume <  (?) AND price < (?) AND sector != "n/a"''',conn,params=(minpx,maxvol,maxpx,))
# data_input = get_data(df,minpx,maxpx,maxvol)
df = df.replace([np.inf,-np.inf],np.nan)
df = df.dropna(axis=0,how='any')
df['valuetraded'] = df['price'] * df['volume']
df = df[(df['valuetraded']>50000) & (df['yrLo'] < 3) & (df['qLo'] < 3) & (df['mLo'] < 3) & (df['volumeratio'] < 8)]
df['ndr'] = df.apply(ndr,axis=1)
# df['Earning'],df['market'] = df['Earning'].map(tf_dict),df.market.map(tf_dict)
# df = df[(df['Earning']==1) & (df['market']==0)]
columns = ['date','ndr','price','volume','dayreturn','yrHi','yrLo','qHi','qLo','mHi','mLo','priorreturn','priorvol','volumeratio','VixClose','VixHigh','VixLow']
df = df[columns]
# df = pd.get_dummies([columns])
df = df.set_index('date')
df = df[(np.abs(stats.zscore(df))<3).all(axis=1)]
a = GetFamaFrench(df)
df = a._merge()
df = df.replace([np.inf,-np.inf],np.nan)
df = df.dropna(axis=0,how='any')
# df = df[df['volume']>20000]
# df.to_csv('trial.csv')
y = df.iloc[:,0].values
for i in df.iloc[:,0].unique():
    print(i,df.iloc[:,0].value_counts()[i]/len(df.iloc[:,0]))
X = df.iloc[:,1:].values
X_train,X_test,y_train,y_test = train_test_split(X,y,test_size=.1,random_state=0)
sc = StandardScaler()
sc.fit(X_train)
X_train_std = sc.transform(X_train)
X_test_std = sc.transform(X_test)

######## PIPELINE ###########
# pipe_lr = Pipeline([('scl',StandardScaler()),('clf',LogisticRegression(random_state=1,penalty='l2'))])
# train_sizes,train_scores,test_scores = learning_curve(estimator=pipe_lr,X=X_train,y=y_train,train_sizes = np.linspace(0.1,1.0,10),cv=10)
# train_mean = np.mean(train_scores,axis=1)
# train_std = np.std(train_scores,axis=1)
# test_mean = np.mean(test_scores,axis=1)
# test_std = np.std(test_scores,axis=1)
# plt.plot(train_sizes,train_mean,color='blue',marker='o',markersize=5,label='training accuracy')
# plt.fill_between(train_sizes,train_mean + train_std,train_mean - train_std,alpha=0.15,color='blue')
# plt.plot(train_sizes,test_mean,color='green',linestyle='--',marker='s',markersize=5,label='validation accuracy')
# plt.fill_between(train_sizes,test_mean + test_std,test_mean - test_std,alpha=.15,color='green')
# plt.grid()
# plt.xlabel('Number of Training Samples')
# plt.ylabel('Accuracy')
# plt.legend(loc='lower right')
# plt.ylim([.3,1.0])
# plt.show()
# scores = cross_val_score(estimator=pipe_lr,X=X_train,y=y_train,cv=10)
# print('CV accuracy score: %s ' % scores)
# print('CV accuracy: %.3f +/- %.3f' % (np.mean(scores),np.std(scores)))
  #### w/ learning curve ####

########## PCA #############
# pca = PCA(n_components=4)
# lr = LogisticRegression()
# X_train_pca = pca.fit_transform(X_train_std)
# X_test_pca = pca.transform(X_test_std)
# lr.fit(X_train_pca,y_train)
# y_pred = lr.predict(X_test_pca)
# print('Accuracy: %.2f' % accuracy_score(y_test,y_pred))

############ PCA Methods #############
# cov_mat = np.cov(X_train_std.T)
# eigen_vals,eigen_vecs = np.linalg.eig(cov_mat)
# eigen_pairs = [(np.abs(eigen_vals[i]),eigen_vecs[:,i])for i in range(len(eigen_vals))]
# eigen_pairs.sort(reverse=True)
# w = np.hstack((eigen_pairs[0][1] [:,np.newaxis],eigen_pairs[1][1][:,np.newaxis]))
# X_train_pca = X_train_std.dot(w)

############ Plot Eigenvalues #############
# print('\n Eigenvalues \n%s' % eigen_vals)
# tot = sum(eigen_vals)
# var_exp = [(i/tot) for i in sorted(eigen_vals,reverse=True)]
# cum_var_exp = np.cumsum(var_exp)
#
# plt.bar(range(1,7),var_exp,alpha=0.5,align='center',label='individual explained variance')
# plt.step(range(1,7),cum_var_exp,where='mid',label='cumulative explained variance')
# plt.ylabel('Explained Variance Ratio')
# plt.xlabel('Principal components')
# plt.legend(loc='best')
# plt.show()

########## Sequetial Backward Selection #################
# knn = KNeighborsClassifier(n_neighbors=2)
# sbs = SBS(knn,k_features=1)
# sbs.fit(X_train_std,y_train)
# k_feat = [len(k) for k in sbs.subsets_]
# plt.plot(k_feat,sbs.scores_,marker='o')
# plt.ylim([0.7,1.1])
# plt.ylabel('Accuracy')
# plt.xlabel('Number of Features')
# plt.grid()
# plt.show()
########## Perceptron #################
# ppn = Perceptron(n_iter=40, eta0=1.0,random_state=0)
# ppn.fit(X_train_std,y_train)
# y_pred = ppn.predict(X_test_std)
# print('Accuracry: %.2f' % accuracy_score(y_test,y_pred))

########## Logistic Regression #################
# lr = LogisticRegression(penalty='l1',C=10)
# lr.fit(X_train_std,y_train)
# print('Training Accuracy: ',lr.score(X_train_std,y_train))
# print('Test Accuracy: ',lr.score(X_test_std,y_test))
# y_pred = lr.predict(X_test_std)
# print('Accuracy: %.2f' % accuracy_score(y_test,y_pred))

########## Kernel SVM  #################
# svm = SVC(kernel='rbf',random_state=0,gamma=0.2,C=1)
# svm.fit(X_train_std,y_train)
# y_pred = svm.predict(X_test_std)
# print('Accuracry: %.2f' % accuracy_score(y_test,y_pred))

########## Decision Tree #################
# tree = DecisionTreeClassifier(criterion='entropy',max_depth=3,random_state=0)
# tree.fit(X_train_std,y_train)
# y_pred = tree.predict(X_test_std)
# print('Accuracry: %.2f' % accuracy_score(y_test,y_pred))

######### Random Forest #################
forest = RandomForestClassifier(n_estimators=1000,random_state=0)
# feat_labels = df.columns[1:]
forest.fit(X_train_std,y_train)
# importances = forest.feature_importances_
# indices = np.argsort(importances)[::-1]
# for f in range(X_train.shape[1]):
#     print(("%2d) %-*s %f" % (f+1,30,feat_labels[f],importances[indices[f]])))
y_pred = forest.predict(X_test_std)
print('Accuracry: %.2f' % accuracy_score(y_test,y_pred))

######### Nearest Neighbors #################
# knn = KNeighborsClassifier(n_neighbors=5,p=2,metric='minkowski')
# knn.fit(X_train_std,y_train)
# y_pred = knn.predict(X_test_std)
# print('Accuracry: %.2f' % accuracy_score(y_test,y_pred))
