import pymysql
import pandas
from sklearn.decomposition import PCA
from sklearn import preprocessing
from sklearn.cluster import KMeans
from sqlalchemy import create_engine
from datetime import date

def PCA_processing(columndata):
    columndata = preprocessing.StandardScaler().fit_transform(columndata)
    p = PCA(n_components=2)
    princomp = p.fit_transform(columndata)
    newdf = pandas.DataFrame(data=princomp, columns = ['PC1', 'PC2'])
    return newdf

pandas.options.mode.chained_assignment = None

dbcon = pymysql.connect(host="localhost", user="root", password = "rootroot", db="stackdata")

insertcursor = create_engine("mysql+pymysql://{user}:{pw}@localhost:3306/{db}".format(user="root",pw="rootroot",db="stackdata"))

sqlselect = pandas.read_sql_query('''select u.user_id, u.reputation, u.up_vote_count, u.badge_gold, t.count from stack_user u, stack_usertags t where t.tag = 'c++' and u.user_id = t.user_id''', dbcon)
datafr = pandas.DataFrame(sqlselect, columns=['user_id', 'reputation', 'up_vote_count', 'badge_gold', 'count'])
columns = ['reputation', 'up_vote_count', 'badge_gold', 'count']
columndata = datafr.loc[:, columns].values
useriddata = datafr.loc[:,['user_id']].values

sqlselect1 = pandas.read_sql_query('''select id,login,name,location,email,public_repos,Num_of_contributions,language from github_users''', dbcon)
datafr1 = pandas.DataFrame(sqlselect1, columns=['id', 'public_repos', 'Num_of_contributions'])
datafr1.rename(columns = {'id':'user_id'}, inplace = True)
columns1 = ['public_repos','Num_of_contributions']
columndata1 = datafr1.loc[:, columns1].values
useriddata1 = datafr1.loc[:,['user_id']].values

newdf = PCA_processing(columndata)
tempdf = datafr[['user_id']]
tempdf['effective_date'] = date.today()

newdf1 = PCA_processing(columndata1)
tempdf1 = datafr1[['user_id']]
tempdf1['effective_date'] = date.today()

#tempdf.loc[:date.today()]


tempdf = pandas.concat([tempdf, tempdf1],axis=0)
newdf = pandas.concat([newdf, newdf1],axis=0)

seqselect = pandas.read_sql_query('''select max(sequence) from user_predicted_data''', dbcon)
if 0 not in seqselect.values or seqselect.values == None:
    seqvalue = seqselect.iloc[0,0] + 1
else:
    seqvalue = 1

tempdf['sequence'] = seqvalue



dffinal = pandas.concat([tempdf, newdf], axis=1)
dffinal.reset_index(inplace=True, drop=True)

kmeans = KMeans(2)
kmeans.fit(newdf)

clusresult = kmeans.fit_predict(newdf)

#print(clusresult)

predictdata = {'prediction':clusresult}
#print(predictdata)
predictdf = pandas.DataFrame(predictdata)
#print(dffinal.to_string())
#print(predictdf.to_string())

dffinal = pandas.concat([dffinal, predictdf], axis=1, ignore_index=False)
# print(dffinal)

dffinal.to_sql('user_predicted_data', con = insertcursor, if_exists = 'append', index = False)