from aiohttp import http
import mysql.connector
import requests
from pprint import pprint
import pandas as p
from sklearn import preprocessing
import pymysql
import pandas
from scipy.sparse import data
from sklearn.decomposition import PCA
from sklearn import preprocessing
from sqlalchemy import create_engine
import aiohttp
import asyncio

dbcon = mysql.connector.connect(
host="localhost",
user="root",
password="rootroot",
database="stackdata"
)

dbcursor = dbcon.cursor()
selectsql = "select user_id from stack_user LIMIT 90"
dbcursor.execute(selectsql)
userresult = dbcursor.fetchall()
responsedict = {}
resp_dict=[]
pagenumber = 1
nextpage = True

def get_tasks(session):
    tasks=[]
    for userval in userresult:
        user = userval[0]
        tag_url = "https://api.stackexchange.com/2.3/users/{u}/tags?page={p}&pagesize=100&order=desc&sort=popular&site=stackoverflow".format(u=user,p=pagenumber)
        tasks.append(session.get(tag_url,ssl=False))
        
    return tasks  

async def stack_usertags():
    connector = aiohttp.TCPConnector(limit_per_host=80)
    client = aiohttp.ClientSession(connector=connector)
    async with client as session:
        tasks=get_tasks(session)
        res = await asyncio.gather(*tasks)
        for r in res:
            resp_dict.append(await r.json())
    #pprint(resp_dict)

asyncio.run(stack_usertags())
print(resp_dict[0])

for i in resp_dict:
    items = i["items"]
    for j in items:
        user_id, count, tag = j["user_id"], j["count"], j["name"]
    
        if count not in (1, 2, 3) or count not in ('1', '2', '3'):
            selectsql = "SELECT COUNT(*) FROM stack_usertags WHERE user_id = %s and tag = %s"
            selectval = (user_id, tag,)
            # print(user_id, '& ', tag)
            dbcursor.execute(selectsql, selectval)
            selectcount = dbcursor.fetchall()

            if selectcount == [(0,)]:
                insertsql = "INSERT INTO stack_usertags (user_id, count, tag) VALUES (%s, %s, %s)"
                insertval = (user_id, count, tag)
                dbcursor.execute(insertsql, insertval)
            else:
                updatesql = "UPDATE stack_usertags SET count = %s WHERE user_id = %s and tag = %s"
                updateval = (count, user_id, tag)
                dbcursor.execute(updatesql, updateval)

            dbcon.commit()