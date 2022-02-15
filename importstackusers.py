import requests
import mysql.connector

pagenumber = 3
responsedict = {}
nextpage = True

dbcon = mysql.connector.connect(
  host="localhost",
  user="root",
  password="**********",
  database="github_data"
)

while pagenumber <= 3000:
    userurl = "https://api.stackexchange.com/2.3/users?page={p}&pagesize=50&order=desc&sort=reputation&site=stackoverflow&filter=!0Z-PEqoWtMFN3pClcA69W1wOw".format(p=pagenumber)
    response = requests.get(userurl)
    resp_dict = response.json()
    responsedict[pagenumber] = resp_dict
    pn = str(pagenumber)
    pn = pn + "0"

    # print(resp_dict)
    pagenumber = int(pn)

dbcursor = dbcon.cursor()

for i in responsedict.values():

    #print(i)
    items = i["items"]
    for j in items:
        user_id, display_name, reputation, up_vote_count = j["user_id"], j["display_name"], j["reputation"], j["up_vote_count"]
        badgecounts = j["badge_counts"]
        bronze, silver, gold = badgecounts['bronze'], badgecounts['silver'], badgecounts['gold']

        selectsql = "SELECT COUNT(*) FROM stack_user WHERE user_id = %s"
        selectval = (user_id,)
        dbcursor.execute(selectsql, selectval)
        selectcount = dbcursor.fetchall()

        if selectcount == [(0,)]:
            insertsql = "INSERT INTO stack_user (user_id, display_name, reputation, up_vote_count, badge_bronze, badge_silver, badge_gold) VALUES (%s, %s, %s,%s, %s, %s, %s)"
            insertval = (user_id, display_name, reputation, up_vote_count, bronze, silver, gold)
            dbcursor.execute(insertsql, insertval)
        else:
            updatesql = "UPDATE stack_user SET display_name = %s, reputation = %s, up_vote_count = %s, badge_bronze = %s, badge_silver = %s, badge_gold = %s WHERE user_id = %s"
            updateval = (display_name, reputation, up_vote_count, bronze, silver, gold, user_id)
            dbcursor.execute(updatesql, updateval)
        
        dbcon.commit()
