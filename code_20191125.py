
import mysql.connector
import pymongo
import json



mydb = mysql.connector.connect(
  host="localhost",
  user="sandeepj",
  passwd="sandeepj!@#",
  database="test"
)


myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb1 = myclient["mydatabase"]
mycol1 = mydb1["qqmaster"]

mycursor = mydb.cursor()
mycursor.execute("SELECT product_id as _id,product_id,product_name,catid,catname,active_flag FROM tbl_pmaster limit 1")
#rows = [i for i in mycursor]

#data_json = []
#header = [i[0] for i in mycursor.description]

#data = mycursor.fetchall()
cols = ["_id", "product_id", "product_name","catid","catname","active_flag"]

results = []
for row in mycursor.fetchall():	
	results.append(dict(zip(cols, row)))
	
print results

    #data_json.append(dict(zip(header, i)))
    #print json.dumps(data_json)
    #mycol1.insert_many(data_json)
    








