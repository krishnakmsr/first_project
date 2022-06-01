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
mycursor.execute("SELECT product_id,product_name,catid,catname,active_flag FROM tbl_pmaster")
data = mycursor.fetchall()


Mongo_Collection_arr=[]
for i in data:
	product_id = i[0]
	product_name=i[1]

	mongo_collection={

		"product_id": product_id,
		"product_name": product_name

		}

"""
	sql_result = "SELECT product_id,spec_id,spec_display_value FROM tbl_spec_display where product_id = {}".format(i[0])
	mycursor.execute(sql_result)
	spec_data=mycursor.fetchall()
	for j in spec_data:
		print "spec_id:",j[1],",","spec_display_value:",j[2]
		
"""
Mongo_Collection_arr.append(mongo_collection)
print Mongo_Collection_arr
mycol1.insert_many(Mongo_Collection_arr) 




	
	#mycursor.execute("SELECT product_id,spec_id,spec_display_value FROM tbl_spec_display where product_id in {}.format(i[0])")


