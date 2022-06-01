
import mysql.connector
import pymongo
import json
import sys
from decimal import Decimal
from bson.decimal128 import Decimal128, create_decimal128_context
from collections import OrderedDict
import collections


mydb = mysql.connector.connect(
  host="localhost",
  user="sandeepj",
  passwd="sandeepj!@#",
  database="test"
)

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb1 = myclient["mydatabase"]
mycol1 = mydb1["qqmaster1"]


mycursor = mydb.cursor()

details=("SELECT product_id,product_name,catid,catname,active_flag,updatedon FROM tbl_master where product_id = 10000433")
mycursor.execute(details)

objects_list=[]
Records = mycursor.fetchall() 
for rows in Records:
	pid = rows[0],
	pn = rows[1],
	cid = rows[2],
	cn = rows[3],
	af = rows[4],
	#uon = rows[5]
		#ordered_dict = OrderedDict(my_dict)
    	#ordered_dict=OrderedDict({"pid":pid,"pn":pn,"cid":cid})
    	d=OrderedDict()
    	d['pid']=pid
    	d['pn']=pn
    	d['cid']=cid
    	d['cn']=cn
    	d['af']=af
    	#d['uon']=uon
    	objects_list.append(d)
    	print(objects_list)
    	row_json = json.dumps(objects_list)
    	print(row_json)

		#j=json.dumps(objects_list) 
    	#print (json.loads(objects_list))
    	





    	



	