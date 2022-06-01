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


mycursor = mydb.cursor()
mycursor.execute("SELECT product_id,product_name,catid,catname,active_flag FROM tbl_pmaster limit 10" )
row_headers=[x[0] for x in mycursor.description]	
rv = mycursor.fetchall()
json_data=[]

for result in rv:
  	json_data.append(dict(zip(row_headers,result)))
   	json.dumps(json_data)
	print(json_data)
	
	
	
	
	
	
   
