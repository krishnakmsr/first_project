
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

details=("SELECT * FROM tbl_pmaster limit 1")
mycursor.execute(details)
Records = mycursor.fetchall() 

spec_details=("SELECT * FROM test.tbl_spec_display limit 1")
mycursor.execute(spec_details)
spec_record = mycursor.fetchall() 

Mongo_Collection1_arr=[]
for spec in spec_record: 
		   pid        = spec[0] 
		   sid        = spec[1] 
		   sdv        = spec[2] 
		   af         = spec[3] 


Mongo_Collection1 =   {
											"pid" :pid,
											"sid":sid,
											"sdv": sdv,
											"af" : af
                       }

Mongo_Collection_arr=[]
for master in Records: 
		   product_id        = master[0] 
		   product_name      = master[1] 
		   catid             = master[2] 
		   catname           = master[3] 
		   active_flag       = master[4] 
       
Mongo_Collection =  {
								"product_id" :product_id,
								"product_name":product_name,
								"catid": catid,
								"catname" : catname,
								"spec_values":Mongo_Collection1_arr
                                 }
print json.dumps(Mongo_Collection)
#Mongo_Collection_arr.append(Mongo_Collection)
#mycol1.insert_many(Mongo_Collection_arr) 
  
  

                      

                        
  


                        
		


