
import mysql.connector
import pymongo


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

details=("SELECT * FROM tbl_pmaster")
mycursor.execute(details)
Records = mycursor.fetchall() 


Mongo_Collection_arr=[]
for catalogue in Records: 
	   product_id        = catalogue[0] 
	   product_name      = catalogue[1] 
	   catid             = catalogue[2] 
	   catname           = catalogue[3] 
	   active_flag       = catalogue[4] 
	   Mongo_Collection =   {
                            "product_id" :product_id,
                            "product_name":product_name,
                            "catid": catid,
                            "catname" : catname
                        }
                        
Mongo_Collection_arr.append(Mongo_Collection)
                        
mycol1.insert_many(Mongo_Collection_arr) 

  
  #print "inserted successfully:" 
#"""


