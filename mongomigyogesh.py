
import mysql.connector
import pymongo
import json
import sys
from decimal import Decimal
from bson.decimal128 import Decimal128, create_decimal128_context

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

details=("SELECT product_id,product_name,catid,catname,active_flag,updatedon FROM tbl_master limit 10")
mycursor.execute(details)
Records = mycursor.fetchall() 
master_arr=[]
for master in Records: 
	product_id        = master[0] 
	product_name      = master[1] 
	catid             = master[2] 
	catname           = master[3] 
	active_flag       = master[4]
	updatedon         = master[5]


	# print master
	# sys.exit

	image_details='SELECT product_imagepath as img_p,height as hgt,width as wdt,image_type as img_t FROM tbl_images where product_id="%s"' %(product_id)
	mycursor.execute(image_details)
	image_records = mycursor.fetchall()
	image_arr=[]
	for image in image_records: 
	   img_p        = image[0] 
	   hgt          = image[1] 
	   wdt          = image[2] 
	   img_t        = image[3] 
	   image_arr.append({	"img_p" :img_p,
							"hgt"   : Decimal128(hgt),
							"wdt"   : Decimal128(wdt),
							"img_t" :img_t
				         }) 

	#print image_arr
	#sys.exit()

	spec_details='SELECT spec_id,spec_display_value,active_flag FROM tbl_spec_display where product_id="%s"' %(product_id)
	mycursor.execute(spec_details)
	spec_record = mycursor.fetchall()
	# print spec_details
	# sys.exit()

	spec_arr=[]
	for spec in spec_record: 
	   
	   sid        = spec[0] 
	   sdv        = spec[1] 
	   af         = spec[2] 
	   spec_arr.append({	
							"sid":sid,
							"sdv": sdv,
							"af" : af
				         }) 


	Mongo_Collection =  {
							"product_id" :product_id,
							"product_name":product_name,
							"catid": catid,
							"catname" : catname,
							"active_flag" : active_flag,
							"updatedon" : updatedon,
							"image_info":image_arr,
							"spec_values":spec_arr
	                         }
	master_arr.append(Mongo_Collection)	
             

#print(master_arr)
mycol1.insert_many(master_arr) 
  
  

                      

                        
  


                        
		


