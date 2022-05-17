import datetime
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta
from collections import OrderedDict
import pymongo
import db_test

s='2022-02-05'
my_date = datetime.strptime(s, "%Y-%m-%d")
d = (datetime.date(my_date) + relativedelta(day=31))
f=str(d)
k=datetime.strptime(f, "%Y-%m-%d")


t=(datetime.date(k) + relativedelta(day=31))


t1=k+timedelta(1)



monthName = t1.strftime("%b").lower()
yearName = t1.strftime("%Y")
month = t1.strftime("%Y%m")


mongo_click_Obj=db_test.MONGODATABASE('mongo_mp_clicktracker_13_65')  
click_Obj=mongo_click_Obj.connections('mp_clicktracker')

  

#query = [('collMod', 'contacts'),
       # ('validator', {'phone': {'$type': 'string'}}),
       # ('validationLevel', 'moderate')]
#query = OrderedDict(query)
#db.command(query)
#{'ok': 1.0}


validator_daily={
			"$jsonSchema" : {
				"bsonType" : "object",
				"required" : [
					"li",
					"ll",
					"dt",
					"vt",
					"ct",
					"mob",
					"src",
					"di",
					"udid",
					"nid",
					"av"
				],
				"properties" : {
					"li" : {
						"bsonType" : "string",
						"description" : "link_identifier - must be a string and is required lower case"
					},
					"ll" : {
						"bsonType" : "string",
						"description" : "link_location - must be a string and is required lower case"
					},
					"dt" : {
						"bsonType" : [ "date", "null" ],
						"description" : "entry_date - must be a date and is required"
					},
					"cnt" : {
						"bsonType" : "int",
						"description" : "cnt - must be a int"
					},
					"ed" : {
						"bsonType" : "string",
						"description" : "entry_day - must be a string lower case"
					},
					"vt" : {
						"bsonType" : "string",
						"description" : "vertical_tag - must be a string and is required lower case"
					},
					"ct" : {
						"bsonType" : "string",
						"description" : "city - must be a string and is required lower case"
					},
					"mob" : {
						"bsonType" : "string",
						"description" : "mobile - must be a string"
					},
					"src" : {
						"bsonType" : "string",
						"description" : "source - must be a string and is required lower case"
					},
					"di" : {
						"bsonType" : "string",
						"description" : "docid - must be a string and is required"
					},
					"sid" : {
						"bsonType" : "string",
						"description" : "sid - must be a string lower case lower case"
					},
					"udid" : {
						"bsonType" : "string",
						"description" : "udid - must be a string and is required lower case lower case"
					},
					"nid" : {
						"bsonType" : "int",
						"description" : "national_catid - must be a int and is required"
					},
					"md" : {
						"bsonType" : "string",
						"description" : "movie_details - must be a string lower case"
					},
					"av" : {
						"bsonType" : "double",
						"description" : "api_version - must take decimal value"
					},
					"f" : {
						"bsonType" : "int",
						"description" : "flag - must be a integer "
					}
				}
			}
		}



query1=OrderedDict()
query1['collMod']='tbl_clicktracker_daily_'+str(month)
query1['validator']=validator_daily
query1['validationLevel']='moderate'
query1['validationAction']='warn' or 'error'

#db.runCommand( {
  #collMod: 'tbl_clicktracker_daily_'+str(month),
  #validator:,
  #validationLevel: "moderate", //off | strict
  #//validationAction: "warn" |"error"
#})

#print(query1)

click_Obj.runCommand(query1)
#db.runCommand(query1)


#elf.db[self.mongo_collection].create_index(
    #[("url", pymongo.DESCENDING), ("category", pymongo.ASCENDING)],
    #unique=True
#)
collection_daily='tbl_clicktracker_daily_'+str(month)
colllectionObj_daily=click_Obj[collection_daily]    
  


index_daily=[("li", pymongo.ASCENDING),("ll", pymongo.ASCENDING),("dt", pymongo.ASCENDING),("vt", pymongo.ASCENDING),("ct", pymongo.ASCENDING),("mob", pymongo.ASCENDING),("src", pymongo.ASCENDING),("di", pymongo.ASCENDING),("udid", pymongo.ASCENDING),("nid", pymongo.ASCENDING),("av", pymongo.ASCENDING)]

colllectionObj_daily.create_index(index_daily,unique=True)


colllectionObj_daily.create_index("li")
colllectionObj_daily.create_index("ll")
colllectionObj_daily.create_index("dt")
colllectionObj_daily.create_index("vt")
colllectionObj_daily.create_index("ct")
colllectionObj_daily.create_index("mob")
colllectionObj_daily.create_index("src")
colllectionObj_daily.create_index("di")
colllectionObj_daily.create_index("udid")
colllectionObj_daily.create_index("nid")
colllectionObj_dailyy.create_index("av")
colllectionObj_daily.create_index("f")



