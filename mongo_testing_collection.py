import pymongo
from pymongo import MongoClient
# pprint library is used to make the output look more pretty
from pprint import pprint
# connect to MongoDB, change the << MONGODB URL >> to reflect your own connection string
client = pymongo.MongoClient('mongodb://192.168.13.65:27017')
db=client['test']
db1=client['db_test']


import datetime
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta
from collections import OrderedDict



#s='2023-01-25'
#my_date = datetime.strptime(s, "%Y-%m-%d")
#d = (datetime.date(my_date) + relativedelta(day=31))
#f=str(d)
#k=datetime.strptime(f, "%Y-%m-%d")
#t=(datetime.date(k) + relativedelta(day=31))
#t1=k+timedelta(1)
#monthName = t1.strftime("%b").lower()
#yearName = t1.strftime("%Y")
#month = t1.strftime("%Y%m")
x=db.list_collection_names() #getting collection list in db

x1=db1.list_collection_names()

kri=datetime.now()
krish=kri.strftime("%Y-%m-%d")
print(krish)
krish1=datetime.strptime(krish,"%Y-%m-%d")
ti=(datetime.date(krish1) + relativedelta(day=31))
print(ti)

if(krish==ti): #checking if today is lastday of the month
	t2=ti+timedelta(1)  #first day of next month
        monthName = t2.strftime("%b").lower()
	yearName = t2.strftime("%Y")
	month = t2.strftime("%Y%m")
        date1=t2.strftime("%Y%m%d")
        vexpr_col='tbl_clicktracker_daily_'+str(month)
	if(vexpr_col not in x):                                    #checking if collection exists
		db.create_collection(vexpr_col)  # force creating colection

		vexpr = {
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
		query=OrderedDict()
		query['collMod']='tbl_clicktracker_daily_'+str(month)
		query['validator']=vexpr
		query['validationLevel']='moderate'
		query['validationAction']='warn' or 'error'






		db.command(query)   # adding validation to the collection
#db.command('collMod',vexpr_col, validator=vexpr, validationLevel='moderate')

#print(query1)
		collection_daily='tbl_clicktracker_daily_'+str(month)
#l=l.append(collection_dayly)
		colllectionObj_daily=db['tbl_clicktracker_daily_'+str(month)]  # collection obj
#print(l)
#print(colllectionObj_daily)
		index_daily=[("li", pymongo.ASCENDING),("ll", pymongo.ASCENDING),("dt", pymongo.ASCENDING),("vt", pymongo.ASCENDING),("ct", pymongo.ASCENDING),("mob", pymongo.ASCENDING),("src", pymongo.ASCENDING),("di", pymongo.ASCENDING),("udid", pymongo.ASCENDING),("nid", pymongo.ASCENDING),("av", pymongo.ASCENDING)]

		colllectionObj_daily.create_index(index_daily,unique=True)   #unique_index
		colllectionObj_daily.create_index("li")
		colllectionObj_daily.create_index("ll")
		colllectionObj_daily.create_index("dt")
		colllectionObj_daily.create_index("vt")
		colllectionObj_daily.create_index("ct")
		colllectionObj_daily.create_index("mob")
		colllectionObj_daily.create_index("src")
		colllectionObj_daily.create_index("di")
		colllectionObj_daily.create_index("udid")
		colllectionObj_daily.create_index("av")
		colllectionObj_daily.create_index("f")

###########################################################


	vexpr_col1='tbl_clicktracker_'+str(monthName)+'_'+str(yearName)
	if(vexpr_col1 not in x):
		db.create_collection(vexpr_col1)
		vexpr1= {
			"$jsonSchema" : {
				"bsonType" : "object",
				"properties" : {
					"ip" : {
						"bsonType" : "string",
						"description" : "ip - must be a string lower case"
					},
					"ur" : {
						"bsonType" : "string",
						"description" : "user - must be a string lower case"
					},
					"tm" : {
						"bsonType" : "string",
						"description" : "time - must be a string lower case"
					},
					"rq" : {
						"bsonType" : "string",
						"description" : "request - must be a string"
					},
					"li" : {
						"bsonType" : "string",
						"description" : "link_identifier - must be a string lower case"
					},
					"ll" : {
						"bsonType" : "string",
						"description" : "link_location - must be a string lower case"
					},
					"stc" : {
						"bsonType" : "int",
						"description" : "statuscode - must be a int"
					},
					"bt" : {
						"bsonType" : "int",
						"description" : "bytes - must be a int"
					},
					"ref" : {
						"bsonType" : "string",
						"description" : "referrer - must be a string lower case"
					},
					"ug" : {
						"bsonType" : "string",
						"description" : "useragent - must be a string"
					},
					"r_ip" : {
						"bsonType" : "string",
						"description" : "referrer_ip - must be a string lower case"
					},
					"et" : {
						"bsonType" : "string",
						"description" : "exectime - must be a string lower case"
					},
					"ust" : {
						"bsonType" : "string",
						"description" : "upstreamtime - must be a string lower case"
					},
					"cc" : {
						"bsonType" : "string",
						"description" : "countrycode - must be a string lower case"
					},
					"p" : {
						"bsonType" : "string",
						"description" : "pipe - must be a string lower case"
					},
					"dt" : {
						"bsonType" : [ "date", "null" ],
						"description" : "entry_datetime - must be a datetime"
					},
					"em" : {
						"bsonType" : "string",
						"description" : "entry_month - must be a string lower case"
					},
					"ey" : {
						"bsonType" : "string",
						"description" : "entry_year - must be a string lower case"
					},
					"vt" : {
						"bsonType" : "string",
						"description" : "vertical_tag - must be a string lower case"
					},
					"ct" : {
						"bsonType" : "string",
						"description" : "city - must be a string lower case"
					},
					"mob" : {
						"bsonType" : "string",
						"description" : "mobile - must be a string"
					},
					"src" : {
						"bsonType" : "string",
						"description" : "source - must be a string lower case"
					},
					"di" : {
						"bsonType" : "string",
						"description" : "docid - must be a string"
					},
					"wap" : {
						"bsonType" : "string",
						"description" : "wap - must be a string lower case"
					},
					"jdl" : {
						"bsonType" : "string",
						"description" : "jdlite - must be a string lower case"
					},
					"sid" : {
						"bsonType" : "string",
						"description" : "sid - must be a string lower case"
					},
					"src_p" : {
						"bsonType" : "string",
						"description" : "src_processed - must be a string lower case"
					},
					"udid" : {
						"bsonType" : "string",
						"description" : "udid - must be a string lower case"
					},
					"nid" : {
						"bsonType" : "int",
						"description" : "national_catid - must be a int"
					},
					"md" : {
						"bsonType" : "string",
						"description" : "movie_details - must be a string lower case"
					},
					"av" : {
						"bsonType" : "double",
						"description" : "api_version - must take decimal value"
					}
					}
				}
			}
		

		query1=OrderedDict()
		query1['collMod']='tbl_clicktracker_'+str(monthName)+'_'+str(yearName)
		query1['validator']=vexpr1
		query1['validationLevel']='moderate'
		query1['validationAction']='warn' or 'error'


		db.command(query1)

		collection_daily1='tbl_clicktracker_'+str(monthName)+'_'+str(yearName)

		colllectionObj_daily1=db['tbl_clicktracker_'+str(monthName)+'_'+str(yearName)]  # collection obj



		colllectionObj_daily1.create_index("li")
		colllectionObj_daily1.create_index("ll")
		colllectionObj_daily1.create_index("dt")
		colllectionObj_daily1.create_index("vt")
		colllectionObj_daily1.create_index("ct")
		colllectionObj_daily1.create_index("mob")
		colllectionObj_daily1.create_index("src")
		colllectionObj_daily1.create_index("di")
		colllectionObj_daily1.create_index("nid")
		colllectionObj_daily1.create_index("av")
		colllectionObj_daily1.create_index("wap")
		colllectionObj_daily1.create_index("jdl")
		colllectionObj_daily1.create_index("sid")

###########################################################################################

	vexpr_col2='tbl_clicktracker_monthly_'+str(yearName)
	if(vexpr_col2 not in x):
		db.create_collection(vexpr_col2)
		vexpr2= {
			"$jsonSchema" : {
				"bsonType" : "object",
				"required" : [
					"li",
					"ll",
					"e_mt",
					"vt",
					"ct",
					"src",
					"di",
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
					"e_mt" : {
						"bsonType" : "string",
						"description" : "entry_month - must be a string and is required (Jan,Feb,Mar,Apr,May,Jun,Jul,Aug,Sep,Oct,Nov,Dec)"
					},
					"cnt" : {
						"bsonType" : "int",
						"description" : "cnt - must be a int"
					},
					"wd_cnt" : {
						"bsonType" : "int",
						"description" : "weekday_cnt - must be a int"
					},
					"we_cnt" : {
						"bsonType" : "int",
						"description" : "weekend_cnt - must be a int"
					},
					"reg_m" : {
						"bsonType" : "int",
						"description" : "reg_mobile - must be a int"
					},
					"vt" : {
						"bsonType" : "string",
						"description" : "vertical_tag - must be a string and is required lower case"
					},
					"ct" : {
						"bsonType" : "string",
						"description" : "city - must be a string and is required lower case"
					},
					"src" : {
						"bsonType" : "string",
						"description" : "source - must be a string and is required lower case"
					},
					"di" : {
						"bsonType" : "string",
						"description" : "docid - must be a string and is required"
					},
					"nid" : {
						"bsonType" : "int",
						"description" : "national_catid - must be a int and is required"
					},
					"av" : {
						"bsonType" : "double",
						"description" : "api_version - must take decimal value"
					}
				}
			}
		}


		query2=OrderedDict()
		query2['collMod']='tbl_clicktracker_monthly_'+str(yearName)
		query2['validator']=vexpr2
		query2['validationLevel']='moderate'
		query2['validationAction']='warn' or 'error'


		db.command(query2)

		collection_daily2='tbl_clicktracker_monthly_'+str(yearName)

		colllectionObj_daily2=db['tbl_clicktracker_monthly_'+str(yearName)]  # collection obj


		index_daily2=[("li", pymongo.ASCENDING),("ll", pymongo.ASCENDING),("e_mt", pymongo.ASCENDING),("vt", pymongo.ASCENDING),("ct", pymongo.ASCENDING),("src", pymongo.ASCENDING),("di", pymongo.ASCENDING),("nid", pymongo.ASCENDING),("av", pymongo.ASCENDING)]

		colllectionObj_daily2.create_index(index_daily2,unique=True)   #unique_index
		colllectionObj_daily2.create_index("li")
		colllectionObj_daily2.create_index("ll")
		colllectionObj_daily2.create_index("e_mt")
		colllectionObj_daily2.create_index("vt")
		colllectionObj_daily2.create_index("ct")
		colllectionObj_daily2.create_index("src")
		colllectionObj_daily2.create_index("di")
		colllectionObj_daily2.create_index("nid")
		colllectionObj_daily2.create_index("av")


########################################################
	vexpr_col3='tbl_clicktracker_quarterly_'+str(yearName)
	if(vexpr_col3 not in x):
		db.create_collection(vexpr_col3)
		vexpr3= {
			"$jsonSchema" : {
				"bsonType" : "object",
				"required" : [
					"li",
					"ll",
					"e_qtr",
					"vt",
					"ct",
					"src",
					"di",
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
					"e_qtr" : {
						"bsonType" : "int",
						"description" : "entry_quarter - must be a int and is required (1 - 1st quarter,2 - 2nd quarter,3 - 3rd quarter,4 - 4th quarter)"
					},
					"cnt" : {
						"bsonType" : "int",
						"description" : "cnt - must be a int"
					},
					"wd_cnt" : {
						"bsonType" : "int",
						"description" : "weekday_cnt - must be a int"
					},
					"we_cnt" : {
						"bsonType" : "int",
						"description" : "weekend_cnt - must be a int"
					},
					"reg_m" : {
						"bsonType" : "int",
						"description" : "reg_mobile - must be a int"
					},
					"vt" : {
						"bsonType" : "string",
						"description" : "vertical_tag - must be a string and is required lower case"
					},
					"ct" : {
						"bsonType" : "string",
						"description" : "city - must be a string and is required lower case"
					},
					"src" : {
						"bsonType" : "string",
						"description" : "source - must be a string and is required lower case"
					},
					"di" : {
						"bsonType" : "string",
						"description" : "docid - must be a string and is required"
					},
					"nid" : {
						"bsonType" : "int",
						"description" : "national_catid - must be a int and is required"
					},
					"av" : {
						"bsonType" : "double",
						"description" : "api_version - must take decimal value"
					}
				}
			}
		}


		query3=OrderedDict()
		query3['collMod']='tbl_clicktracker_quarterly_'+str(yearName)
		query3['validator']=vexpr3
		query3['validationLevel']='moderate'
		query3['validationAction']='warn' or 'error'


		db.command(query3)

		collection_daily3='tbl_clicktracker_quarterly_'+str(yearName)

		colllectionObj_daily3=db['tbl_clicktracker_quarterly_'+str(yearName)]  # collection obj

		index_daily3=[("li", pymongo.ASCENDING),("ll", pymongo.ASCENDING),("e_qtr", pymongo.ASCENDING),("vt", pymongo.ASCENDING),("ct", pymongo.ASCENDING),("src", pymongo.ASCENDING),("di", pymongo.ASCENDING),("nid", pymongo.ASCENDING),("av", pymongo.ASCENDING)]

		colllectionObj_daily3.create_index(index_daily3,unique=True)   #unique_index
		colllectionObj_daily3.create_index("li")
		colllectionObj_daily3.create_index("ll")
		colllectionObj_daily3.create_index("e_qtr")
		colllectionObj_daily3.create_index("vt")
		colllectionObj_daily3.create_index("ct")
		colllectionObj_daily3.create_index("src")
		colllectionObj_daily3.create_index("di")
		colllectionObj_daily3.create_index("nid")
		colllectionObj_daily3.create_index("av")



##########################################################################

	vexpr_col4='tbl_clicktracker_yearly_'+str(yearName)
	if(vexpr_col4 not in x):
		db.create_collection(vexpr_col4)
		vexpr4= {
			"$jsonSchema" : {
				"bsonType" : "object",
				"required" : [
					"li",
					"ll",
					"e_yr",
					"vt",
					"ct",
					"src",
					"di",
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
					"e_yr" : {
						"bsonType" : "int",
						"description" : "entry_year - must be a int"
					},
					"cnt" : {
						"bsonType" : "int",
						"description" : "cnt - must be a int"
					},
					"wd_cnt" : {
						"bsonType" : "int",
						"description" : "weekday_cnt - must be a int"
					},
					"we_cnt" : {
						"bsonType" : "int",
						"description" : "weekend_cnt - must be a int"
					},
					"reg_m" : {
						"bsonType" : "int",
						"description" : "reg_mobile - must be a int"
					},
					"vt" : {
						"bsonType" : "string",
						"description" : "vertical_tag - must be a string and is required lower case"
					},
					"ct" : {
						"bsonType" : "string",
						"description" : "city - must be a string and is required lower case"
					},
					"src" : {
						"bsonType" : "string",
						"description" : "source - must be a string and is required lower case"
					},
					"di" : {
						"bsonType" : "string",
						"description" : "docid - must be a string and is required"
					},
					"nid" : {
						"bsonType" : "int",
						"description" : "national_catid - must be a int and is required"
					},
					"av" : {
						"bsonType" : "double",
						"description" : "api_version - must take decimal value"
					}
				}
			}
		}


		query4=OrderedDict()
		query4['collMod']='tbl_clicktracker_yearly_'+str(yearName)
		query4['validator']=vexpr4
		query4['validationLevel']='moderate'
		query4['validationAction']='warn' or 'error'


		db.command(query4)

		collection_daily4='tbl_clicktracker_yearly_'+str(yearName)

		colllectionObj_daily4=db['tbl_clicktracker_yearly_'+str(yearName)]  # collection obj

		index_daily4=[("li", pymongo.ASCENDING),("ll", pymongo.ASCENDING),("e_yr", pymongo.ASCENDING),("vt", pymongo.ASCENDING),("ct", pymongo.ASCENDING),("src", pymongo.ASCENDING),("di", pymongo.ASCENDING),("nid", pymongo.ASCENDING),("av", pymongo.ASCENDING)]

		colllectionObj_daily4.create_index(index_daily4,unique=True)   #unique_index
		colllectionObj_daily4.create_index("li")
		colllectionObj_daily4.create_index("ll")
		colllectionObj_daily4.create_index("e_yr")
		colllectionObj_daily4.create_index("vt")
		colllectionObj_daily4.create_index("ct")
		colllectionObj_daily4.create_index("src")
		colllectionObj_daily4.create_index("di")
		colllectionObj_daily4.create_index("nid")
		colllectionObj_daily4.create_index("av")

###############################################################################################
	vexpr_col5='tbl_mp_event_data_'+str(date1)
   	if(vexpr_col5 not in x1):
		db1.create_collection(vexpr_col5)
		vexpr5={
			"$jsonSchema" : {
			"bsonType" : "object",
			"required" : [ "r_dt", "ssid", "srchid", "di", "pid" ],
			"properties" : {
				"r_dt" : {
					"bsonType" : "date",
					"description" : "entry date - must be timestamp and is required"
				},
				"ssid" : {
					"bsonType" : "string",
					"description" : "session id - must be string in lower case and is required"
				},
				"srchid" : {
					"bsonType" : "string",
					"description" : "search id - must be string in lower case and is required"
				},
				"di" : {
					"bsonType" : "string",
					"description" : "docid - must be string in upper case and is required"
				},
				"bc_di" : {
					"bsonType" : "string",
					"description" : "brand connect docid - must be string in upper case and is required"
				},
				"cm_nm" : {
					"bsonType" : "string",
					"description" : "companyname - must be string and is not required"
				},
				"s_flg" : {
					"bsonType" : "int",
					"description" : "sponsored flag - must be string and is not required"
				},
				"s_bid" : {
					"bsonType" : "decimal",
					"description" : "sponsored bid - must be decimal128 and is not required"
				},
				"bb_flg" : {
					"bsonType" : "int",
					"description" : "best buy flag - must be int and is not required"
				},
				"jdc_flg" : {
					"bsonType" : "int",
					"description" : "justdial choice flag - must be int and is not required"
				},
				"bbx_flg" : {
					"bsonType" : "int",
					"description" : "buy box flag - must be int and is not required"
				},
				"ty" : {
					"bsonType" : "string",
					"description" : "type - must be string in lower case and is not required"
				},
				"pid" : {
					"bsonType" : "string",
					"description" : "product id - must be string  and is not required"
				},
				"pnm" : {
					"bsonType" : "string",
					"description" : "product name - must be string and is not required"
				},
				"d_pid" : {
					"bsonType" : "string",
					"description" : "dc product id - must be string  and is not required"
				},
				"psvnid" : {
					"bsonType" : "string",
					"description" : "psv node id - must be string and is not required"
				},
				"psvnm" : {
					"bsonType" : "string",
					"description" : "psv node name - must be string in lower case and is not required"
				},
				"pc" : {
					"bsonType" : "int",
					"description" : "pincode - must be integer case and is not required"
				},
				"d_ct" : {
					"bsonType" : "string",
					"description" : "data city - must be string in lower case and is not required"
				},
				"ct" : {
					"bsonType" : "string",
					"description" : "city - must be string in lower case and is not required"
				},
				"dsrc" : {
					"bsonType" : "string",
					"description" : "data source - must be string in lower case and is not required"
				},
				"nc" : {
					"bsonType" : "int",
					"description" : "national catid - must be int and is not required"
				},
				"paid" : {
					"bsonType" : "int",
					"description" : "paid - must be int and is not required"
				},
				"rf" : {
					"bsonType" : "string",
					"description" : "referrer - must be string in lower and is not required"
				},
				"rf_flg" : {
					"bsonType" : "int",
					"description" : "referrer_flag - must be int and is not required"
				},
				"rfa" : {
					"bsonType" : "string",
					"description" : "referrer_attribute - must be string in lower case and is not required"
				},
				"rfa_flg" : {
					"bsonType" : "int",
					"description" : "referrer_attribute_flag - must be int and is not required"
				},
				"pt" : {
					"bsonType" : "string",
					"description" : "page type - must be string in lower case and is not required"
				},
				"pt_flg" : {
					"bsonType" : "int",
					"description" : "page type flag - must be int and is not required"
				},
				"evt" : {
					"bsonType" : "string",
					"description" : "event name - must be string in lower case and is not required"
				},
				"evt_flg" : {
					"bsonType" : "int",
					"description" : "event name flag - must be int and is not required"
				},
				"sevt" : {
					"bsonType" : "string",
					"description" : "sub event name - must be string in lower case and is not required"
				},
				"sevt_flg" : {
					"bsonType" : "int",
					"description" : "sub event name flag - must be int and is not required"
				},
				"uip" : {
					"bsonType" : "string",
					"description" : "user ip - must be string and is not required"
				},
				"ar" : {
					"bsonType" : "string",
					"description" : "areaname - must be string in lower case and is not required"
				},
				"jduid" : {
					"bsonType" : "string",
					"description" : "jduid - must be string and is not required"
				},
				"dv_src" : {
					"bsonType" : "int",
					"description" : "device source - must be int and is not required"
				},
				"dv_id" : {
					"bsonType" : "string",
					"description" : "device id - must be string and is not required"
				},
				"cat_nm" : {
					"bsonType" : "string",
					"description" : "category name - must be string in lower case and is not required"
				},
				"ua" : {
					"bsonType" : "string",
					"description" : "user agent - must be string and is not required"
				},
				"ci" : {
					"bsonType" : "int",
					"description" : "campaign id - must be int and is not required"
				},
				"pos" : {
					"bsonType" : "int",
					"description" : "position - must be int and is not required"
				},
				"d_flg" : {
					"bsonType" : "int",
					"description" : "deduction flag - must be int and is not required"
				},
				"ob" : {
					"bsonType" : "decimal",
					"description" : "opening balance - must be decimal128 and is not required"
				},
				"oc" : {
					"bsonType" : "decimal",
					"description" : "closing balance - must be decimal128 and is not required"
				},
				"ccb" : {
					"bsonType" : "object",
					"description" : "consolidated closing balance - must be object and is not required"
				},
				"ins" : {
					"bsonType" : "string",
					"description" : "instrument id - must be string and is not required"
				},
				"ord_id" : {
					"bsonType" : "string",
					"description" : "ordered id - must be string and is not required"
				},
				"txd" : {
					"bsonType" : "object",
					"description" : "txn details - must be object and is not required"
				},
				"ods_com" : {
					"bsonType" : "object",
					"description" : "ods_commission_info - must be object and is not required"
				},
				"ls" : {
					"bsonType" : "int",
					"description" : "lead_source - must be int and is not required"
				},
				"orq" : {
					"bsonType" : "int",
					"description" : "origin_req - must be int and is not required"
				},
				"st" : {
					"bsonType" : "int",
					"description" : "search_type - must be int and is not required"
				},
				"lt_cl" : {
					"bsonType" : "int",
					"description" : "late_call - must be int and is not required"
				},
				"th_id" : {
					"bsonType" : "int",
					"description" : "thread_id - must be int and is not required"
				},
				"mt_flg" : {
					"bsonType" : "int",
					"description" : "monetary_flag - must be int and is not required"
				},
				"gst_amt" : {
					"bsonType" : "decimal",
					"description" : "gst_amt - must be decimal128 and is not required"
				},
				"txn_amt" : {
					"bsonType" : "decimal",
					"description" : "transaction_amount - must be decimal128 and is not required"
				},
				"amt" : {
					"bsonType" : "decimal",
					"description" : "amt_deducted - must be decimal128 and is not required"
				},
				"n_amt" : {
					"bsonType" : "decimal",
					"description" : "net_amt_deducted - must be decimal128 and is not required"
				},
				"d_amt" : {
					"bsonType" : "decimal",
					"description" : "delta_amt_deducted - must be decimal128 and is not required"
				},
				"disc_amt" : {
					"bsonType" : "decimal",
					"description" : "discount_amt_deducted - must be decimal128 and is not required"
				},
				"bal" : {
					"bsonType" : "decimal",
					"description" : "balance - must be decimal128 and is not required"
				},
				"n_bal" : {
					"bsonType" : "decimal",
					"description" : "net_balance - must be decimal128 and is not required"
				},
				"d_bal" : {
					"bsonType" : "decimal",
					"description" : "delta_balance - must be decimal128 and is not required"
				},
				"disc_bal" : {
					"bsonType" : "decimal",
					"description" : "discount_balance - must be decimal128 and is not required"
				},
				"d_dt" : {
					"bsonType" : [ "date", "null" ],
					"description" : "deduction_date - must be timestamp and is not required"
				},
				"txn_dt" : {
					"bsonType" : [ "date", "null" ],
					"description" : "transaction_time - must be timestamp and is not required"
				},
				"dup_cal" : {
					"bsonType" : "int",
					"description" : "duplicate_caller - must be int and is not required"
				},
				"inv_ci" : {
					"bsonType" : "int",
					"description" : "charged_campaign_id - must be int and is not required"
				},
				"c_dsrc" : {
					"bsonType" : "string",
					"description" : "contract_datasrc - must be string in lower and is not required"
				},
				"cm_ty" : {
					"bsonType" : "string",
					"description" : "campaign_type - must be string in lower and is not required"
				},
				"ost" : {
					"bsonType" : "int",
					"description" : "old_structure - must be int and is not required"
				},
				"ps_flg" : {
					"bsonType" : "int",
					"description" : "ps_flg - must be int and is not required"
				},
				"is_rfd" : {
					"bsonType" : "int",
					"description" : "is_refunded - must be int and is not required"
				},
				"rfd_id" : {
					"bsonType" : "string",
					"description" : "refund_obj_id - must be string and is not required"
				},
				"on_rfd" : {
					"bsonType" : "string",
					"description" : "on_refund_obj_id - must be string and is not required"
				},
				"rf_dt" : {
					"bsonType" : [ "date", "null" ],
					"description" : "refund_date - must be timestamp and is not required"
				},
				"rv_dt" : {
					"bsonType" : [ "date", "null" ],
					"description" : "reversal_date - must be timestamp and is not required"
				},
				"ad_amt" : {
					"bsonType" : "decimal",
					"description" : "monetized_adjusted_amt - must be decimal128 and is not required"
				},
				"ad_n_amt" : {
					"bsonType" : "decimal",
					"description" : "monetized_adjusted_net_amt - must be decimal128 and is not required"
				},
				"ad_d_amt" : {
					"bsonType" : "decimal",
					"description" : "monetized_adjusted_delta_amt - must be decimal128 and is not required"
				},
				"ad_disc_amt" : {
					"bsonType" : "decimal",
					"description" : "monetized_adjusted_discount_amt - must be decimal128 and is not required"
				},
				"fdi" : {
					"bsonType" : "object",
					"description" : "finance_deduction_info - must be object and is not required"
				},
				"ctid" : {
					"bsonType" : "string",
					"description" : "ctid - must be string and is required"
				},
				"logic_id" : {
					"bsonType" : "string",
					"description" : "logic_id - must be string and is required"
				},
				"kw_id" : {
					"bsonType" : "string",
					"description" : "keyword_id - must be string and is required"
				},
				"kw_nm" : {
					"bsonType" : "string",
					"description" : "keyword_ name - must be string and is required"
				},
				"dc" : {
					"bsonType" : "string",
					"description" : "data_city - must be string and is required"
				},
				"bn_flg" : {
					"bsonType" : "int",
					"description" : "banner flag - must be int and is not required"
				}
			}
		}
	}
    
	query5=OrderedDict()
	query5['collMod']='tbl_mp_event_data_'+str(date1)
	query5['validator']=vexpr5
	query5['validationLevel']='moderate'
	query5['validationAction']='warn' or 'error'


	db1.command(query5)

	collection_daily5='tbl_mp_event_data_'+str(date1)

	colllectionObj_daily5=db1['tbl_mp_event_data_'+str(date1)]  # collection obj
	colllectionObj_daily5.create_index("bn_flg")

############################################################################################################################		
	vexpr_col6='tbl_mp_event_unique_data_'+str(date1)
   	if(vexpr_col6 not in x1):
		db1.create_collection(vexpr_col6)
        	vexpr6={
		"$jsonSchema" : {
			"bsonType" : "object",
			"required" : [
				"e_dt",
				"ssid",
				"di",
				"pid",
				"d_pid",
				"evt_flg"
			],
			"properties" : {
				"e_dt" : {
					"bsonType" : "date",
					"description" : "entry date - must be timestamp and is required"
				},
				"ssid" : {
					"bsonType" : "string",
					"description" : "session id - must be string in lower case  and is required"
				},
				"di" : {
					"bsonType" : "string",
					"description" : "docid - must be string  in upper case  and is not required"
				},
				"pid" : {
					"bsonType" : "string",
					"description" : "product id  - must be string  and is required"
				},
				"d_pid" : {
					"bsonType" : "string",
					"description" : "dc product id  - must be string and is required"
				},
				"evt_flg" : {
					"bsonType" : "int",
					"description" : "event flag - must be int and is required"
				},
				"sevt_flg" : {
					"bsonType" : "int",
					"description" : "sub event flag - must be int and is required"
				},
				"pt_flg" : {
					"bsonType" : "int",
					"description" : "page type flag - must be int and is required"
				},
				"dv_src" : {
					"bsonType" : "int",
					"description" : "device source - must be int and is required"
				},
				"pos" : {
					"bsonType" : "int",
					"description" : "position - must be int and is required"
				},
				"bn_flg" : {
					"bsonType" : "int",
					"description" : "banner flag - must be int and is not required"
				}
			}
		}}

		query6=OrderedDict()
		query6['collMod']='tbl_mp_event_unique_data_'+str(date1)
		query6['validator']=vexpr6
		query6['validationLevel']='moderate'
		query6['validationAction']='warn' or 'error'
	

		db1.command(query6)

		collection_daily6='tbl_mp_event_unique_data_'+str(date1)

		colllectionObj_daily6=db1['tbl_mp_event_unique_data_'+str(date1)]  # collection obj

		index_daily6=[("e_dt", pymongo.ASCENDING),("ssid", pymongo.ASCENDING),("di", pymongo.ASCENDING),("pid", pymongo.ASCENDING),("d_pid", pymongo.ASCENDING),("evt_flg", pymongo.ASCENDING)]

		colllectionObj_daily6.create_index(index_daily6,unique=True) 
###########################################
else:
	kris=datetime.date(kri)	
	t3=kris+timedelta(1) 
        t3=str(t3)
	krish3=datetime.strptime(t3,"%Y-%m-%d") #first day of next month
        monthName = krish3.strftime("%b").lower()
	yearName = krish3.strftime("%Y")
	month = krish3.strftime("%Y%m")
        date1=krish3.strftime("%Y%m%d")
	vexpr_col5='tbl_mp_event_data_'+str(date1)
   	if(vexpr_col5 not in x1):
		db1.create_collection(vexpr_col5)
		vexpr5={
			"$jsonSchema" : {
			"bsonType" : "object",
			"required" : [ "r_dt", "ssid", "srchid", "di", "pid" ],
			"properties" : {
				"r_dt" : {
					"bsonType" : "date",
					"description" : "entry date - must be timestamp and is required"
				},
				"ssid" : {
					"bsonType" : "string",
					"description" : "session id - must be string in lower case and is required"
				},
				"srchid" : {
					"bsonType" : "string",
					"description" : "search id - must be string in lower case and is required"
				},
				"di" : {
					"bsonType" : "string",
					"description" : "docid - must be string in upper case and is required"
				},
				"bc_di" : {
					"bsonType" : "string",
					"description" : "brand connect docid - must be string in upper case and is required"
				},
				"cm_nm" : {
					"bsonType" : "string",
					"description" : "companyname - must be string and is not required"
				},
				"s_flg" : {
					"bsonType" : "int",
					"description" : "sponsored flag - must be string and is not required"
				},
				"s_bid" : {
					"bsonType" : "decimal",
					"description" : "sponsored bid - must be decimal128 and is not required"
				},
				"bb_flg" : {
					"bsonType" : "int",
					"description" : "best buy flag - must be int and is not required"
				},
				"jdc_flg" : {
					"bsonType" : "int",
					"description" : "justdial choice flag - must be int and is not required"
				},
				"bbx_flg" : {
					"bsonType" : "int",
					"description" : "buy box flag - must be int and is not required"
				},
				"ty" : {
					"bsonType" : "string",
					"description" : "type - must be string in lower case and is not required"
				},
				"pid" : {
					"bsonType" : "string",
					"description" : "product id - must be string  and is not required"
				},
				"pnm" : {
					"bsonType" : "string",
					"description" : "product name - must be string and is not required"
				},
				"d_pid" : {
					"bsonType" : "string",
					"description" : "dc product id - must be string  and is not required"
				},
				"psvnid" : {
					"bsonType" : "string",
					"description" : "psv node id - must be string and is not required"
				},
				"psvnm" : {
					"bsonType" : "string",
					"description" : "psv node name - must be string in lower case and is not required"
				},
				"pc" : {
					"bsonType" : "int",
					"description" : "pincode - must be integer case and is not required"
				},
				"d_ct" : {
					"bsonType" : "string",
					"description" : "data city - must be string in lower case and is not required"
				},
				"ct" : {
					"bsonType" : "string",
					"description" : "city - must be string in lower case and is not required"
				},
				"dsrc" : {
					"bsonType" : "string",
					"description" : "data source - must be string in lower case and is not required"
				},
				"nc" : {
					"bsonType" : "int",
					"description" : "national catid - must be int and is not required"
				},
				"paid" : {
					"bsonType" : "int",
					"description" : "paid - must be int and is not required"
				},
				"rf" : {
					"bsonType" : "string",
					"description" : "referrer - must be string in lower and is not required"
				},
				"rf_flg" : {
					"bsonType" : "int",
					"description" : "referrer_flag - must be int and is not required"
				},
				"rfa" : {
					"bsonType" : "string",
					"description" : "referrer_attribute - must be string in lower case and is not required"
				},
				"rfa_flg" : {
					"bsonType" : "int",
					"description" : "referrer_attribute_flag - must be int and is not required"
				},
				"pt" : {
					"bsonType" : "string",
					"description" : "page type - must be string in lower case and is not required"
				},
				"pt_flg" : {
					"bsonType" : "int",
					"description" : "page type flag - must be int and is not required"
				},
				"evt" : {
					"bsonType" : "string",
					"description" : "event name - must be string in lower case and is not required"
				},
				"evt_flg" : {
					"bsonType" : "int",
					"description" : "event name flag - must be int and is not required"
				},
				"sevt" : {
					"bsonType" : "string",
					"description" : "sub event name - must be string in lower case and is not required"
				},
				"sevt_flg" : {
					"bsonType" : "int",
					"description" : "sub event name flag - must be int and is not required"
				},
				"uip" : {
					"bsonType" : "string",
					"description" : "user ip - must be string and is not required"
				},
				"ar" : {
					"bsonType" : "string",
					"description" : "areaname - must be string in lower case and is not required"
				},
				"jduid" : {
					"bsonType" : "string",
					"description" : "jduid - must be string and is not required"
				},
				"dv_src" : {
					"bsonType" : "int",
					"description" : "device source - must be int and is not required"
				},
				"dv_id" : {
					"bsonType" : "string",
					"description" : "device id - must be string and is not required"
				},
				"cat_nm" : {
					"bsonType" : "string",
					"description" : "category name - must be string in lower case and is not required"
				},
				"ua" : {
					"bsonType" : "string",
					"description" : "user agent - must be string and is not required"
				},
				"ci" : {
					"bsonType" : "int",
					"description" : "campaign id - must be int and is not required"
				},
				"pos" : {
					"bsonType" : "int",
					"description" : "position - must be int and is not required"
				},
				"d_flg" : {
					"bsonType" : "int",
					"description" : "deduction flag - must be int and is not required"
				},
				"ob" : {
					"bsonType" : "decimal",
					"description" : "opening balance - must be decimal128 and is not required"
				},
				"oc" : {
					"bsonType" : "decimal",
					"description" : "closing balance - must be decimal128 and is not required"
				},
				"ccb" : {
					"bsonType" : "object",
					"description" : "consolidated closing balance - must be object and is not required"
				},
				"ins" : {
					"bsonType" : "string",
					"description" : "instrument id - must be string and is not required"
				},
				"ord_id" : {
					"bsonType" : "string",
					"description" : "ordered id - must be string and is not required"
				},
				"txd" : {
					"bsonType" : "object",
					"description" : "txn details - must be object and is not required"
				},
				"ods_com" : {
					"bsonType" : "object",
					"description" : "ods_commission_info - must be object and is not required"
				},
				"ls" : {
					"bsonType" : "int",
					"description" : "lead_source - must be int and is not required"
				},
				"orq" : {
					"bsonType" : "int",
					"description" : "origin_req - must be int and is not required"
				},
				"st" : {
					"bsonType" : "int",
					"description" : "search_type - must be int and is not required"
				},
				"lt_cl" : {
					"bsonType" : "int",
					"description" : "late_call - must be int and is not required"
				},
				"th_id" : {
					"bsonType" : "int",
					"description" : "thread_id - must be int and is not required"
				},
				"mt_flg" : {
					"bsonType" : "int",
					"description" : "monetary_flag - must be int and is not required"
				},
				"gst_amt" : {
					"bsonType" : "decimal",
					"description" : "gst_amt - must be decimal128 and is not required"
				},
				"txn_amt" : {
					"bsonType" : "decimal",
					"description" : "transaction_amount - must be decimal128 and is not required"
				},
				"amt" : {
					"bsonType" : "decimal",
					"description" : "amt_deducted - must be decimal128 and is not required"
				},
				"n_amt" : {
					"bsonType" : "decimal",
					"description" : "net_amt_deducted - must be decimal128 and is not required"
				},
				"d_amt" : {
					"bsonType" : "decimal",
					"description" : "delta_amt_deducted - must be decimal128 and is not required"
				},
				"disc_amt" : {
					"bsonType" : "decimal",
					"description" : "discount_amt_deducted - must be decimal128 and is not required"
				},
				"bal" : {
					"bsonType" : "decimal",
					"description" : "balance - must be decimal128 and is not required"
				},
				"n_bal" : {
					"bsonType" : "decimal",
					"description" : "net_balance - must be decimal128 and is not required"
				},
				"d_bal" : {
					"bsonType" : "decimal",
					"description" : "delta_balance - must be decimal128 and is not required"
				},
				"disc_bal" : {
					"bsonType" : "decimal",
					"description" : "discount_balance - must be decimal128 and is not required"
				},
				"d_dt" : {
					"bsonType" : [ "date", "null" ],
					"description" : "deduction_date - must be timestamp and is not required"
				},
				"txn_dt" : {
					"bsonType" : [ "date", "null" ],
					"description" : "transaction_time - must be timestamp and is not required"
				},
				"dup_cal" : {
					"bsonType" : "int",
					"description" : "duplicate_caller - must be int and is not required"
				},
				"inv_ci" : {
					"bsonType" : "int",
					"description" : "charged_campaign_id - must be int and is not required"
				},
				"c_dsrc" : {
					"bsonType" : "string",
					"description" : "contract_datasrc - must be string in lower and is not required"
				},
				"cm_ty" : {
					"bsonType" : "string",
					"description" : "campaign_type - must be string in lower and is not required"
				},
				"ost" : {
					"bsonType" : "int",
					"description" : "old_structure - must be int and is not required"
				},
				"ps_flg" : {
					"bsonType" : "int",
					"description" : "ps_flg - must be int and is not required"
				},
				"is_rfd" : {
					"bsonType" : "int",
					"description" : "is_refunded - must be int and is not required"
				},
				"rfd_id" : {
					"bsonType" : "string",
					"description" : "refund_obj_id - must be string and is not required"
				},
				"on_rfd" : {
					"bsonType" : "string",
					"description" : "on_refund_obj_id - must be string and is not required"
				},
				"rf_dt" : {
					"bsonType" : [ "date", "null" ],
					"description" : "refund_date - must be timestamp and is not required"
				},
				"rv_dt" : {
					"bsonType" : [ "date", "null" ],
					"description" : "reversal_date - must be timestamp and is not required"
				},
				"ad_amt" : {
					"bsonType" : "decimal",
					"description" : "monetized_adjusted_amt - must be decimal128 and is not required"
				},
				"ad_n_amt" : {
					"bsonType" : "decimal",
					"description" : "monetized_adjusted_net_amt - must be decimal128 and is not required"
				},
				"ad_d_amt" : {
					"bsonType" : "decimal",
					"description" : "monetized_adjusted_delta_amt - must be decimal128 and is not required"
				},
				"ad_disc_amt" : {
					"bsonType" : "decimal",
					"description" : "monetized_adjusted_discount_amt - must be decimal128 and is not required"
				},
				"fdi" : {
					"bsonType" : "object",
					"description" : "finance_deduction_info - must be object and is not required"
				},
				"ctid" : {
					"bsonType" : "string",
					"description" : "ctid - must be string and is required"
				},
				"logic_id" : {
					"bsonType" : "string",
					"description" : "logic_id - must be string and is required"
				},
				"kw_id" : {
					"bsonType" : "string",
					"description" : "keyword_id - must be string and is required"
				},
				"kw_nm" : {
					"bsonType" : "string",
					"description" : "keyword_ name - must be string and is required"
				},
				"dc" : {
					"bsonType" : "string",
					"description" : "data_city - must be string and is required"
				},
				"bn_flg" : {
					"bsonType" : "int",
					"description" : "banner flag - must be int and is not required"
				}
			}
		}
	}
    
		query5=OrderedDict()
		query5['collMod']='tbl_mp_event_data_'+str(date1)
		query5['validator']=vexpr5
		query5['validationLevel']='moderate'
		query5['validationAction']='warn' or 'error'


		db1.command(query5)

		collection_daily5='tbl_mp_event_data_'+str(date1)

		colllectionObj_daily5=db1['tbl_mp_event_data_'+str(date1)]  # collection obj
		colllectionObj_daily5.create_index("bn_flg")

############################################################################################################################		
	vexpr_col6='tbl_mp_event_unique_data_'+str(date1)
   	if(vexpr_col6 not in x1):
		db1.create_collection(vexpr_col6)
        	vexpr6={
		"$jsonSchema" : {
			"bsonType" : "object",
			"required" : [
				"e_dt",
				"ssid",
				"di",
				"pid",
				"d_pid",
				"evt_flg"
			],
			"properties" : {
				"e_dt" : {
					"bsonType" : "date",
					"description" : "entry date - must be timestamp and is required"
				},
				"ssid" : {
					"bsonType" : "string",
					"description" : "session id - must be string in lower case  and is required"
				},
				"di" : {
					"bsonType" : "string",
					"description" : "docid - must be string  in upper case  and is not required"
				},
				"pid" : {
					"bsonType" : "string",
					"description" : "product id  - must be string  and is required"
				},
				"d_pid" : {
					"bsonType" : "string",
					"description" : "dc product id  - must be string and is required"
				},
				"evt_flg" : {
					"bsonType" : "int",
					"description" : "event flag - must be int and is required"
				},
				"sevt_flg" : {
					"bsonType" : "int",
					"description" : "sub event flag - must be int and is required"
				},
				"pt_flg" : {
					"bsonType" : "int",
					"description" : "page type flag - must be int and is required"
				},
				"dv_src" : {
					"bsonType" : "int",
					"description" : "device source - must be int and is required"
				},
				"pos" : {
					"bsonType" : "int",
					"description" : "position - must be int and is required"
				},
				"bn_flg" : {
					"bsonType" : "int",
					"description" : "banner flag - must be int and is not required"
				}
			}
		}
       }

		query6=OrderedDict()
		query6['collMod']='tbl_mp_event_unique_data_'+str(date1)
		query6['validator']=vexpr6
		query6['validationLevel']='moderate'
		query6['validationAction']='warn' or 'error'


		db1.command(query6)

		collection_daily6='tbl_mp_event_unique_data_'+str(date1)

		colllectionObj_daily6=db1['tbl_mp_event_unique_data_'+str(date1)]  # collection obj

		index_daily6=[("e_dt", pymongo.ASCENDING),("ssid", pymongo.ASCENDING),("di", pymongo.ASCENDING),("pid", pymongo.ASCENDING),("d_pid", pymongo.ASCENDING),("evt_flg", pymongo.ASCENDING)]

		colllectionObj_daily6.create_index(index_daily6,unique=True) 
###########################################

	
	
