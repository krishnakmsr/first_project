from collections import OrderedDict
import pymongo
from pymongo import MongoClient
from pymongo import ReadPreference
from pprint import pprint
import json
import sys
import csv
import itertools
from collections import deque
from functools import partial

import re
from decimal import *
import math
import multiprocessing as mp
import time
from datetime import date, timedelta, datetime
from pymongo.collation import Collation
import argparse
from ast import literal_eval
import os
import mysql.connector as mysql
#import pymysql ##for load file

#for ObjectId
from bson.objectid import ObjectId
import pymongo
import dateutil
from dateutil import parser

def data_move_to_structre_collection(db_old, col_old,d1,d2, db_new, col_new,count_col):
    
    clientRead = MongoClient('mongodb://192.168.13.65:27017')
    clientWrite = MongoClient('mongodb://192.168.13.65:27017')
    
    dbRead=clientRead[db_old]
    collectionNameRead = col_old
    collectionRead = dbRead[collectionNameRead]
    collection_cnts= count_col
    collectionWrite1 = dbRead[collection_cnts]    

    dbWrite=clientWrite[db_new]
    collectionNameWrite = col_new
    collectionWrite = dbWrite[collectionNameWrite]

    #print(collectionRead)
    #print(collectionWrite)
    dateStr1 = d1
    dateStr2 = d2
    myDatetime1 = dateutil.parser.parse(dateStr1)
    myDatetime2 = dateutil.parser.parse(dateStr2)
    query = {}
    if d1:
        query ={ "e_dt": {
                "$gte": myDatetime1,
                "$lte": myDatetime2
              }}
    count = collectionRead.count(query)
    #print(count)
    agg_result= collectionRead.aggregate([
          {
            "$match": {
              "e_dt": {
                "$gte": myDatetime1,
                "$lte": myDatetime2
              }
            }
          },
          {
            "$group": {
              "_id": {
                "date": {
                  "$dateToString": {
                    "format": "%Y-%m-%d",
                    "date": "$e_dt",
                    "timezone": "Asia/Calcutta"
                  }
                },
                 "src": "$src"
						}, "cnt": {"$sum": 1}
          }
					}
        ])
#####################################
    k1=[]
    for i in agg_result:
        k1.append(i)
    res_f=[]
    query1=OrderedDict()
    for i in k1:
    	e_dt=i.get("_id").get("date",None)
    	src=i.get("_id").get("src","")
    	cnt=i.get('cnt',0)
        res=OrderedDict()
        res["e_dt"]=datetime.strptime(e_dt, "%Y-%m-%d")
        res["src"]=src
        res["cnt"]=cnt
    	#res={
           # "e_dt":datetime.strptime(e_dt, "%Y-%m-%d"),
           # "src":src,
           # "cnt":cnt
    		#}
    	res_f.append(res)
    #print(res_f)
    collectionWrite1.insert_many(res_f)

    var_skip = 0
    var_limit = 1000
    while(var_skip <= count):
        data=[]
        #print(db_old, col_old, d1,d2, var_skip, var_limit, count)
        data = list(collectionRead.find(query).skip(var_skip).limit(var_limit))
        c=0
        if data:
            try:
                collectionWrite.insert_many(data)
                c=c+1
                if(c>0):
                    #print("delete data")
                    collectionRead.delete_many(query).skip(var_skip).limit(var_limit)
            except Exception as e:
                print("error")
        else:
            break
        var_skip = var_skip + var_limit
        # break       # TESTING

    clientRead.close()
    clientWrite.close()



if __name__=="__main__":
    main_t1=datetime.now()
    print('Main Start Time ==> {}\n'.format(main_t1.strftime('%X')))
    dirDate=main_t1.strftime("%Y%m%d")

    ap = argparse.ArgumentParser()
    ap.add_argument('-1', '--detail', required=True, type=str)      # detail ==> db_old.col_old.id.db_new.col_new
    #ap.add_argument('-2', '--testing', required=True, type=int)
    args = vars(ap.parse_args())

    locals().update(args)
    # print(args)

    #print('User Given Command Line Argument:\n{}'.format(json.dumps(args,indent=4)))
    
    if detail:
        detail = detail.split(',')
    
    for i in args:
        args[i]=eval(i)
    #print('Processed Command Line Argument:\n{}'.format(json.dumps(args,indent=4)))


    for temp_db_col in detail:
        temp_db_col = temp_db_col.split('.')
        db_old = temp_db_col[0]
        col_old = temp_db_col[1]
        d1=temp_db_col[2]
        d2=temp_db_col[3]
        db_new = temp_db_col[4]
        col_new = temp_db_col[5]
        count_col=temp_db_col[6]
    	#print(db_old)
    	#print(col_old)
    	#print(d1)
    	#print(d2)
    	#print(db_new)
    	#print(col_new) 
        #print(count_col)       
        #print(f"temp_db_col==>{temp_db_col},\n db_old==>{db_old},\n col_old==>{col_old},\n d1==>{d1},\n d2==>{d2},\n db_new==>{db_new},\n col_new==>{col_new}\n\n")
        data_move_to_structre_collection(db_old, col_old,d1,d2, db_new, col_new,count_col)

    main_t2=datetime.now()
    print('\n\nMain End Time ==> {} AND Execution Time ==> {}\n\n'.format(main_t2.strftime('%X'), main_t2-main_t1))


#db: db_clicktracker_mp
#tbl_clicktracker_daily_202202  from table

#tbl_clicktracker_daily_202202_test to table 
#db_test


#python /home/justdial/Downloads/collection_move_test.py --testing=1 --detail=db_searchinput_mp.tbl_unq_user_data.#2022-05-05T00:00:00+05:30.2022-05-06T00:00:00+05:30.db_test.tbl_unq_user_data_test#



##python /home/justdial/Downloads/collection_move_test.py --testing=1 --detail=db_searchinput_mp.tbl_unq_user_data.2022-05-08T18:30:00+05:30.2022-05-09T18:30:00+05:30.db_searchinput_mp.tbl_unq_user_data_202205.tbl_unq_user_cnts_2022


#"e_dt": {
               # "$gte": ISODate("2022-05-04T18:30:00.000Z"),
               # "$lte": ISODate("2022-05-05T18:29:59.999Z")
             # }



##python /home/justdial/Downloads/collection_move_test.py --testing=1 --detail=db_searchinput_mp.tbl_unq_user_data.2022-05-08T18:30:00.000.2022-05-09T18:29:59.999.db_searchinput_mp.tbl_unq_user_data_202205.tbl_unq_user_cnts_2022

#python /home/justdial/Downloads/collection_move_test.py  --detail=db_searchinput_mp.tbl_unq_user_data.2022-05-06T18:30:00.2022-05-07T18:29:59.db_searchinput_mp.tbl_unq_user_data_202205.tbl_unq_user_cnts_2022

