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
##for load file

#for ObjectId
from bson.objectid import ObjectId





if __name__=="__main__":
    main_t1=datetime.now()
    print('Main Start Time ==> {}\n'.format(main_t1.strftime('%X')))
    dirDate=main_t1.strftime("%Y%m%d")

    ap = argparse.ArgumentParser()
    ap.add_argument('-1', '--detail', required=True, type=str)      # detail ==> db_old.col_old.id.db_new.col_new
    ap.add_argument('-2', '--testing', required=True, type=int)
    args = vars(ap.parse_args())

    locals().update(args)
    # print(args)

    print('User Given Command Line Argument:\n{}'.format(json.dumps(args,indent=4)))

    if testing is None:
        testing = 0
    
    if detail:
        detail = detail.split(',')
    
    for i in args:
        args[i]=eval(i)
    print('Processed Command Line Argument:\n{}'.format(json.dumps(args,indent=4)))


    for temp_db_col in detail:
        temp_db_col = temp_db_col.split('.')
        db_old = temp_db_col[0]
        col_old = temp_db_col[1]
        d1=temp_db_col[2]
        d2=temp_db_col[3]
        db_new = temp_db_col[4]
        col_new = temp_db_col[5]
        
        #print(f"temp_db_col==>{temp_db_col},\n db_old==>{db_old},\n col_old==>{col_old},\n d1==>{d1},\n d2==>{d2},\n db_new==>{db_new},\n col_new==>{col_new}\n")
        #data_move_to_structre_collection(db_old, col_old, id, db_new, col_new, testing)
    print(db_old)
    print(col_old)
    print(d1)
    print(d2)
    print(db_new)
    print(col_new)
    main_t2=datetime.now()
    print('\n\nMain End Time ==> {} AND Execution Time ==> {}\n\n'.format(main_t2.strftime('%X'), main_t2-main_t1))





# NEW (20220505)
#python /home/justdial/Downloads/move_coleection.py --testing=1 --detail=db_searchmis.tbl_websearches_client_2022_old.2022-05-04T18:30:00Z.2022-05-05T06:10:00Z.db_searchmis.tbl_websearches_client_2022
#'''
