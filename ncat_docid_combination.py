# query_log_categorymaster_migration.py

from posixpath import split
from pymongo import MongoClient
from pymongo import ReadPreference,WriteConcern
import pymongo
import json
from ast import literal_eval
import sys
import csv
import itertools
from collections import deque
import re
import math
import multiprocessing as mp
import time
from datetime import date, timedelta, datetime
from pytz import timezone
import pandas as pd
from pymongo.collation import Collation
from functools import partial
import os
from bson.codec_options import CodecOptions
from decimal import Decimal
from bson.decimal128 import Decimal128
from bson.int64 import Int64
import argparse
import urllib.parse
import unicodedata

# 19938568

filenameToProcess_op = "/home/justdial/ncat_docid_combination/ncat_docid_combination_op.csv"

def insertionData(testing,itr):
    filenumber = str(itr).zfill(6)
    if testing:
        filenameToProcess = '/home/justdial/ncat_docid_combination/files{}'.format(filenumber)
        # filenameToProcess = '/home/justdial/query_log_categorymaster_migration/files/test'
    else:
        filenameToProcess = '/home/justdial/ncat_docid_combination/files{}'.format(filenumber)

    print(itr, filenameToProcess)

    readFile = open(filenameToProcess, 'r')
    reader = csv.reader(readFile, delimiter='\t', quoting=csv.QUOTE_ALL)

    error = 0
    try:
        for i in reader:
            nc = i[0]
            di = i[1].split(',')
            res_f = []
            for j in di:
                res_f.append([nc, j])
            writeFile = open(filenameToProcess_op, 'a')
            writer = csv.writer(writeFile, delimiter='#', quoting=csv.QUOTE_ALL)
            writer.writerows(res_f)
            writeFile.close()
            res_f.clear()
        
        readFile.close()
    except Exception as e:
        print(f"Error : {e}")
        error = 1


    if error==0:
        os.remove(filenameToProcess)



if __name__=='__main__':
    main_t1=datetime.now()
    print('Main Start Time ==> {}\n'.format(main_t1.strftime('%X')))
    dateFormat=main_t1.strftime("%Y%m%d")

    ap = argparse.ArgumentParser()
    ap.add_argument("-1", "--np", required=False, type=int)
    ap.add_argument("-2", "--testing", required=False, type=int, default=1)
    ap.add_argument("-3", "--no_of_files", required=False, type=int, default=0)
    ap.add_argument("-4", "--file_no", required=False, type=str, default=0)

    # ap.add_argument("-5", "--db", required=True, type=str)
    # ap.add_argument("-6", "--tbl", required=True, type=str)

    args = vars(ap.parse_args())

    locals().update(args)
    # print(args)

    print('User Given Command Line Argument:\n{}'.format(json.dumps(args,indent=4)))

    if np is None or np>15:
        np = 15
    
    if file_no:
        file_no = file_no.split(',')

    for i in args:
        args[i]=eval(i)
    print('Processed Command Line Argument:\n{}'.format(json.dumps(args,indent=4)))


    if file_no:
        itr = iter(file_no)
    else:
        itr = iter(range(no_of_files))


    pool = mp.Pool(processes=np)
    func = partial(insertionData,testing)
    pool.map(func,itr)
    pool.close()
    pool.join()

    main_t2=datetime.now()
    print('\n\nMain End Time ==> {} AND Execution Time ==> {}\n\n'.format(main_t2.strftime('%X'), main_t2-main_t1))
