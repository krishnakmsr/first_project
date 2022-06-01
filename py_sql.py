import sys
import csv
import itertools
from collections import deque
import re
# from decimal import *
import math
import multiprocessing as mp
import time
from datetime import date, timedelta, datetime
import pandas as pd


import argparse
# from ast import literal_eval

import mysql.connector as c
from mysql.connector import Error


if __name__ == '__main__':
    con = None

    con = c.connect(host='172.29.86.28', database='trainee_vipink', user='vipink', password='justdial$123')
    # con = c.connect(host='192.168.12.147', database='test', user='vipink', password='vipink!@#')
    # con = c.connect(host='172.29.67.213', database='test', user='vipink', password='vipink@123')

    # con = c.connect(database='vipin', user='root', password='pass')

    cur = con.cursor()

    cur.execute('SELECT * FROM A')
    row_headers=[x[0] for x in cur.description] #this will extract row headers
    rv = cur.fetchall()
    json_data=[]
    for result in rv:
        json_data.append(dict(zip(row_headers,result)))
    # json.dumps(json_data)

    print(json_data)
    #
    # for i in x:
    #     sql='select count() from ' + str(i[0])
