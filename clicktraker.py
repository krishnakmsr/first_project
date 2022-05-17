import sys
#import apis
import qryContext
import argparse
from pprint import pprint
from datetime import datetime,timedelta
import db
import os 
import glob
import math
import multiprocess



def clicktracker(**kwargs):
    dir="/home/justdial/Desktop/clicktracker_final1/*"
    print(dir)
    filename=kwargs.get('filename')
    #print(filename)
    filegrep=dir+str(filename)+"*"
    files=[]
    #print(filegrep)
    for f in glob.glob(filegrep):
        files.append(f.strip())
    print(files)
    triggerRes=clicktrackerMultiProcessTrigger(files=files,filedates=filename)
    if triggerRes.get('e',1) == 1:
        return qryContext.QRYCONTEXT.ack(e=1,m=triggerRes)
    return qryContext.QRYCONTEXT.ack(m=files) 

def clicktrackerMultiProcessTrigger(**kwargs):
    files=kwargs.get('files')
    filedates=kwargs.get('filedates')
    noOfThreads    =10
    lengthOfList=int(math.ceil(len(files)/noOfThreads))
    j,k=0,noOfThreads
    #print(files)
    for i in range(lengthOfList):
        subList=files[j:k]
        try:
            multiObj=multiprocess_mp.MULTI(files=subList,case=1,filedates=filedates)
            multiprocessData=multiObj.multiProcessingPreProcess()
        except Exception as e :
            print("multiprocessing fileprocess failed :",e)
            return qryContext.QRYCONTEXT.ack(e=1,m=str(e))  
            
        if  multiprocessData.get('e',1) == 1:
            print("multiprocessing fileprocess failed:",multiprocessData)
            return qryContext.QRYCONTEXT.ack(e=1,m=multiprocessData)
        
        j+=noOfThreads
        k+=noOfThreads  
        
  #  mysqlClickObj=db.MYSQLDATABASE('search_mis_mongo_lookup_tbl')  
  #  conMysqlClick=mysqlClickObj.connections()
  # cursor=conMysqlClick.cursor()
  # try:
  #     sql='''update clicktracker.tbl_clicktracker_file_lookup set done_flag = 2 where file_name_date_part="%s" ''' %(filedates)
  #     cursor.execute(sql)
  #     conMysqlClick.commit()
  # except Exception as e :
  #     print("clicktracker.tbl_clicktracker_file_lookup  update failed :",e)
  #     return qryContext.QRYCONTEXT.ack(e=1,m=e)      
  # 
  # return qryContext.QRYCONTEXT.ack(m='success') 
       


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument("-1", "--cases", required=True)
    ap.add_argument("-2", "--filename", required=False)
    print(ap)
    
    args = vars(ap.parse_args())
    print(args)
    if args["cases"] is not None:
        try:
            cases = args["cases"]
        except:
            cases=None
    else:
        cases=None   

    if args["filename"] is not None:
            try:
                filename = args["filename"]
            except:
                filename=None
    else:
        filename=None              

    if cases :
        pass
    else:
        print("Improper case is passed:",cases)            
        raise 


    if cases == 'clicktracker' :
        
        if filename :
            # res = clicktracker(filename=filename)
            pass
        else:
            print("Improper filename is passed:",filename)            
            raise 
        res = clicktracker(filename=filename)
        
        if res.get('e',1) == 1:
            print("erorr:",res)
            raise
        else:
            print("clicktracker-Response:",res)


        
           

