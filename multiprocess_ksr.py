import multiprocessing as mp
from pprint import pprint
from functools import partial
import qryContext
import sys
import parser_mp
from pprint import pprint
import db_test
from datetime import date, timedelta, datetime
import pytz
import sys
import pymongo
from collections import OrderedDict
timezone = pytz.timezone('Asia/Calcutta')
utc = pytz.utc
import os
import re
import json

location_list = ['Comments Page','Social Feed','Top Stories','video','Share Pop_Feed','Sponsored_Post','top','Fashion And Beauty ','spirituality','India','latest','Automobile','Share Pop_Comments','football','My City ','Latest Stories','health','jobs','tech','fashion','Business','entertainment','Gaming','World','cricket','Videos','Environment','Social_Feed','money','sports','opinionandblogs','lifestyle','Economy Politics','News_Share','Sponsored_Post_Share','mycity','astrology','Horoscope ','Politics','Wealth','Travel','Trending']

def utcToIst(inputDateTime):
    return inputDateTime.replace(tzinfo=pytz.utc).astimezone(timezone)
def istToUtc(inputDateTime):
    return inputDateTime.astimezone(utc)


def process_data(files,filedates,i):
    file=files[i] 
    filedates=filedates
    filedates_obj=datetime.strptime(filedates, "%Y%m%d%H%M")
    HHMMFiles = filedates_obj.strftime("%H%M")
    if HHMMFiles == '0000' :
        filedates_obj=filedates_obj  - timedelta(days=1)
        
    monthName = filedates_obj.strftime("%b").lower()
    yearName = filedates_obj.strftime("%Y")

      
    mongo_click_Obj=db_test.MONGODATABASE('mongo_mp_clicktracker_13_65')  
    click_Obj=mongo_click_Obj.connections('mp_clicktracker')
    raw_click_collection = 'tbl_clicktracker_'+monthName+"_"+str(yearName)
    raw_click_collection_object=click_Obj[raw_click_collection]    
    
    mongo_click_Obj1=db_test.MONGODATABASE('mongo_mp_clicktracker_13_65')  
    click_Obj1=mongo_click_Obj1.connections('mp_clicktracker')
    raw_click_collection1 = 'tbl_clicktracker_daily_202201'
    raw_click_collection_object1=click_Obj1[raw_click_collection1] 


    try:
        with open(file,'r') as f:
            for line in f:
                parserObj=parser_mp.PARSER(log=line)
                parserResponse=parserObj.fileFormatting()
                data=OrderedDict()
                
                data['ip'] =  parserResponse.get('remote_ip','')
                data['ur'] =  parserResponse.get('user','')
                data['tm'] =  parserResponse.get('tm','')
                data['rq'] =  json.dumps(parserResponse.get('request',''))
                data['li'] =  parserResponse.get('request',{}).get('link_idf','').lower()
                data['ll'] =  parserResponse.get('request',{}).get('lnk_loc','').lower()
                try:
                    data['stc'] = int(parserResponse.get('statuscode',0))  
                except : 
                    data['stc'] =0
                try:
                    data['bt'] = int(parserResponse.get('bytes',0)) 
                except :
                    data['bt'] =0
                data['ref'] = parserResponse.get('referrer','').lower()
                data['ug'] = parserResponse.get('useragent','')
                data['r_ip'] = parserResponse.get('referrer_ip','').lower()
                data['et'] = parserResponse.get('exectime','').lower()
                data['ust'] = parserResponse.get('upstreamtime','').lower()
                data['cc'] = parserResponse.get('countrycode','').lower()
                data['p'] = parserResponse.get('pipe','').lower()
                try:
                     data['dt'] = istToUtc(parserResponse.get('time',None))  
                except:
                     data['dt']=None
                data['em'] = parserResponse.get('entry_month','')
                data['ey'] = parserResponse.get('entry_year','')
                data['vt'] = parserResponse.get('request',{}).get('li_vtl','').lower() 
                data['ct'] = parserResponse.get('request',{}).get('city','').lower()
                data['mob'] =  parserResponse.get('request',{}).get('mobile','')
                data['src'] = parserResponse.get('request',{}).get('source','').lower()
                data['di'] = parserResponse.get('request',{}).get('docid','').lower()
                data['wap'] = parserResponse.get('request',{}).get('wap','').lower()
                data['jdl'] = parserResponse.get('request',{}).get('jdlite','').lower()
                data['sid'] = parserResponse.get('request',{}).get('sid','').lower()
                data['src_p'] = parserResponse.get('src_processed','web').lower()
                data['udid'] = parserResponse.get('request',{}).get('udid','').lower()
                try:
                    data['nid'] = int(parserResponse.get('request',{}).get('ncatid',0))
                except :
                    data['nid'] =0
                data['md'] = parserResponse.get('request',{}).get('movie_details','').lower()
                try:
                    data['av'] = float(parserResponse.get('request',{}).get('version',''))
                except :
                    data['av']=float(0)
                if(re.search('[a-zA-Z]', data['mob'])):
                    data['mob']=''
                if(data['jdl']=='1' and data['wap']=="11"):
                    data['src_p']='jdlite'
                if(data['src']=='b2bweb'):
                    data['src_p']='b2bweb'
                if(data['src_p']==';'):
                    data['src_p']='other'
                if(data['src']=='jdmartweb'):
                    data['src_p']='jdmartweb'

                
                if(len(data['li'].replace('+',''))>20 and (data['ll'] in location_list) and (data['jdl'] and data['jdl'][0]=='1')):
                    data['li']='social_blank'
                    print(data['li'])
                
                data['li']= data['li'].replace('+','').replace('' ,'<>').replace('><','').replace('<>','')        
                data['ll']= data['ll'].replace('+','').replace('' ,'<>').replace('><','').replace('<>','')        
                data['md']= data['md'].replace('+','').replace('' ,'<>').replace('><','').replace('<>','') 

                index_data=OrderedDict()
		index_data['li']=data['li']
		index_data['ll']=data['ll']
		index_data['dt']=data['dt']
		index_data['vt']=data['vt']
		index_data['ct']=data['ct']
		index_data['mob']=data['mob']
		index_data['src']=data['src']
		index_data['di']=data['di']
		index_data['udid']=data['udid']
		index_data['nid']=data['nc']
		index_data['av']=data['av']

		extra_data=OrderedDict()
		extra_data['ed']=calendar.day_name[data['dt'].weekday()])
		extra_data['sid']=data['sid']
		extra_data['md']=data['md']
		extra_data['f']=0

		x='%'
		x1=['-','~']
		x2='"'
		if x in index_data['li']:
   			extra_data['f']=10
		else:
    			for i in x1:
        			if i not in index_data['li']:
           				extra_data['f']=10


		x3=['\'','\"','^','GoogleCan u Just','Google,can u Just']

		for i in x3:
    			if i in index_data['li']:
           			extra_data['f']=10

		if (extra_data['f'] != 10 and x2 in  index_data['li'] ):
           		index_data['li']=index_data['li'].replace(x2,'')


		if  data :
                    	count_raw_res = qryContext.QRYCONTEXT.mongo_count(raw_click_collection_object1,index_data,extra_data)
                    	if coun_raw_res.get('e',1) == 1:
                        	print("erorr:",insert_raw_res)
                        	return qryContext.QRYCONTEXT.ack(e=1,m="count data insert error") 

                p=dict(data)
                # print(p)
    
                if  data :
                    insert_raw_res = qryContext.QRYCONTEXT.insert_one(raw_click_collection_object,data)
                    if insert_raw_res.get('e',1) == 1:
                        print("erorr:",insert_raw_res)
                        return qryContext.QRYCONTEXT.ack(e=1,m="raw data insert error") 
                                       
                    
                           
    except Exception as e:
        print("unable to open file:",e)
        qryContext.QRYCONTEXT.ack(e=1,m="unable to open file")   
    
    if os.path.isfile(file):
        os.remove(file)              
    return qryContext.QRYCONTEXT.ack(d=1)

class MULTI():
    def __init__(self,**kwargs):
        self.case=kwargs.get('case',None)
        self.noOfThread=kwargs.get('noOfThread',10)
        self.files=kwargs.get('files',None)
        self.filedates=kwargs.get('filedates',None)
                

    def multiProcessingPreProcess(self):
        if self.case == 1:
            itr= list(range(len(self.files)))
            # itr= list(range(2,3))
            func = partial(process_data,self.files,self.filedates)
    
        with mp.Pool(processes=10) as pool:
            try:
                res=pool.map(func,itr)
            except Exception as e:
                return qryContext.QRYCONTEXT.ack(e=1,m='error in multiprocess--->:'+str(e))
            pool.close()
            pool.join()
            return qryContext.QRYCONTEXT.ack(d=res)
