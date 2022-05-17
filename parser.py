
import json
from qryContext import *
import urllib,re
import urllib.parse
from datetime import date, timedelta, datetime
from pprint import pprint
import sys
class PARSER(object):
    def __init__(self, **kwargs):
        self.log=kwargs.get('log')
        self.log_type=kwargs.get('tsv')
    
    def fileFormatting(self):
        clicktracker_src_mapping={
            '1':'app',
            '2':'tpro',
            '3':'ios',
            '10':'web',
            'web':'web',
            '21':'jdmart_android',
            '22':'jdmart_touch',
            '23':'jdmart_ios',
            'b2bweb':'b2bweb',
            'jdmartweb':'jdmartweb'            
        }        
        try:
            log_lst = self.log.split("\t")
        except Exception as e:
            log_lst =[]
            print("except in log split:",e) 
            

        try:
            ip_log = log_lst[0]
        except :
            ip_log=''
        try:
            user = log_lst[1]
        except :
            user=''
            
        try:
            time_log =  log_lst[2]
        except :
            time_log = ''
        
        try:
            requests_log = log_lst[3]
        except :
            requests_log =''
        try:
            statuscode = log_lst[4]
        except :
            statuscode =''
        try:
            bytes = log_lst[5]
        except :
            bytes =''    
        try:
            referrer = log_lst[6]
        except :
            referrer =''
        try:
            useragent = log_lst[7]
        except :
            useragent =''    
        try:
            link_identifier_log = log_lst[8]
        except :
            link_identifier_log =''
            
        try:
            link_location_log = log_lst[9]
        except :
            link_location_log =''
        

            
        try:
            requests_log=requests_log.split("php?")

            del requests_log[0]
            requests_log=requests_log[0]
            requests_log = ''.join(requests_log).replace(" HTTP/1.1","")
            requests_log = urllib.parse.unquote(urllib.parse.unquote(requests_log))
            request = requests_log.split('&')
            request_dict=dict(x.split('=')[0:2] for x in request)
            time_stamp = request_dict.get('time_stamp','')
            print(time_stamp)
            if time_stamp :
                time_stamp_obj=datetime.strptime(time_stamp, "%Y-%m-%d+%H:%M:%S")
                # 2021-08-04+00:06:04
            else:
                time_stamp_obj=None
            time_stamp_obj1=time_stamp_obj.strftime("%Y-%m-%d+%H:%M:%S")

            request_dict['time_stamp_obj']=time_stamp_obj1 
            
            
            
            src=request_dict.get('source','')       
            wap=request_dict.get('wap','')       
            try:
                jdlite=int(request_dict.get('jdlite',0))
            except :
                jdlite=0    
                
            if  jdlite == 1 :
                src_processed = 'jdlite'
            elif  clicktracker_src_mapping.get(src,None):
                src_processed = clicktracker_src_mapping.get(src,None)
            elif clicktracker_src_mapping.get(wap,None): 
                src_processed = clicktracker_src_mapping.get(wap,None)   
            else:
                src_processed='web' 
              
            request_dict['src_processed'] =src_processed
            
            request_dict.pop("time_stamp", None)
                
        except :
            request_dict={}
            time_stamp_obj=None
        time =  datetime.strptime(time_log, "%d/%b/%Y:%H:%M:%S")
        month = time.month
        year = time.year
   
        final_dict={}
        final_dict['remote_ip'] = ip_log
        final_dict['user'] = user
        final_dict['tm'] = time_log
        final_dict['request'] = request_dict  
        final_dict['statuscode'] = statuscode      
        final_dict['bytes'] = bytes   
        final_dict['referrer'] = referrer
        final_dict['useragent'] = useragent  
        final_dict['entry_month']=str(month)
        final_dict['entry_year']=str(year)
        final_dict['time']=time_stamp_obj
        return final_dict
        
