from ast import Try
from logging import exception
from db import *
from qryContext import *
from datetime import date, timedelta, datetime
# from parser import *
# import logging
# from csvPorting import *
import json

import urllib,re
import urllib.parse

from pytz import timezone
from decimal import Decimal
from bson.decimal128 import Decimal128
from collections import OrderedDict


# 35.126
# Script Path: '/scripts/mis_scripts/'



class migration_elastic_to_mongo:
    def __init__(self, day):
        self.day = day
        if self.day == 1:
            print("\n Fetching {} day old data".format(day))
        else:
            print("\n Fetching {} days old data".format(day))

        self.stddate = date.today() - timedelta(int(self.day))
        self.table_date = self.stddate.strftime('%Y-%m-%d')
        self.table_year = self.stddate.strftime('%Y')
        self.table_yearmonth = self.stddate.strftime('%Y%m')    # collection_name year_month
        self.table_date1 = self.stddate.strftime('%d%m%Y')
        self.stdate = self.stddate.strftime('%Y-%m-%d') + "T00:00:00.000Z"
        self.stdate = datetime.strptime(self.stdate, '%Y-%m-%dT%H:%M:%S.000Z') - timedelta(hours=5,minutes=30)
        self.enddate = self.stddate.strftime('%Y-%m-%d') + "T23:59:59.999Z"
        self.enddate = datetime.strptime(self.enddate, '%Y-%m-%dT%H:%M:%S.999Z') - timedelta(hours=5,minutes=30)
        self.enddate = self.enddate.strftime('%Y-%m-%d') + "T" + self.enddate.strftime('%H:%M:%S.999Z')
        self.stdate = self.stdate.strftime('%Y-%m-%d') + "T" + self.stdate.strftime('%H:%M:%S.000Z')

        print('\n Data Fetch Interval: \n From: {} \n To: {} \n'.format(self.stdate, self.enddate))
        # print('self.table_yearmonth: ', self.table_yearmonth)

        self.collection_create_1_of_every_month(self.table_yearmonth)

    # IP : Connection, Database
    # localhost : mongo_clicktracker_local,db_autosuggest
    # 13.65 : mongo_clicktracker_13_65,search_input
    # 35.126 : mongo_clicktracker_35_126,db_autosuggest
    # 35.126 : mongo_clicktracker_35_126_vipink, db_autosuggest

    ## Anusha Narayan changes
    ## changes done at 2021-06-07

    # web_jdmart
    def webAutoLog_JDMart(self):
        # Mongo Connection
        # mongoObj = MONGODATABASE(connection='mongo_clicktracker_35_126')
        mongoObj = MONGODATABASE(connection='mongo_searches_13_65_testing')
        mongoCon = mongoObj.connections(databaseName='db_autosuggest')
        mongoConCollection = 'tbl_autolog_{}'.format(self.table_yearmonth)
        mongoConCollectionObj = mongoCon[mongoConCollection]

        print('mongoConCollectionObj: {}'.format(mongoConCollectionObj))
        print("stdate",self.stdate,"enddate",self.enddate,"table_year",self.table_yearmonth)

        # Elastic Connection
        elasticObj = ELASTICDATABASE(connection='app_and_touch') # jdmart data comes from 192.168.41.27
        elasticCon = elasticObj.connections()

        # print('elasticCon: {}'.format(elasticCon))

        if not elasticCon.ping():
            raise ValueError("Connection failed")
        else:
            print("Elastic connected")
        #################website autusuggest##########################
        qry = {"query":
                   {"bool":
                       {"must": [
                                   {
                                       "query_string":
                                       {
                                           "analyze_wildcard":"true",
                                           "query":"_type:\"www-access\" AND  request:\"autoLog.php\""
                                       }
                                   },
                                   {
                                       "range": {
                                           "@timestamp": {
                                               "gte": self.stdate,
                                               "lt": self.enddate ,
                                               "time_zone": "+05:30"
                                               }
                                           }
                                   }

                               ]
                       }
                   }
                }
        print(f'\nWeb Jdmart qry: {qry}\n') #testing
        page = QRYCONTEXT.esSearch(elasticCon, qry)
        page = page.get('d', None)

        WebsiteTotalCnt = page["hits"]["total"]
        print("website (JD Mart) total cnt",WebsiteTotalCnt)

        for doc in page["hits"]["hits"]:
            id_test = doc['_id']
            index_test = doc['_index']
            log = doc["_source"]["message"]
            line={}
            line= self.logFormattting(log, 'web_jdmart', id_test, index_test)

            dataIns = QRYCONTEXT.insert_one(mongoConCollectionObj, line)
            if dataIns.get('e', 1) == 1:
                print("error in subsequent insert one:", dataIns)

        sid = page['_scroll_id']
        scroll_size = page['hits']['total']
        while (scroll_size > 0):
            page = QRYCONTEXT.esScroll(elasticCon, sid, '2m')
            page = page.get('d', None)
            # page =elasticCon.scroll(scroll_id=sid, scroll='10m')
            sid = page['_scroll_id']
            scroll_size = len(page['hits']['hits'])
            for doc in page["hits"]["hits"]:
                id_test = doc['_id']
                index_test = doc['_index']
                log = doc["_source"]["message"]
                line={}
                line= self.logFormattting(log, 'web_jdmart', id_test, index_test)

                dataIns = QRYCONTEXT.insert_one(mongoConCollectionObj, line)
                if dataIns.get('e', 1) == 1:
                    print("error in subsequent insert one:", dataIns)

        mongoObj.disconnectCon()
        elasticObj.disconnectCon()
    
    ## Anusha Narayan changes
    ## changes done at 2021-06-07

    # web_justdial
    def webAutoLog(self):
        # Mongo Connection
        # mongoObj = MONGODATABASE(connection='mongo_clicktracker_35_126')
        mongoObj = MONGODATABASE(connection='mongo_searches_13_65_testing')
        mongoCon = mongoObj.connections(databaseName='db_autosuggest')
        mongoConCollection = 'tbl_autolog_{}'.format(self.table_yearmonth)
        mongoConCollectionObj = mongoCon[mongoConCollection]

        # print('mongoConCollectionObj: {}'.format(mongoConCollectionObj))
        # print("stdate",self.stdate,"enddate",self.enddate,"table_year",self.table_yearmonth)

        # Elastic Connection
        elasticObj = ELASTICDATABASE(connection='web')
        elasticCon = elasticObj.connections()

        # print('elasticCon: {}'.format(elasticCon))

        if not elasticCon.ping():
            raise ValueError("Connection failed")
        else:
            print("Elastic connected")
        #################website autusuggest##########################
        qry = {"query":
                   {"bool":
                       {"must": [
                                   {
                                       "query_string":
                                       {
                                           "analyze_wildcard":"true",
                                           "query":"_type:\"www-access\" AND  request:\"autoLog.php\""
                                       }
                                   },
                                   {
                                       "range": {
                                           "@timestamp": {
                                               "gte": self.stdate,
                                               "lt": self.enddate ,
                                               "time_zone": "+05:30"
                                               }
                                           }
                                   }

                               ]
                       }
                   }
                }
        print(f'\nWeb Justdial qry: {qry}\n') #testing
        page = QRYCONTEXT.esSearch(elasticCon, qry)
        page = page.get('d', None)

        WebsiteTotalCnt = page["hits"]["total"]
        print("website total cnt",WebsiteTotalCnt)

        for doc in page["hits"]["hits"]:
            id_test = doc['_id']
            index_test = doc['_index']
            log = doc["_source"]["message"]
            line={}
            line= self.logFormattting(log, 'web_justdial', id_test, index_test)

            dataIns = QRYCONTEXT.insert_one(mongoConCollectionObj, line)
            if dataIns.get('e', 1) == 1:
                print("error in subsequent insert one:", dataIns)

        sid = page['_scroll_id']
        scroll_size = page['hits']['total']
        while (scroll_size > 0):
            page = QRYCONTEXT.esScroll(elasticCon, sid, '2m')
            page = page.get('d', None)
            # page =elasticCon.scroll(scroll_id=sid, scroll='10m')
            sid = page['_scroll_id']
            scroll_size = len(page['hits']['hits'])
            for doc in page["hits"]["hits"]:
                id_test = doc['_id']
                index_test = doc['_index']
                log = doc["_source"]["message"]
                line={}
                line= self.logFormattting(log, 'web_justdial', id_test, index_test)

                dataIns = QRYCONTEXT.insert_one(mongoConCollectionObj, line)
                if dataIns.get('e', 1) == 1:
                    print("error in subsequent insert one:", dataIns)

        mongoObj.disconnectCon()
        elasticObj.disconnectCon()


    def touchAutoLog(self):
        # Mongo Connection
        # mongoObj = MONGODATABASE(connection='mongo_clicktracker_35_126')
        mongoObj = MONGODATABASE(connection='mongo_searches_13_65_testing')
        mongoCon = mongoObj.connections(databaseName='db_autosuggest')
        mongoConCollection = 'tbl_autolog_{}'.format(self.table_yearmonth)
        mongoConCollectionObj = mongoCon[mongoConCollection]

        # print('mongoConCollectionObj: {}'.format(mongoConCollectionObj))
        # print("stdate",self.stdate,"enddate",self.enddate,"table_year",self.table_yearmonth)

        # Elastic Connection
        elasticObj = ELASTICDATABASE(connection='app_and_touch')
        elasticCon = elasticObj.connections()

        # print('elasticCon: {}'.format(elasticCon))


        if not elasticCon.ping():
            raise ValueError("Connection failed")
        else:
            print("Elastic connected")

        qry = {"query":
                   {"bool":
                       {"must": [
                                   {

                                       "query_string":
                                       {
                                           "analyze_wildcard":"true",
                                           "query":"_type:\"touch-access\" AND  request:\"autotracker.php\""
                                       }
                                   },
                                   {
                                       "range": {
                                           "@timestamp": {
                                               "gte": self.stdate,
                                               "lt": self.enddate ,
                                               "time_zone": "+05:30"
                                               }
                                           }
                                   }

                               ]
                       }
                   }
                }
        print(f'\nTouch (Justdial/JD Mart) qry: {qry}\n') #testing
        page = QRYCONTEXT.esSearch(elasticCon, qry)
        page = page.get('d', None)

        touchTotalCnt = page["hits"]["total"]
        print("touch total cnt",touchTotalCnt)

        for doc in page["hits"]["hits"]:
            id_test = doc['_id']
            index_test = doc['_index']
            log = doc["_source"]["message"]
            line = {}
            line = self.logFormattting(log, 'touch', id_test, index_test)

            dataIns = QRYCONTEXT.insert_one(mongoConCollectionObj, line)
            if dataIns.get('e', 1) == 1:
                print("error in subsequent insert one:", dataIns)
        
        sid = page['_scroll_id']
        scroll_size = page['hits']['total']
        while (scroll_size > 0):
            page = QRYCONTEXT.esScroll(elasticCon, sid, '2m')
            page = page.get('d', None)
            # page = elasticCon.scroll(scroll_id=sid, scroll='10m')
            sid = page['_scroll_id']
            scroll_size = len(page['hits']['hits'])
            for doc in page["hits"]["hits"]:
                id_test = doc['_id']
                index_test = doc['_index']
                log = doc["_source"]["message"]
                line = {}
                line = self.logFormattting(log, 'touch', id_test, index_test)

                dataIns = QRYCONTEXT.insert_one(mongoConCollectionObj, line)
                if dataIns.get('e', 1) == 1:
                    print("error in subsequent insert one:", dataIns)

        mongoObj.disconnectCon()
        elasticObj.disconnectCon()


    # app=ios
    def appAutoLog(self):
        # Mongo Connection
        # mongoObj = MONGODATABASE(connection='mongo_clicktracker_35_126')
        mongoObj = MONGODATABASE(connection='mongo_searches_13_65_testing')
        mongoCon = mongoObj.connections(databaseName='db_autosuggest')
        mongoConCollection = 'tbl_autolog_{}'.format(self.table_yearmonth)
        mongoConCollectionObj = mongoCon[mongoConCollection]

        # print('mongoConCollectionObj: {}'.format(mongoConCollectionObj))
        # print("stdate",self.stdate,"enddate",self.enddate,"table_year",self.table_yearmonth)

        # Elastic Connection
        elasticObj = ELASTICDATABASE(connection='app_and_touch')
        elasticCon = elasticObj.connections()

        # print('elasticCon: {}'.format(elasticCon))


        try:
            if not elasticCon.ping():
                raise ValueError("Connection failed")
            else:
                print("Elastic connected")
        except Exception as e:
            print(f'Error: {e}')


        #################app autosuggest##########################
        # request:"autotracker.php"  AND  _type:"mobileapi-access" AND !request:"05july2019" AND !request:"20march2020"

        qry = {"query":
                {"bool":
                    {"must": [
                                {
                                    "query_string":
                                    {
                                        "analyze_wildcard":"true",
                                        "query":"_type:\"mobileapi-access\" AND request:\"autotracker.php\"          AND !request:\"05july2019\"                 AND !request:\"20march2020\""
                                    }
                                },
                                {
                                    "range": {
                                        "@timestamp": {
                                            "gte": self.stdate,
                                            "lt": self.enddate ,
                                            "time_zone": "+05:30"
                                            }
                                        }
                                }

                            ]
                    }
                }
             }
        
        print(f'\nApp (Justdial/JD Mart) qry: {qry}\n') #testing

        page = QRYCONTEXT.esSearch(elasticCon, qry)
        page = page.get('d', None)

        appTotalCnt = page["hits"]["total"]
        print("app total cnt",appTotalCnt)

        for doc in page["hits"]["hits"]:
            id_test = doc['_id']
            index_test = doc['_index']
            log = doc["_source"]["message"]
            line = {}
            # line = self.logFormattting(log, 'app')
            line = self.logFormattting(log, 'app', id_test, index_test)

            dataIns = QRYCONTEXT.insert_one(mongoConCollectionObj, line)
            if dataIns.get('e', 1) == 1:
                print("error in subsequent insert one:", dataIns)

        sid = page['_scroll_id']
        scroll_size = page['hits']['total']
        while (scroll_size > 0):
            page = QRYCONTEXT.esScroll(elasticCon, sid, '2m')
            page = page.get('d', None)
            # page = elasticCon.scroll(scroll_id=sid, scroll='10m')
            sid = page['_scroll_id']
            scroll_size = len(page['hits']['hits'])
            for doc in page["hits"]["hits"]:
                id_test = doc['_id']
                index_test = doc['_index']
                log = doc["_source"]["message"]
                line = {}
                # line = self.logFormattting(log, 'app')
                line = self.logFormattting(log, 'app', id_test, index_test)

                dataIns = QRYCONTEXT.insert_one(mongoConCollectionObj, line)
                if dataIns.get('e', 1) == 1:
                    print("error in subsequent insert one:", dataIns)

        mongoObj.disconnectCon()
        elasticObj.disconnectCon()



    def logFormattting(self, log, logtype, id_test, index_test):

        # self.src = {
        #     'app' : -1,
        #     'web' : 7,
        #     'touch' : -2
        # }
        # self.src = self.src[logtype]

        ## Anusha Narayan changes
        ## changes done at 2021-06-07
        self.src = {
            'app' : -1,
            'web_justdial' : 10,
            'web_jdmart' : 27,
            'touch' : -2
        }
        self.src = self.src[logtype]
        ## Anusha Narayan changes
        ## changes done at 2021-06-07

        self.keyval = {
            'status'              : '',
            'body_bytes_sent'     : '',
            'remote_user'         : '',
            'http_referer'        : '',
            'remote_addr'         : '',
            'request'             : '',
            'http_user_agent'     : '',
            'time_local'          : ''
        }
        self.keyval_final = {}

        conf = '$remote_addr - $remote_user [$time_local] "$request" $status $body_bytes_sent "$http_referer" "$http_user_agent"'
        regex = ''.join(
        '(?P<' + g + '>.*?)' if g else re.escape(c)
        for g, c in re.findall(r'\$(\w+)|(.)', conf))

        m = re.match(regex, log)
        logdict = m.groupdict()

        # print(f'logdict: {logdict}\n') #testing

        if type(logdict) is dict:
            if 'status' in logdict:
                self.keyval['status'] = urllib.parse.unquote(logdict['status']).replace("+"," ")
            if 'body_bytes_sent' in logdict:
                self.keyval['body_bytes_sent'] = urllib.parse.unquote(logdict['body_bytes_sent']).replace("+"," ")
            if 'remote_user' in logdict:
                self.keyval['remote_user'] = urllib.parse.unquote(logdict['remote_user']).replace("+"," ")

            if 'http_referer' in logdict:
                self.keyval['http_referer'] = urllib.parse.unquote(logdict['http_referer']).replace("+"," ")
            if 'remote_addr' in logdict:
                self.keyval['remote_addr'] = urllib.parse.unquote(logdict['remote_addr']).replace("+"," ")

            if 'http_user_agent' in logdict:
                self.keyval['http_user_agent'] = urllib.parse.unquote(logdict['http_user_agent']).replace("+"," ").replace("%20AND%20"," & ")

            if 'time_local' in logdict:
                # self.keyval['time_local'] = urllib.parse.unquote(logdict['time_local']).replace("+"," ").replace("%20AND%20"," & ")
                self.keyval['time_local'] = urllib.parse.unquote(logdict['time_local']).replace("%20AND%20"," & ")
                # "18/Oct/2020:09:30:31  0530"
                # self.keyval['time_local'] = datetime.strptime(self.keyval['time_local'][:-6], '%d/%b/%Y:%H:%M:%S').astimezone(timezone('UTC'))
                # self.keyval['time_local'] = datetime.strptime(self.keyval['time_local'], '%d/%b/%Y:%H:%M:%S %z').astimezone(timezone('UTC'))
                self.keyval['time_local'] = datetime.strptime(self.keyval['time_local'], '%d/%b/%Y:%H:%M:%S %z')

            if  'request' in logdict:
                # if logtype == 'app':
                #     self.keyval['src'] = 1
                # elif logtype == 'touch':
                #     self.keyval['src'] = 2
                # if logtype == 'web':
                #     self.keyval['src'] = 3
                request = logdict['request']
                request = request.split("php?")
                del request[0]
                request = ''.join(request).replace(" HTTP/1.1","")
                request = request.replace("%20&%20","%20AND%20").replace("==","")
                if logtype == 'app':
                    self.keyval['request'] = request.strip("&")
                else:
                    self.keyval['request'] = request
                
                # print(f'request: {request}\n') #testing

                if request:
                    error_flag=0
                    try:
                        ## Old One
                        # self.keyval['request'] = dict(x.split('=')[0:2] for x in self.keyval['request'].split('&'))

                        ## testing Part Start
                        self.keyval['request'] = self.keyval['request'].split('&')
                        try:
                            self.keyval['request'] = dict(x.split('=')[0:2] for x in self.keyval['request'])
                        except Exception as e:
                            while 1:
                                test = 0
                                for index in range(len(self.keyval['request'])):
                                    if '=' not in self.keyval['request'][index]:
                                        test = 1
                                        break
                                if test == 0:
                                    break
                                else:
                                    test_value = self.keyval['request'][index-1] + '&' + self.keyval['request'][index]
                                    self.keyval['request'].pop(index)
                                    self.keyval['request'].pop(index-1)
                                    self.keyval['request'].insert(index-1, test_value)

                            self.keyval['request'] = dict(x.split('=')[0:2] for x in self.keyval['request'])
                        ## testing Part End

                        ##print("self.keyval['request']",self.keyval['request'])
                        self.keyval['request'] = dict((k.lower(), v) for k,v in self.keyval['request'].items())  # for converting all keys into lowercase
                        for key in self.keyval['request'] :
                            nkey = 'data_'+key
                            #nkey = key
                            try:
                                self.keyval[nkey] = urllib.parse.unquote(self.keyval['request'][key]).replace("+"," ")
                                # self.keyval['request'][key] = self.keyval[nkey]
                            except Exception as e:
                                print("error in request parsing 1",e)
                            try:
                                jsonData = json.loads(urllib.parse.unquote(self.keyval['request'][key]))
                                # self.keyval['request'][key] = jsonData
                                # self.keyval[nkey] = jsonData
                            except Exception as e:
                                jsonData = {}
                            if logtype=='app':
                                if  isinstance(jsonData,dict) :
                                    for key,val in jsonData.items():
                                        nkey2 = 'data_'+key
                                        self.keyval[nkey2] = str(val).replace('+',' ')
                            else:
                                if key == 'data' and  isinstance(jsonData,dict) :
                                    for key,val in jsonData.items():
                                        nkey2 = 'data_'+key
                                        self.keyval[nkey2] = str(val).replace('+',' ')
                    except Exception as e:
                        print("\n\nerror  in request  parsing 2",e)
                        print('logdict: ', logdict)
                        print('logdict.request: ', logdict.get('request',''))
                        print('self.keyval: ', self.keyval,'\n\n')

                self.keyval_final['ra'] = str(self.keyval.get('remote_addr', ''))  # remote_addr
                self.keyval_final['ua'] = str(self.keyval.get('http_user_agent', ''))  # http_user_agent
                self.keyval_final['t'] = self.keyval.get('time_local', '')  # time_local
                self.keyval_final['s'] = str(self.keyval.get('data_search', ''))  # data_search
                self.keyval_final['a'] = str(self.keyval.get('data_area', ''))  # data_area
                try:
                    self.keyval_final['p']  = self.keyval['data_pos']  # data_pos (app)
                except Exception as e:
                    pass
                try:
                    self.keyval_final['p']  = self.keyval['data_apos']  # data_pos (web)
                except Exception as e:
                    pass
                try:
                    self.keyval_final['p']  = self.keyval['data_position']  # data_pos (touch)
                except Exception as e:
                    pass
                try:
                    self.keyval_final['p'] = int(self.keyval_final['p'].replace(' ',''))
                except Exception as e:
                    self.keyval_final['p'] = -1
                self.keyval_final['tx'] = str(self.keyval.get('data_atext', ''))  # data_atext
                self.keyval_final['id'] = str(self.keyval.get('data_id', ''))  # data_id
                self.keyval_final['ty'] = str(self.keyval.get('data_type', ''))  # data_type
                self.keyval_final['ci'] = str(self.keyval.get('data_city', ''))  # data_city
                self.keyval_final['ft'] = str(self.keyval.get('data_freetext', ''))  # data_freetext
                self.keyval_final['ud'] = str(self.keyval.get('data_userid', ''))  # data_userid
                # self.keyval_final['src'] = self.keyval['src']  # src
                self.keyval_final['ds'] = str(self.keyval.get('data_selectedcity', ''))  # data_selectedcity
                try:
                    self.keyval_final['dw'] = int(self.keyval['data_web'])  # data_web
                except Exception as e:
                    pass
                self.keyval_final['dn'] = str(self.keyval.get('data_name', ''))  # data_name
                self.keyval_final['de'] = str(self.keyval.get('data_email', ''))  # data_email
                self.keyval_final['dm'] = str(self.keyval.get('data_mobile', ''))  # data_mobile
                self.keyval_final['da'] = str(self.keyval.get('data_selectedarea', ''))  # data_selectedarea
                self.keyval_final['du'] = str(self.keyval.get('data_udid', ''))  # data_udid
                self.keyval_final['sid'] = str(self.keyval.get('data_sid', ''))  # data_sid
                self.keyval_final['v'] = str(self.keyval.get('data_version', ''))  # data_version
                try:
                    self.keyval_final['v'] = Decimal128(format(Decimal(self.keyval_final['v']), '.5f'))  # data_version
                except Exception as e:
                    try:
                        if isinstance(self.keyval_final['v'], str):
                            self.keyval_final['v'] = self.keyval_final['v'].split(' ')
                            self.keyval_final['v'] = self.keyval_final['v'][0]
                            self.keyval_final['v'] = Decimal128(format(Decimal(self.keyval_final['v']), '.5f'))  # data_version
                    except Exception as e:
                        pass

                self.keyval_final['dr'] = str(self.keyval.get('data_recent', ''))  # data_recent
                self.keyval_final['dc'] = str(self.keyval.get('data_case', ''))  # data_case
                self.keyval_final['dv'] = str(self.keyval.get('data_value', ''))  # data_value
                self.keyval_final['dev'] = str(self.keyval.get('data_deviceid', ''))  # device_id
                try:
                    self.keyval_final['wap'] = int(self.keyval['data_wap'])  # rs_wap
                except Exception as e:
                    pass
                self.keyval_final['sr'] = str(self.keyval.get('data_searchreferrer', ''))  # search_referrer
                self.keyval_final['rt'] = str(self.keyval.get('data_type', ''))  # rs_type

                # self.keyval_final['request'] = self.keyval.get('request', '')  # request


        # try:
        #     if self.src == 7:
        #         self.keyval_final['src'] = self.src
        #     else:
        #         try:
        #             self.keyval_final['src'] = int(self.keyval['request']['source'])
        #         except:
        #             self.keyval_final['src'] = self.src
        # except Exception as e:
        #     print(f'Error at src: {e}')
        

        ## Anusha Narayan changes
        ## changes done at 2021-06-07
        try:
            try:
                self.keyval_final['src'] = int(self.keyval['request']['source'])
            except:
                try:
                    self.keyval_final['src'] = int(self.keyval['request']['wap'])
                except:
                    self.keyval_final['src'] = self.src
        except Exception as e:
            print(f'Error at src: {e}')
        ## Anusha Narayan changes
        ## changes done at 2021-06-07



        keyval_final_keys_list = list(self.keyval_final.keys())
        for key in keyval_final_keys_list:
            if isinstance(self.keyval_final[key], str):
                if self.keyval_final[key].replace(' ','') == '':
                    del self.keyval_final[key]
                else:
                    self.keyval_final[key] = self.keyval_final[key].lower()
        keyval_final_keys_list = list(self.keyval_final.keys())
        if len(keyval_final_keys_list) == 1 and keyval_final_keys_list[0] == 'src':
            del self.keyval_final['src']
        else:
            # testing
            self.keyval_final['id_test'] = id_test
            self.keyval_final['index_test'] = index_test
            if logtype=='web_justdial':
                self.keyval_final['elastic_check_url'] = f'http://192.168.29.121:9200/{index_test}/_search?q=_id:{id_test}'
            else:
                self.keyval_final['elastic_check_url'] = f'http://192.168.41.27:9200/{index_test}/_search?q=_id:{id_test}'


        # print(f'self.keyval: {self.keyval}\n')
        # print(f'self.keyval_final: {self.keyval_final}\n')

        

        return self.keyval_final
        # return self.keyval


    def collection_create_1_of_every_month(self, table_yearmonth):
        # Mongo Connection
        # mongoObj = MONGODATABASE(connection='mongo_clicktracker_35_126')
        mongoObj = MONGODATABASE(connection='mongo_searches_13_65_testing')
        mongoCon = mongoObj.connections(databaseName='db_autosuggest')

        print('mongoCon: {}'.format(mongoCon))

        self.autolog_collName = 'tbl_autolog_{}'.format(self.table_yearmonth)
        self.autosuggest_ft_cnts_collName = 'tbl_autosuggest_ft_cnts_{}'.format(self.table_yearmonth[:4])
        self.autolog_cnts_collName = 'tbl_autolog_cnts_{}'.format(self.table_yearmonth[:4])
        print('autolog collName : ', self.autolog_collName)
        print('autosuggest_ft_cnts collName : ', self.autosuggest_ft_cnts_collName)
        print('autolog_cnts collName : ', self.autolog_cnts_collName)

        # self.collection_names_list = mongoCon.collection_names()
        # The collection_names is deprecated from 3.7 onwards and been replaced by list_collection_names()
        self.collection_names_list = mongoCon.list_collection_names()
        print('Collection Names List : ', self.collection_names_list)

        # autolog collection creation
        if self.autolog_collName not in self.collection_names_list:
            try:
                mongoCon.create_collection(self.autolog_collName)  # Force create!

                #  $jsonSchema expression type is prefered.  New since v3.6 (2017):
                # Validation for Autolog Collection
                self.vexpr = {"$jsonSchema":
                  {
                         "bsonType": "object",
                         "required": [ "src" ],
                         "properties" : {
                					"ra" : {
                						"bsonType" : "string",
                						"description" : "remote_addr - must be a string"
                					},
                					"ua" : {
                						"bsonType" : "string",
                						"description" : "http_user_agent - must be a string and is required"
                					},
                					"t" : {
                						"bsonType" : [
                							"date",
                							"null"
                						],
                						"description" : "time_local - must be a timestamp"
                					},
                					"s" : {
                						"bsonType" : "string",
                						"description" : "data_search - must be a string and is required"
                					},
                					"a" : {
                						"bsonType" : "string",
                						"description" : "data_area - must be a string"
                					},
                					"p" : {
                						"bsonType" : "int",
                						"description" : "data_pos - must be int"
                					},
                					"tx" : {
                						"bsonType" : "string",
                						"description" : "data_atext - must be a string"
                					},
                					"id" : {
                						"bsonType" : "string",
                						"description" : "data_id - must be a string and is required"
                					},
                					"ty" : {
                						"bsonType" : "string",
                						"description" : "data_type - must be a string"
                					},
                					"ci" : {
                						"bsonType" : "string",
                						"description" : "data_city - must be a string and is required"
                					},
                					"ft" : {
                						"bsonType" : "string",
                						"description" : "data_freetext - must be a string"
                					},
                					"ud" : {
                						"bsonType" : "string",
                						"description" : "data_userid - must be a string"
                					},
                					"src" : {
                						"bsonType" : "int",
                						"description" : "src - must be int"
                					},
                					"ds" : {
                						"bsonType" : "string",
                						"description" : "data_selectedcity - must be a string"
                					},
                					"dw" : {
                						"bsonType" : "int",
                						"description" : "data_web - must be int"
                					},
                					"dn" : {
                						"bsonType" : "string",
                						"description" : "data_name - must be a string"
                					},
                					"de" : {
                						"bsonType" : "string",
                						"description" : "data_email - must be a string"
                					},
                					"dm" : {
                						"bsonType" : "string",
                						"description" : "data_mobile - must be a string"
                					},
                					"da" : {
                						"bsonType" : "string",
                						"description" : "data_selectedate - must be a string"
                					},
                					"du" : {
                						"bsonType" : "string",
                						"description" : "data_udid - must be a string"
                					},
                					"sid" : {
                						"bsonType" : "string",
                						"description" : "data_sid - must be a string"
                					},
                					"v" : {
                						"bsonType" : "decimal",
                						"description" : "data_version - must be a decimal"
                					},
                					"dr" : {
                						"bsonType" : "string",
                						"description" : "data_recent - must be a string"
                					},
                					"dc" : {
                						"bsonType" : "string",
                						"description" : "data_case - must be a string"
                					},
                					"dv" : {
                						"bsonType" : "string",
                						"description" : "data_value - must be a string"
                					},
                					"dev" : {
                						"bsonType" : "string",
                						"description" : "device_id - must be a string"
                					},
                					"wap" : {
                						"bsonType" : "int",
                						"description" : "rs_wap - must be a int"
                					},
                					"sr" : {
                						"bsonType" : "string",
                						"description" : "search_referrer - must be a string"
                					},
                					"rt" : {
                						"bsonType" : "string",
                						"description" : "rs_type - must be a string"
                					}
                				}
                  }
                }

                self.cmd = OrderedDict([('collMod', self.autolog_collName),
                        ('validator', self.vexpr),
                        ('validationLevel', 'moderate')])

                try:
                    mongoCon.command(self.cmd)
                except Exception as e:
                    print(f'Collection Structure Creation {self.autolog_collName}: {e}')

                mongoConCollection = self.autolog_collName
                mongoConCollectionObj = mongoCon[mongoConCollection]

                mongoConCollectionObj.create_index([('src', pymongo.ASCENDING)])
                mongoConCollectionObj.create_index([('t', pymongo.ASCENDING), ('src', pymongo.ASCENDING)])
                mongoConCollectionObj.create_index([('t', pymongo.ASCENDING), ('ci', pymongo.ASCENDING)])
                mongoConCollectionObj.create_index([('t', pymongo.ASCENDING), ('id', pymongo.ASCENDING)])
                mongoConCollectionObj.create_index([('t', pymongo.ASCENDING), ('ty', pymongo.ASCENDING)])
                mongoConCollectionObj.create_index([('t', pymongo.ASCENDING), ('dc', pymongo.ASCENDING)])
                mongoConCollectionObj.create_index([('t', pymongo.ASCENDING), ('dr', pymongo.ASCENDING)])
                mongoConCollectionObj.create_index([('t', pymongo.ASCENDING), ('wap', pymongo.ASCENDING)])
                mongoConCollectionObj.create_index([('t', pymongo.ASCENDING), ('rt', pymongo.ASCENDING)])
                mongoConCollectionObj.create_index([('t', pymongo.ASCENDING), ('p', pymongo.ASCENDING)])

                print('Collection {} Created with Validation Successfully.'.format(self.autolog_collName))
            except Exception as e:
                print('Error ==> ', e)

        # autosuggest_ft_cnts collection creation
        if self.autosuggest_ft_cnts_collName not in self.collection_names_list:
            try:
                mongoCon.create_collection(self.autosuggest_ft_cnts_collName)  # Force create!

                #  $jsonSchema expression type is prefered.  New since v3.6 (2017):
                # Validation for Autolog Collection
                self.vexpr = {"$jsonSchema":
                    {
                        "bsonType" : "object",
                            "required" : [
                                "d"
                            ],
                        "properties" : {
                            "d" : {
                						"bsonType" : [
                							"date",
                							"null"
                						],
                						"description" : "date - must be a timestamp"
                			},
                            "src" : {
                            	"bsonType" : "int",
                            	"description" : "src - must be a int"
                            },
                            "a" : {
                            	"bsonType" : "int",
                            	"description" : "autosuggest_cnt - must be a int"
                            },
                            "ft" : {
                            	"bsonType" : "int",
                            	"description" : "freetext_cnt - must be a int"
                            }
                        }
                    }
                }

                self.cmd = OrderedDict([('collMod', self.autosuggest_ft_cnts_collName),
                        ('validator', self.vexpr),
                        ('validationLevel', 'moderate')])

                try:
                    mongoCon.command(self.cmd)
                except Exception as e:
                    print(f'Collection Structure Creation {self.autosuggest_ft_cnts_collName}: {e}')

                mongoConCollection = self.autosuggest_ft_cnts_collName
                mongoConCollectionObj = mongoCon[mongoConCollection]

                mongoConCollectionObj.create_index([('d', pymongo.ASCENDING),('src', pymongo.ASCENDING)],unique=True)

                print('Collection {} Created with Validation Successfully.'.format(self.autosuggest_ft_cnts_collName))
            except Exception as e:
                print('Error ==> ', e)

        # autolog_cnts collection creation
        if self.autolog_cnts_collName not in self.collection_names_list:
            try:
                mongoCon.create_collection(self.autolog_cnts_collName)  # Force create!

                #  $jsonSchema expression type is prefered.  New since v3.6 (2017):
                # Validation for Autolog Collection
                self.vexpr = {"$jsonSchema":
                    {
                        "bsonType" : "object",
                            "required" : [
                                "d"
                            ],
                        "properties" : {
                            "d" : {
                						"bsonType" : [
                							"date",
                							"null"
                						],
                						"description" : "date - must be a timestamp"
                			},
                            "p" : {
                            	"bsonType" : "int",
                            	"description" : "position - must be a int"
                            },
                            "src" : {
                            	"bsonType" : "int",
                            	"description" : "src - must be a int"
                            },
                            "ty" : {
                            	"bsonType" : "string",
                            	"description" : "data_type - must be a string"
                            },
                            "dc" : {
                            	"bsonType" : "string",
                            	"description" : "data_case - must be a string"
                            },
                            "total_cnt" : {
                            	"bsonType" : "int",
                            	"description" : "total_cnt - must be a int"
                            }
                        }
                    }
                }

                self.cmd = OrderedDict([('collMod', self.autolog_cnts_collName),
                        ('validator', self.vexpr),
                        ('validationLevel', 'moderate')])

                try:
                    mongoCon.command(self.cmd)
                except Exception as e:
                    print(f'Collection Structure Creation {self.autolog_cnts_collName}: {e}')

                mongoConCollection = self.autolog_cnts_collName
                mongoConCollectionObj = mongoCon[mongoConCollection]


                mongoConCollectionObj.create_index([('d', pymongo.ASCENDING),('p', pymongo.ASCENDING),('dr', pymongo.ASCENDING),('dc', pymongo.ASCENDING),('ty', pymongo.ASCENDING),('dv', pymongo.ASCENDING),('src', pymongo.ASCENDING)],unique=True)
                mongoConCollectionObj.create_index([('d', pymongo.ASCENDING),('p', pymongo.ASCENDING),('dr', pymongo.ASCENDING),('ty', pymongo.ASCENDING),('dv', pymongo.ASCENDING),('src', pymongo.ASCENDING)])
                mongoConCollectionObj.create_index([('d', pymongo.ASCENDING),('p', pymongo.ASCENDING),('dr', pymongo.ASCENDING),('dc', pymongo.ASCENDING),('dv', pymongo.ASCENDING),('src', pymongo.ASCENDING)])
                mongoConCollectionObj.create_index([('d', pymongo.ASCENDING),('p', pymongo.ASCENDING),('dr', pymongo.ASCENDING),('dv', pymongo.ASCENDING),('src', pymongo.ASCENDING)])
                mongoConCollectionObj.create_index([('dc', pymongo.ASCENDING)])
                mongoConCollectionObj.create_index([('ty', pymongo.ASCENDING)])

                print('Collection {} Created with Validation Successfully.'.format(self.autolog_cnts_collName))
            except Exception as e:
                print('Error ==> ', e)


    def countsPrepare(self):
        # Mongo Connection
        # mongoObj = MONGODATABASE(connection='mongo_clicktracker_35_126')
        mongoObj = MONGODATABASE(connection='mongo_searches_13_65_testing')
        mongoCon = mongoObj.connections(databaseName='db_autosuggest')

        mongoConCollectionRead = 'tbl_autolog_{}'.format(self.table_yearmonth)
        mongoConCollectionObjRead = mongoCon[mongoConCollectionRead]
        # print('mongoConCollectionObjRead: {}'.format(mongoConCollectionObjRead))

        mongoConCollectionWrite1 = 'tbl_autosuggest_ft_cnts_{}'.format(self.table_yearmonth[:4])
        mongoConCollectionObjWrite1 = mongoCon[mongoConCollectionWrite1]
        # print('mongoConCollectionObjWrite1: {}'.format(mongoConCollectionObjWrite1))

        mongoConCollectionWrite2 = 'tbl_autolog_cnts_{}'.format(self.table_yearmonth[:4])
        mongoConCollectionObjWrite2 = mongoCon[mongoConCollectionWrite2]
        # print('mongoConCollectionObjWrite2: {}'.format(mongoConCollectionObjWrite2))

        self.stdate_cnt = datetime.strptime(self.stdate, '%Y-%m-%dT%H:%M:%S.000Z')
        self.enddate_cnt = datetime.strptime(self.enddate, '%Y-%m-%dT%H:%M:%S.999Z')
 
        # # testing
        # # "t": { $gte:ISODate("2021-05-01T18:30:00.000Z"), $lt:ISODate("2021-05-02T18:29:59.999Z") },
        # self.stdate_cnt = datetime.strptime("2021-05-01T18:30:00.000Z", '%Y-%m-%dT%H:%M:%S.000Z')
        # self.enddate_cnt = datetime.strptime("2021-05-02T18:29:59.999Z", '%Y-%m-%dT%H:%M:%S.999Z')

        data_autosuggest = mongoConCollectionObjRead.aggregate(
            [{
                "$match":{
                    "t": {"$gte":self.stdate_cnt, "$lte":self.enddate_cnt},
                    "p": { "$ne" : -1 }
                }
            },
            {
                "$group" :
                {
                    "_id" : {
                        "date":
                        {
                            "$dateToString": 
                                { 
                                    "format": "%Y-%m-%d", "date": "$t" , "timezone" : "Asia/Calcutta"
                                }
                        },
                        "src" : "$src"
                    },
                    "autosuggest_cnt":{"$sum":1}
                }
            }]
        )

        autosugg_and_freetxt_cnt = {}
        for i in data_autosuggest:
            # print(json.dumps(i, indent=4, default=str))
            res = {}
            insert_date = ''
            src = ''
            a = ''
            try:
                # res['d'] = i['_id']['date']
                # res['d'] = datetime.strptime(res['d'], '%Y-%m-%d')

                insert_date = i['_id']['date']
            except Exception as e:
                pass
            try:
                res['src'] = i['_id']['src']

                src = str(i['_id']['src'])
            except Exception as e:
                pass
            try:
                res['a'] = i['autosuggest_cnt']

                a = i['autosuggest_cnt']
            except Exception as e:
                pass

            # print(insert_date, src, a)
            if insert_date not in autosugg_and_freetxt_cnt:
                autosugg_and_freetxt_cnt[insert_date] = {src:{'a':a}}
            else:
                autosugg_and_freetxt_cnt[insert_date][src] = {'a':a}

        # print("autosugg_and_freetxt_cnt: ", json.dumps(autosugg_and_freetxt_cnt, indent=4, default=str))



        data_freetext = mongoConCollectionObjRead.aggregate(
            [{
                "$match":{
                    "t": {"$gte":self.stdate_cnt, "$lte":self.enddate_cnt},
                    "p": { "$eq" : -1 }
                }
            },
            {
                "$group" :
                {
                    "_id" : {
                        "date":
                        {
                            "$dateToString": 
                                { 
                                    "format": "%Y-%m-%d", "date": "$t" , "timezone" : "Asia/Calcutta"
                                }
                        },
                        "src" : "$src"
                    },
                    "freetext_cnt":{"$sum":1}
                }
            }]
        )

        for i in data_freetext:
            # print(json.dumps(i, indent=4, default=str))
            res = {}
            insert_date = ''
            src = ''
            ft = ''
            try:
                # res['d'] = i['_id']['date']
                # res['d'] = datetime.strptime(res['d'], '%Y-%m-%d')

                insert_date = i['_id']['date']
            except Exception as e:
                pass
            try:
                res['src'] = i['_id']['src']

                src = str(i['_id']['src'])
            except Exception as e:
                pass
            try:
                res['ft'] = i['freetext_cnt']

                ft = i['freetext_cnt']
            except Exception as e:
                pass

            # print(insert_date, src, a)
            if insert_date not in autosugg_and_freetxt_cnt:
                autosugg_and_freetxt_cnt[insert_date] = {src:{'ft':ft}}
            else:
                if src not in autosugg_and_freetxt_cnt[insert_date]:
                    autosugg_and_freetxt_cnt[insert_date][src] = {'ft':ft}
                else:
                    autosugg_and_freetxt_cnt[insert_date][src]['ft'] = ft


        # print("autosugg_and_freetxt_cnt: ", json.dumps(autosugg_and_freetxt_cnt, indent=4, default=str))


        insert_arr = []
        for i in autosugg_and_freetxt_cnt:
            insert_date = datetime.strptime(i, '%Y-%m-%d')
            insert_date = insert_date - timedelta(hours=5,minutes=30,seconds=00)
            for j in autosugg_and_freetxt_cnt[i]:
                insert_dict = {}
                insert_dict['d'] = insert_date
                insert_dict['src'] = int(j)
                try:
                    insert_dict['a'] = autosugg_and_freetxt_cnt[i][j]['a']
                except:
                    insert_dict['a'] = 0
                try:
                    insert_dict['ft'] = autosugg_and_freetxt_cnt[i][j]['ft']
                except:
                    insert_dict['ft'] = 0

                insert_arr.append(insert_dict)
        # print("insert_arr: ", json.dumps(insert_arr, indent=4, default=str))



        dataIns = QRYCONTEXT.insert_many(mongoConCollectionObjWrite1, insert_arr)
        if dataIns.get('e', 1) == 1:
            print("error in subsequent insert many:", dataIns)
        insert_arr.clear()



        data_cnts = mongoConCollectionObjRead.aggregate(
            [{
                "$match":{
                    "t": {"$gte":self.stdate_cnt, "$lte":self.enddate_cnt},
                }
            },
            {
                "$group" :
                {
                    "_id" : {
                        "date":
                        {
                            "$dateToString": 
                                { 
                                    "format": "%Y-%m-%d", "date": "$t" , "timezone" : "Asia/Calcutta"
                                }
                        },
                        "p" : "$p",
                        "src" : "$src",
                        "ty" :"$ty",
                        "dc" : "$dc",

                        "dr" :"$dr",
                        "dv" : "$dv"
                    },
                    "total_cnt":{"$sum":1}
                }
            }]
        )

        data_cnts_ref = []
        for i in data_cnts:
            res = {}
            try:
                res['d'] = i['_id']['date']
                res['d'] = datetime.strptime(res['d'], '%Y-%m-%d')
                res['d'] = res['d'] - timedelta(hours=5,minutes=30,seconds=00)
            except Exception as e:
                pass
            try:
                res['p'] = i['_id']['p']
            except Exception as e:
                pass
            try:
                res['src'] = i['_id']['src']
            except Exception as e:
                pass
            try:
                res['ty'] = i['_id']['ty']
            except Exception as e:
                pass
            try:
                res['dc'] = i['_id']['dc']
            except Exception as e:
                pass
            
            try:
                res['dr'] = i['_id']['dr']
            except Exception as e:
                pass
            try:
                res['dv'] = i['_id']['dv']
            except Exception as e:
                pass

            try:
                res['total_cnt'] = i['total_cnt']
            except Exception as e:
                pass

            data_cnts_ref.append(res)

        # print(f'data_cnts_ref: {data_cnts_ref}\n')
        dataIns = QRYCONTEXT.insert_many(mongoConCollectionObjWrite2, data_cnts_ref)
        if dataIns.get('e', 1) == 1:
            print("error in subsequent insert many:", dataIns)
        data_cnts_ref.clear()


