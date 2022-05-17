# import mysql.connector
# from mysql.connector import errorcode

import pymongo
from pymongo import MongoClient
from pymongo import ReadPreference,WriteConcern
from pymongo.collation import Collation
import socket

from elasticsearch import Elasticsearch

from pprint import pprint
import os,sys,time,random
import yaml
# pip3 install pyymal instead use pip3 install -U PyYAML
import base64
import urllib.parse
from urllib.parse import unquote
script_dir = os.path.dirname(__file__)

# rel_path = "/home/justdial/yogesh/local/scripts_35_126/config.yaml"
#rel_path = "/scripts/config.yaml"
rel_path = "/home/justdial/Desktop/clicktracker_final/config.yaml"


abs_file_path = os.path.join(script_dir, rel_path)

dbCrendentialsYaml = open(abs_file_path)

dbCrendentials = yaml.load(dbCrendentialsYaml, Loader=yaml.FullLoader)

class DATABASE:
    def credentials(self):

        # conName=self.connection
        conDict=dbCrendentials.get('databases',{}).get(self.dbType,{}).get(self.connection,{})
        host=conDict.get('host',None)
        if host is None:
            return({'errCode':1,'errMsg':'Database object not present'})

        connectionObj={
        'conDict':conDict,
        'host':host
        }
        return connectionObj

# class MYSQLDATABASE(DATABASE):
#     def __init__(self,connection):
#         self.connection=connection
#         self.dbType='mysql'

#     def connections(self):
#         con=self.credentials()
#         host=con.get('host',None)
#         try:
#             host=str(random.choice(host.split(',')))
#         except:
#             host=None

        conDict=con.get('conDict',{})
        username=conDict.get('username',None)
        password=conDict.get('password',None)
        use_pure=conDict.get('use_pure',True)
        port=conDict.get('port',3306)
        database=conDict.get('database',None)
        connectionData={
                'host':host,
                'username':username,
                'password':password,
                'port':port,
                'use_pure':use_pure,
                'database':database
                }

        # try:
        #     self.cnx=mysql.connector.connect(
        #             user=connectionData.get('username',None),
        #             password=connectionData.get('password',None),
        #             host=connectionData.get('host',None),
        #             port=connectionData.get('port',None),
        #             database=connectionData.get('database',None),
        #             use_pure=True)
        #     return self.cnx
        # except mysql.connector.Error as err:
        #     if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        #         return ({'errCode':1,'errMsg':'Something wrong with user crendentials'})
        #     elif err.errno == errorcode.ER_BAD_DB_ERROR:
        #         return({'errCode':1,'errMsg':'Database dose not exist'})
        #     else:
        #         return({'errCode':1,'errMsg':err})


    def disconnect(self):
        self.cnx.close()



class MONGODATABASE(DATABASE):
    def __init__(self,connection):
        self.connection=connection
        self.dbType='mongo'

    def connections(self,databaseName):
        con=self.credentials()
        host=con.get('host',None)
        conDict=con.get('conDict',{})
        username=conDict.get('username',None)
        password=conDict.get('password',None)
        database=conDict.get('database',None)
        read_preference=conDict.get('read_preference',None)

        connectionData={
                'host':host,
                'username':username,
                'password':password,
                'read_preference':read_preference
                }

        try:
            self.client = MongoClient(connectionData.get('host',None),
                    username=connectionData.get('username',None),
                    password=connectionData.get('password',None)
                    )
            self.db=self.client[databaseName]
            return self.db
        except Exception as e:
            print("error in Mongo Connection",e)

    def disconnect(self):
        self.client.close()


class ELASTICDATABASE(DATABASE):
    def __init__(self,connection):
        self.connection=connection
        self.dbType='elasticsearch'

    def connections(self):
        con=self.credentials()
        host=con.get('host',None)
        conDict=con.get('conDict',{})
        username=conDict.get('username',None)
        password=conDict.get('password',None)
        port=conDict.get('port',None)
        timeout=conDict.get('timeout',30)
        max_retries=conDict.get('max_retries',3)

        connectionData={
                'host':host,
                'username':username,
                'password':password,
                'port':port,
                'timeout':timeout,
                'max_retries':max_retries,

                }

        try:
            self.client = Elasticsearch([{'host':connectionData.get('host',None),
                                            'port':connectionData.get('port',None),
                                            'timeout':connectionData.get('timeout',None),
                                            'max_retries':connectionData.get('max_retries',None),
                                            }])

            return self.client
        except Exception as e:
            print("error in elastic Connection",e)

    def disconnectCon(self):
        self.client.transport.close()




class ENCRYPTION():
    def __init__(self,data):
        self.data=data

    def encodeString(self):
        message = self.data
        message_bytes = message.encode('ascii')
        base64_bytes = base64.b64encode(message_bytes)
        base64_message = base64_bytes.decode('ascii')
        return(urllib.parse.quote(base64_message))
    
    def decodeString(self):
        message = urllib.parse.unquote(self.data)
        base64_bytes = message.encode("ascii")
        string_bytes = base64.b64decode(base64_bytes)
        decode_string = string_bytes.decode("ascii")  
        
        return(decode_string)  

class APIS():
    def __init__(self,api):
        self.api=api

    def getApiHost(self):
        url = dbCrendentials.get('urls',{}).get(self.api,{}).get('url',None)
        return url
    
    
# class MAILS():
#     def __init__(self,mails):
#         self.mails = mails
        
#     def  getMailsCredentials(self):
#         toAddress       =  dbCrendentials.get('mail',{}).get('mails',{}).get('toAddress',None)  
#         senderAddress   =  dbCrendentials.get('mail',{}).get('mails',{}).get('senderAddress',None)  
#         password        =  dbCrendentials.get('mail',{}).get('mails',{}).get('password',None)  
#         smtp            =  dbCrendentials.get('mail',{}).get('mails',{}).get('smtp',None)  
        
#         return {'toAddress':toAddress,'senderAddress':senderAddress,'password':password,'smtp':smtp}