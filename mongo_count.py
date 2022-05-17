


def mongo_count(collectionobj,insert_data):
     try:
        data1=collectionobj.update(insert_data,{ $inc:{cnt:1}},{ upsert: true })
        return QRYCONTEXT.ack(d=data1)
     except Exception as e:
            return QRYCONTEXT.ack(e=1,m='qry failed for qry:'+str(insert_data)+"error:"+str(e))







db.krishna.updateOne({"name":"krishna","work":"db"},
{
   $inc:{cnts:1},
   //$setOnInsert: { cnt: 1 }
},
  { upsert: true })
  
  


def update_one(collectionObj,filter,updateStr):
        modCount=0
        try:
            res=collectionObj.update_one(filter,updateStr)
            modCount=res.modified_count
            return QRYCONTEXT.ack(d=modCount)
        except Exception as e:
            modCount=-1
            return QRYCONTEXT.ack(e=1,m='qry failed for qry:'+str(filter)+'uptStr:'+str(updateStr)+"error:"+str(e),d=modCount)


if  data :
                    insert_raw_res = qryContext.QRYCONTEXT.insert_one(raw_click_collection_object,data)
                    if insert_raw_res.get('e',1) == 1:
                        print("erorr:",insert_raw_res)
                        return qryContext.QRYCONTEXT.ack(e=1,m="raw data insert error")  
    


 db.krishna.updateOne({"name":"krishna1234","work":"db"},
{
   $inc:{cnt:1}
   //$setOnInsert: { cnt: 1 }
},
  { upsert: true })
  
  
  
  
  db.krishna.find({})





 db.krishna.updateOne({"name":"krishna1234","work":"db","age":22},
{
   $inc:{cnt:1}
   //$setOnInsert: { cnt: 1 }
},
  { upsert: true })
  
  
  
  
  db.krishna.find({})











#####################

import calendar
from datetime import datetime
my_string='2022-01-03'


my_date = datetime.strptime(my_string, "%Y-%m-%d")

print(my_date)
#print('Type: ',type(my_date))
print('Day of Month:', my_date.day)

# to get name of day(in number) from date
print('Day of Week (number): ', my_date.weekday())

# to get name of day from date
print('Day of Week (name): ', calendar.day_name[my_date.weekday()])
############################################################################

UPDATE temp_clicktracker SET flag =10 WHERE link_identifier REGEXP  '%' OR  link_identifier  REGEXP '[^ -~]';
#
UPDATE temp_clicktracker SET flag =10 WHERE link_identifier LIKE "%\'%" OR link_identifier LIKE '%^%' OR link_identifier LIKE '%\"%' OR 
link_identifier LIKE '%GoogleCan u Just%' OR link_identifier LIKE '%“%'OR link_identifier LIKE '%Google,Can u Just%' ;
#
UPDATE temp_clicktracker SET link_identifier = REPLACE(link_identifier,'"','') WHERE flag<>10 AND link_identifier LIKE '%"%';


if(index_data['li'] 








# data1=collectionobj.updateOne(index_data1,increment_data,upsert_data)
#increment_data={ $inc:{cnt:1},$setOnInsert:extra_data1}
        #upsert_data={ upsert: true }
        #data1=collectionobj.updateOne(index_data1,{ $inc:{cnt:1},$setOnInsert:extra_data1},{ upsert: true })
###################################################################################################################
import calendar
from datetime import datetime
import re
#my_date = datetime.strptime(my_string, "%Y-%m-%d")
#calendar.day_name[my_date.weekday()])
index_data=OrderedDict()



index_data['li']=data['li']
index_data['ll']=data['ll']
index_data['dt']=data['r_dt']
index_data['vt']=data['vtl']
index_data['ct']=data['ct']
index_data['mob']=data['m']
index_data['src']=data['source']
index_data['di']=data['di']
index_data['udid']=data['udid']
index_data['nid']=data['nc']
index_data['av']=data['av']

extra_data=OrderedDict()
extra_data['ed']=calendar.day_name[data['r_dt'].weekday()])
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

#data1=collectionobj.updateOne(index_data1,{$inc:{cnt:1},$setOnInsert:})
if  data :
                    count_raw_res = qryContext.QRYCONTEXT.mongo_count(collectonobj,index_data,extra_data)
                    if coun_raw_res.get('e',1) == 1:
                        print("erorr:",insert_raw_res)
                        return qryContext.QRYCONTEXT.ack(e=1,m="count data insert error")

def mongo_count(collectionobj,index_data,extra_data):
     try:
        increment_data=OrderedDict()
        increment_data['$inc']={'cnt':1}
        increment_data['$setOnInsert']=extra_data
        data1=collectionobj.updateOne(index_data,increment_data,upsert=True)
        return QRYCONTEXT.ack(d=data1)
     except Exception as e:
            return QRYCONTEXT.ack(e=1,m='qry failed for qry:'+str(index_data)+"error:"+str(e))
