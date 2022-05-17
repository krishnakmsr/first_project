
from collections import OrderedDict
import calendar
from datetime import datetime
with open('kmsr_practise_mongo') as f:
    lines = f.readlines()


a=eval(str(lines))
b=a[0]
c=b.split('\t')
#print(c[1])
d=[]
e=[]


requests_log=c[0]
#print(requests_log)


#GET /webmain/ARWXRX.php?link_idf=listing&li_vtl=&lnk_loc=dtlpg&city=Sikar&mobile=&Ip=2401%3A4900%3A462a%3Ad6d2%3A4bf5%3Aa164%3A621b%3A8766%2C+23.58.93.181%2C+23.11.215.62%2C+192.168.8.253&source=2&docid=9999P1572.1572.190911211045.R1T6&time_stamp=2022-01-06+17%3A45%3A02&wap=2&jdlite=&sid=&udid=16414712903367786&ncatid= HTTP/1.1


krish=OrderedDict()
krish1=OrderedDict()
krish['ip']=requests_log
krish['request']=c[3]
krish['int1']=1
krish['e_yr']='2022'
krishn=krish.get('request',0)
print(dict(krish))

my_string='2022-01-26'

krish1['kmsr']='frog'

my_date = datetime.strptime(my_string, "%Y-%m-%d")
print(my_date)
print(my_date.month)
#print(datetime.my_date.strftime('%A'))
print(type(my_date))

kidate=str(my_date)
kispli=kidate.split(' ')
kmiu=datetime.strptime(kispli[0], "%Y-%m-%d")
print(type(kmiu))

#print('Type: ',type(my_date))
print('Day of Month:', my_date.day)

# to get name of day(in number) from date
print('Day of Week (number): ', my_date.weekday())

krish['date']=my_date
#k[0]=my_date
# to get name of day from date
k1= calendar.day_name[krish['date'].weekday()]
print(k1)



def krishna(index_data,extra_data):
        increment_data=OrderedDict()
        increment_data['$inc']={'cnt':1}
        increment_data['$setOnInsert']=extra_data
        kr='upsert=True'
        #print(index_data'+'extra_data'+'upsert=True)
        print("{}, {}, {}".format(dict(index_data),dict(increment_data),kr)) 
krishna(krish,krish1)


import pandas as pd
df = pd.Timestamp(my_string)
print( df.day_name())

from datetime import date
import calendar
curr_date = date.today()
print(curr_date)
print(calendar.day_name[curr_date.weekday()])

kt=int(krish['e_yr'])

print(type(kt))
             


print(my_date.month)
print(my_date.year)

sam=''
if sam!='':
    print(sam)

else:
    print('kmsr')




year='2022'
year1=int(year)
print(type(year1))


import datetime
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta
from collections import OrderedDict
import pymongo
#import db_test

s='2022-02-05'
my_date = datetime.strptime(s, "%Y-%m-%d")
d = (datetime.date(my_date) + relativedelta(day=31))
f=str(d)
k=datetime.strptime(f, "%Y-%m-%d")


t=(datetime.date(k) + relativedelta(day=31))


t1=k+timedelta(1)



monthName = t1.strftime("%b").lower()
yearName = t1.strftime("%Y")
month = t1.strftime("%Y%m")

print(month)


   
