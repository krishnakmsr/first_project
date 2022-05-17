import datetime
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta
from collections import OrderedDict



s='2022-12-25'
my_date = datetime.strptime(s, "%Y-%m-%d")
d = (datetime.date(my_date) + relativedelta(day=31))
print(d)
f=str(d)
print(f)
k=datetime.strptime(f, "%Y-%m-%d")  #compare with file datetime ,if equal start collection creation process
print(k)
t=(datetime.date(k) + relativedelta(day=31))
print(t)
t1=k+timedelta(1)      # next day datetime to be used to craete collections
print(t1)
monthName = t1.strftime("%b").lower()
yearName = t1.strftime("%Y")
month = t1.strftime("%Y%m")

vexpr_col='tbl_clicktracker_daily_'+str(month)

print(vexpr_col)

print(yearName)

vexpr_col2='tbl_clicktracker_'+str(monthName)+'_'+str(yearName)

print(vexpr_col2)





kri=datetime.now()
krish=kri.strftime("%Y-%m-%d")
print(krish)
krish1=datetime.strptime(krish,"%Y-%m-%d")
ti=(datetime.date(krish1) + relativedelta(day=31))
print(ti)

t2=krish1+timedelta(1)

krish5=datetime.strftime(t2,"%Y-%m-%d")
print(krish5)
