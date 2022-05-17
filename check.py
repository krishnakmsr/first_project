import datetime
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta
from collections import OrderedDict


kri=datetime.now()
krish=kri.strftime("%Y-%m-%d")
print(type(krish))
krish1=datetime.strptime(krish,"%Y-%m-%d")
ti=(datetime.date(krish1) + relativedelta(day=31))
print(type(ti))
x=datetime.date(kri)
print(x)

y=x+timedelta(1)
print(type(y))
