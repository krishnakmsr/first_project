mo=data['mob']
wd=['Monday','Tuesday','Wednesday','Thursday','Friday']

if(extra_data['ed'] in wd):
      da=1
else:
      da=2


qtr1=['Jan','Feb','Mar']
qtr2=['Apr','May','Jun']
qtr3=['Jul','Aug','Sep']
qtr4=['Oct','Nov','Dec']





monthly_index=OrderedDict()

monthly_index['li']=data['li']
monthly_index['ll']=data['ll']
monthly_index['e_mt']=str(monthName)
monthly_index['vt']=data['vt']
monthly_index['ct']=data['ct']
monthly_index['src']=data['src']
monthly_index['di']=data['di']
monthly_index['nid']=data['nc']
monthly_index['av']=data['av']

quarterly_index=OrderedDic()

quarterly_index['li']=data['li']
quarterly_index['ll']=data['ll']
quarterly_index['e_qtr']=0
quarterly_index['vt']=data['vt']
quarterly_index['ct']=data['ct']
quarterly_index['src']=data['src']
quarterly_index['di']=data['di']
quarterly_index['nid']=data['nc']
quarterly_index['av']=data['av']

if(monthName in qtr1):
       quarterly_index['e_qtr']=1
else if (monthName in qtr2):
       quarterly_index['e_qtr']=2
else if (monthName in qtr3):
       quarterly_index['e_qtr']=3
else:
    quarterly_index['e_qtr']=4


yearly_index=OrderedDic()

yearly_index['li']=data['li']
yearly_index['ll']=data['ll']
yearly_index['e_yr']=data['ey']
yearly_index['vt']=data['vt']
yearly_index['ct']=data['ct']
yearly_index['src']=data['src']
yearly_index['di']=data['di']
yearly_index['nid']=data['nc']
yearly_index['av']=data['av']

#############################################################

extra_data=OrderedDict()

if(da==1):
      if(mo!=''):
            extra_data['ins']={'wd_cnt'=1,'reg_m':1}
else:
      if(mo)
##########################






##cahnges
yearly_index['e_yr']=int(data['ey'])



if  data :
 	count_raw_res_monthly = qryContext.QRYCONTEXT.mongo_count_mon_quar_year(raw_click_collection_object1,monthly_index,da,mo)
        if coun_raw_res.get('e',1) == 1:
                     print("erorr:",insert_raw_res)
             	     return qryContext.QRYCONTEXT.ack(e=1,m="count monthly data insert error") 


if  data :
 	count_raw_res_quarterly = qryContext.QRYCONTEXT.mongo_count_mon_quar_year(raw_click_collection_object1,quarrterly_index,da,mo)
        if coun_raw_res_quarterly.get('e',1) == 1:
                     print("erorr:",count_raw_res_quarterly)
             	     return qryContext.QRYCONTEXT.ack(e=1,m="count quarterly data insert error") 


if  data :
 	count_raw_res_yearly = qryContext.QRYCONTEXT.mongo_count_mon_quar_year(raw_click_collection_object1,yearly_index,da,mo)
        if coun_raw_res_yearly.get('e',1) == 1:
                     print("erorr:",count_raw_res_yearly)
             	     return qryContext.QRYCONTEXT.ack(e=1,m="count yearly data insert error") 



def mongo_count_mon_quar_year(collectionobj,index_data,da,mo):
        try:
            increment_data2=OrderedDict()
            if(da==1):
                   if (mo !=''):
                          increment_data2['$inc']={'cnt':1,'wd_cnt':1,'reg_m':1}
                   else:
                         increment_data2['$inc']={'cnt':1,'wd_cnt':1}
            else:
                if (mo !=''):
                          increment_data2['$inc']={'cnt':1,'we_cnt':1,'reg_m':1}
                else:
                         increment_data2['$inc']={'cnt':1,'we_cnt':1}
            data3=collectionobj.update_one(index_data,increment_data2,upsert=True)
            return QRYCONTEXT.ack(d=data3)
        except Exception as e:
            return QRYCONTEXT.ack(e=1,m='qry failed for qry:'+str(index_data)+"error:"+str(e))






















##############################################################################



      
##function  




def mongo_count_monthly(collectionobj,monthly_index,da,mo):
        try:
            increment_data1=OrderedDict()
            if(da==1):
                   if (mo !=''):
                          increment_data1['$inc']={'cnt':1,'wd_cnt':1,'reg_m':1}
                   else:
                         increment_data1['$inc']={'cnt':1,'wd_cnt':1}
            else:
                if (mo !=''):
                          increment_data1['$inc']={'cnt':1,'we_cnt':1,'reg_m':1}
                else:
                         increment_data1['$inc']={'cnt':1,'we_cnt':1}
            data2=collectionobj.update_one(monthly_index,increment_data1,upsert=True)
            return QRYCONTEXT.ack(d=data2)
        except Exception as e:
            return QRYCONTEXT.ack(e=1,m='qry failed for qry:'+str(monthly_index)+"error:"+str(e))

######################################################









def mongo_count_quarterly(collectionobj,quarterly_index,da,mo):
        try:
            increment_data2=OrderedDict()
            if(da==1):
                   if (mo !=''):
                          increment_data2['$inc']={'cnt':1,'wd_cnt':1,'reg_m':1}
                   else:
                         increment_data2['$inc']={'cnt':1,'wd_cnt':1}
            else:
                if (mo !=''):
                          increment_data2['$inc']={'cnt':1,'we_cnt':1,'reg_m':1}
                else:
                         increment_data2['$inc']={'cnt':1,'we_cnt':1}
            data3=collectionobj.update_one(quarterly_index,increment_data2,upsert=True)
            return QRYCONTEXT.ack(d=data3)
        except Exception as e:
            return QRYCONTEXT.ack(e=1,m='qry failed for qry:'+str(quarterly_index)+"error:"+str(e))


###########################################################







def mongo_count_yearly(collectionobj,yearly_index,da,mo):
        try:
            increment_data3=OrderedDict()
            if(da==1):
                   if (mo !=''):
                          increment_data3['$inc']={'cnt':1,'wd_cnt':1,'reg_m':1}
                   else:
                         increment_data3['$inc']={'cnt':1,'wd_cnt':1}
            else:
                if (mo !=''):
                          increment_data3['$inc']={'cnt':1,'we_cnt':1,'reg_m':1}
                else:
                         increment_data3['$inc']={'cnt':1,'we_cnt':1}
            data4=collectionobj.update_one(yearly_index,increment_data3,upsert=True)
            return QRYCONTEXT.ack(d=data4)
        except Exception as e:
            return QRYCONTEXT.ack(e=1,m='qry failed for qry:'+str(yearly_index)+"error:"+str(e))



