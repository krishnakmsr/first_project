#version-1
class QRYCONTEXT():
    def __init__(mongoObj=None):
        if mongoObj :
            mongoObj=mongoObj
        val=1


    def esSearch(esObj, qry):
        data=esObj.search(
            scroll = '2m',
            # search_type = 'scan',
            size = 5000,
            # size = 1,
            # scroll_id = sid,
            body = qry
            )
        return QRYCONTEXT.ack(d=data)

    def esScroll(esObj, sid, scrollm = '2m'):
        data=esObj.scroll(scroll_id = sid, scroll = scrollm)
        return QRYCONTEXT.ack(d=data)


    def count(collectionObj,query):
        try:
            data=collectionObj.count(query)
            return QRYCONTEXT.ack(d=data)
        except Exception as e:
            return QRYCONTEXT.ack(e=1,m='qry failed for qry:'+str(query)+"error:"+str(e))

    def find_one(collectionObj,query,projection={'_id':0}):
        try:
            data=collectionObj.find_one(query,projection)
            if data is None :
                return QRYCONTEXT.ack(e=2,m='data not present')
            else:
                return QRYCONTEXT.ack(d=data)
        except Exception as e:
            return QRYCONTEXT.ack(e=1,m='qry failed for qry:'+str(query)+"error:"+str(e))

    def find(collectionObj,query,projection={'_id':0},limit=None):
        try:
            if limit is None :
                data=collectionObj.find(query,projection)
            else :
                data=collectionObj.find(query,projection).limit(limit)
            if data is None :
                return QRYCONTEXT.ack(e=2,m='data not present')
            else:
                return QRYCONTEXT.ack(d=list(data))
            return QRYCONTEXT.ack(d=data)
        except Exception as e:
            return QRYCONTEXT.ack(e=1,m='qry failed for qry:'+str(query)+"error:"+str(e))

    def update_one(collectionObj,filter,updateStr):
        modCount=0
        try:
            res=collectionObj.update_one(filter,updateStr)
            modCount=res.modified_count
            return QRYCONTEXT.ack(d=modCount)
        except Exception as e:
            modCount=-1
            return QRYCONTEXT.ack(e=1,m='qry failed for qry:'+str(filter)+'uptStr:'+str(updateStr)+"error:"+str(e),d=modCount)

    def update_many(collectionObj,filter,updateStr):
        modCount=0
        try:
            res=collectionObj.update_many(filter,updateStr)
            modCount=res.modified_count
            return QRYCONTEXT.ack(d=modCount)
        except Exception as e:
            modCount=-1
            return QRYCONTEXT.ack(e=1,m='qry failed for qry:'+str(filter)+'uptStr:'+str(updateStr)+"error:"+str(e),d=modCount)



    def insert_one(collectionObj,insertData):
        try:
            res=collectionObj.insert_one(insertData)
            return QRYCONTEXT.ack(d=res)
        except Exception as e:
            res=None
            return QRYCONTEXT.ack(e=1,m='insert_one qry failed for qry:'+str(insertData)+"error:"+str(e),d=res)

    def insert_many(collectionObj,insertData):
        try:
            res=collectionObj.insert_many(insertData,ordered = False)
            return QRYCONTEXT.ack(d=res)
        except Exception as e:
            return QRYCONTEXT.ack(e=1,m='insert_many qry failed for qry:'+str(insertData)+"error:"+str(e))



    def aggs(collectionObj,pipeline):
        try:
            res=collectionObj.aggregate(pipeline)
            return QRYCONTEXT.ack(d=list(res))
        except Exception as e:
            return QRYCONTEXT.ack(e=1,m='aggregate qry failed pipeline:'+str(pipeline)+"error:"+str(e))


    def mysql_select(mysqlObj,**kwargs):
        select=kwargs.get('select','*')
        tableName=kwargs.get('tableName',None)
        database=kwargs.get('database',None)
        where=kwargs.get('where','')
        groupby=kwargs.get('groupby','')
        having=kwargs.get('having','')
        orderby=kwargs.get('orderby','')
        limit=kwargs.get('limit','')


        sql='''SELECT %s from %s.%s %s %s %s %s %s ''' %(select,database,tableName,where,groupby,having,orderby,limit)

        print("sql",sql)
        try:
            cursor=mysqlObj.cursor()
            cursor.execute(sql)
            data=cursor.fetchall()
            cursor.close()

            return QRYCONTEXT.ack(d=data)
        except Exception as e:
            return QRYCONTEXT.ack(e=1,m='mysql_select failed qry:'+str(sql)+"error:"+str(e))



    def chunk_list(lst,n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]


    def ack(**kwargs):
        ret_dict={
        'e':kwargs.get('e',0),
        'm':kwargs.get('m','success'),
        'd':kwargs.get('d',None)
        }
        return ret_dict

    def customDict(data,rev=0):
        tempDict={}
        tempDictNL={}

        try:
            if rev==1:
                for elm  in data :
                    # print("elm",elm,elm[2])
                    if type(elm) is tuple:
                        tempDict[str(elm[1])] = elm[0]

            else:
                for elm  in data :
                    if type(elm) is tuple:
                        tempDict[str(elm[0])] = elm[1]
                        if  int(elm[2]) == 1:
                            tempDictNL[str(elm[0])] = elm[1]
            return QRYCONTEXT.ack(d={'a':tempDict,'n':tempDictNL})
        except Exception as e:
            return QRYCONTEXT.ack(e=1,m=str(e))


    def customDictionary(data,key,val):
        tempDict={}
        try:
            for elm  in data :
                tempDict[elm[key]] = elm[val]
        except Exception as  e:
            return QRYCONTEXT.ack(e=1,m='customDictionary:error:'+str(e))
        return  QRYCONTEXT.ack(d=tempDict)
