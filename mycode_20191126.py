
import mysql.connector
import pymongo
import json



mydb = mysql.connector.connect(
  host="localhost",
  user="sandeepj",
  passwd="sandeepj!@#",
  database="test"
)

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb1 = myclient["mydatabase"]
mycol1 = mydb1["qqmaster"]

mycursor = mydb.cursor()
mycursor.execute("SELECT product_id,product_name,catid,catname,active_flag FROM tbl_pmaster")
data = mycursor.fetchall()



temp_data = {}
for i in data:
	temp_data[i] = i
	pids = temp_data.keys()
	print pids
	
	"""
	sql_select_query="SELECT * FROM tbl_spec_display where product_id = {}".format(i[0])
	mycursor.execute(sql_select_query)
	spec_data = mycursor.fetchall()
	print spec_data
	
	for j in spec_data
		if j['product_id'] in temp_data:
			if 'spec_info' not in temp_data:
				temp_data['spec_info'] = []
                temp_data['spec_info'].append(j)
		
	"""


#for j in new_dat:
#	if j['product_id'] in temp_data:
#		if 'spec_info' not in temp_data:
#			temp_data['spec_info'] = []
		
#		temp_data['spec_info'].append(j)
	
#temp_data['893893289'] = {'893893289' : [name, pro, new_set: [{sdi; ldlk lsld }, {sdfk, }], '893893289' : [name, pro new_set: [{sdi; ldlk lsld }, {sdfk, }], '893893289' : [name, pro new_set: [{sdi; ldlk lsld }, {sdfk, }]}



"""
data_json=[]

for row in data:
	data_json.append(row) 
print json.dumps(data_json)
#mycol1.insert_one(data_json)
	
"""	
	









