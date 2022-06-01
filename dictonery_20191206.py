import mysql.connector, json
from mysql.connector import Error
import pymongo


mydb = mysql.connector.connect(
  host="localhost",
  user="sandeepj",
  passwd="sandeepj!@#",
  database="test"
)

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb1 = myclient["mydatabase"]
mycol1 = mydb1["qqmaster"]


col2 = ['product_id', 'product_name', 'catname']
col1 = ['spec_id', 'spec_display_value']



def datajsonprepare(result):
    	for i in result:
			sql_result_query="SELECT product_id,spec_id,spec_display_value,active_flag FROM tbl_spec_display where  product_id = {}".format(i[0])
			cursor.execute(sql_result_query)
			spec_result = cursor.fetchall()
			for row in spec_result:
				data = dict((x, y) for x, y in zip(col1,i))
				data['SPEC_INFO'] = dict((x, y) for x, y in zip(col2,spec_result[0])) 
				
				
				
				
				print json.dumps(data)
				mycol1.insert(data)
				#print json.dumps(data2)
			
try:
			cursor = mydb.cursor()
			cursor.execute("SELECT product_id,product_name,catname FROM tbl_pmaster")
			result = cursor.fetchall()
			for row in result:
				print row
			if result:
				print "i am in if loop"
				datajsonprepare(result) 
			else:
				print "i am in else loop"
				print("No data")
			cursor.close
except mysql.connector.Error as err:
			print("Error: {}".format(err))
