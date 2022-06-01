import mysql.connector, json
from mysql.connector import Error

# Open database connection
mydb = mysql.connector.connect(
  host="localhost",
  user="sandeepj",
  passwd="sandeepj!@#",
  database="test"
)


col1 = ['product_id', 'product_name', 'catname']
col2 = ['spec_id', 'spec_display_value']

cursor = mydb.cursor()
cursor.execute("SELECT product_id,product_name,catname FROM tbl_pmaster")
result = cursor.fetchall()
for row in result:
	print row


"""
def upstream(result):
    	for i in result:
          	cursor.execute("SELECT spec_id,spec_display_value FROM tbl_spec_display where  product_id = {}".format(i[0]) )
            spec_result=cursor.fetchall()
          	data = dict((x, y) for x, y in zip(col1,i))
          	data['SPEC_INFO'] = dict((x, y) for x, y in zip(col2,spec_result[0])) 
          	print json.dumps(data)

		#try:

    	##print result
   
   
    	if result:
          	upstream(result) 
    	else:
          	print("No data")
    	cursor.close
		except mysql.connector.Error as err:
    	print("Error: {}".format(err))
"""
