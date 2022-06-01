import mysql.connector
from mysql.connector import Error

#def getLaptopDetail(product_id):
    
mySQLConnection = mysql.connector.connect(host='localhost',
                                                  database='test',
                                                  user='sandeepj',
                                                  password='sandeepj!@#')


cursor = mySQLConnection.cursor()
sql_select_query = "SELECT product_id FROM tbl_pmaster"
cursor.execute(sql_select_query)
record = cursor.fetchall()

for row in record
	print row


			#sql_select_query1 = """SELECT product_id,product_name,catname FROM tbl_pmaster WHERE product_id = '%s'"""%a



