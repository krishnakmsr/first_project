import mysql.connector
from mysql.connector import Error

#def getLaptopDetail(product_id):
    
mySQLConnection = mysql.connector.connect(host='localhost',
                                                  database='test',
                                                  user='sandeepj',
                                                  password='sandeepj!@#')
                                                  
                                                  
cursor = mySQLConnection.cursor()
## sql_select_query = """SELECT * FROM tbl_pmaster WHERE product_id = '%s'"""%product_id
sql_select_query = "SELECT product_id FROM tbl_pmaster"
cursor.execute(sql_select_query)
record = cursor.fetchall()
for a in record:
			print 'i m here'
			sql_select_query1 = """SELECT product_id,product_name,catname FROM tbl_pmaster WHERE product_id = '%s'"""%a
			print sql_select_query1
			cursor.execute(sql_select_query1)
			record_master = cursor.fetchall()
			for b in record_master:
			 print ("pid:", b[0],"pname:",b[1],"caname:",b[2])
			 sql_select_query2 = """SELECT product_id,spec_id,spec_display_value FROM tbl_spec_display WHERE product_id = '%s'"""%a
		    #print sql_select_query2
			cursor.execute(sql_select_query2)
			record_master = cursor.fetchall()
			for row in record_master:
			 print ("p_id:", row[0],"spec_id:",row[1],"spec_display_value:",row[2])
			 print 'xxxxxxxxx'
					


