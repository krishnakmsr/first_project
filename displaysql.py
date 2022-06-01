import mysql.connector
import pymongo

# Open database connection
mydb = mysql.connector.connect(
  host="localhost",
  user="sandeepj",
  passwd="sandeepj!@#",
  database="test"
)


cursor = mydb.cursor()

var='12345'

cursor.execute("SELECT * FROM tbl_pmaster where product_id in (%s) "%(var))
print var
result = cursor.fetchall()
print result


#mydb.close()

