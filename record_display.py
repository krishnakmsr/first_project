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
sql = "SELECT product_id FROM tbl_pmaster"
cursor.execute(sql)
results = cursor.fetchone()
print results[0]

a=results[0]
print "$$$$$$$$$$"
print a
cursor.execute(sql, a)

sql2 =  "SELECT spec_display_value FROM tbl_spec_display where product_id=%s"



print "inside the query "

cursor.execute(sql2)
results = cursor.fetchall()


for row in results:
    print(row)

mydb.close()
