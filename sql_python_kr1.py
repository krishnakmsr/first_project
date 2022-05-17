import mysql.connector as SQLC
import json
def createtable():
     DataBase=SQLC.connect(host="192.168.12.25",user="krishnak",password="krishnak@123",database="test")
     cursorObject=DataBase.cursor()
     TableName=" SELECT * FROM test.tbl_temp_202110 LIMIT 10;"
     cursorObject.execute(TableName)
     xms=cursorObject.fetchall()
     print(xms)
     return 

#createtable()
