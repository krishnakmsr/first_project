import mysql.connector
from mysql.connector import Error

def getLaptopDetail(product_id):
    try:
        mySQLConnection = mysql.connector.connect(host='localhost',
                                                  database='test',
                                                  user='sandeepj',
                                                  password='sandeepj!@#')
                                                    

        cursor = mySQLConnection.cursor(buffered=True)
        sql_select_query = """SELECT * FROM tbl_pmaster WHERE product_id = '%s'"""%product_id
        print sql_select_query
        cursor.execute(sql_select_query)
        record = cursor.fetchall()
        
        for row in record:
            print "name:",row[0]
            
            

    except mysql.connector.Error as error:
        print("Failed to get record from MySQL table: {}".format(error))

    finally:
        if (mySQLConnection.is_connected()):
            cursor.close()
            mySQLConnection.close()
            print("MySQL connection is closed")

product_id = 10003
getLaptopDetail(product_id)

