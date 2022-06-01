import mysql.connector
from mysql.connector import Error

def getLaptopDetail(product_id1):
    try:
        mySQLConnection = mysql.connector.connect(host='localhost',
                                                  database='test',
                                                  user='sandeepj',
                                                  password='sandeepj!@#')
                                                    

        cursor = mySQLConnection.cursor(buffered=True)
        sql_select_query = """SELECT product_id FROM tbl_pmaster WHERE product_id = '%s'"""%product_id1
        print sql_select_query
        cursor.execute(sql_select_query)
        record = cursor.fetchall()
        
        for row in record:
            print row[0]
            
            

    except mysql.connector.Error as error:
        print("Failed to get record from MySQL table: {}".format(error))

    finally:
        if (mySQLConnection.is_connected()):
            cursor.close()
            mySQLConnection.close()
            print("MySQL connection is closed")

product_id1 = '10001'
getLaptopDetail(product_id1)

