import mysql.connector


mydb = mysql.connector.connect(
  host="localhost",
  user="sandeepj",
  passwd="sandeepj!@#"
)

print(mydb)


mycursor = mydb.cursor()

mycursor.execute("CREATE DATABASE mydatabase")

