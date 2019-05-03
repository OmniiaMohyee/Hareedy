import mysql.connector

cnx = mysql.connector.MySQLConnection(user='root', password='hydragang',
                                 host='localhost',
                                 database='db0')
# # Create a Cursor object to execute queries.
cur = cnx.cursor()
 
# # Select data from table using SQL query.
cur.execute('INSERT INTO Users VALUES("maryam","maryam@whatever.com","123456" )')
cur.execute("SELECT * FROM Users")

 
# # print the first and second columns      
for row in cur.fetchall() :
    print( row[0], " ", row[1])

cnx.commit()
cnx.close()
