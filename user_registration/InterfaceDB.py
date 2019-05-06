import mysql.connector
from mysql.connector import errorcode
class InterfaceDB:

    def  __init__(self,user,password,host,database):
        print("dbint successfully created!")
        self.user=user
        self.password=password
        self.host=host
        self.database=database
    def addUser(self,username, email, password):
        #try to add and send alerts if necessary 
        try:
            #establish connection
            cnx = mysql.connector.MySQLConnection(user=self.user, password=self.password,
                                    host=self.host,
                                    database=self.database)  
        except mysql.connector.Error as err:
            print("Something went wrong: {}".format(err))
            # cnx.close()
            return err.errno
        # Create a Cursor object to execute queries.
        cur = cnx.cursor()

        try:
            # insert data into table using SQL query.
            cur.execute('INSERT INTO Users VALUES(%s,%s,%s )',[username,email,password])
            cnx.commit()
            return 1
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_DUP_ENTRY:
                print("User/email already exists")
                return 2
            print("Something went wrong: {}".format(err))
            cnx.close()
            return err.errno

        cnx.close()

    
    def authUser(self,username,password):
    #return if user exists or if user password is wrong
        #try to add and send alerts if necessary 
        try:
            #establish connection
            cnx = mysql.connector.MySQLConnection(user=self.user, password=self.password,
                                    host=self.host,
                                    database=self.database) 
        except mysql.connector.Error as err:
            print("Something went wrong: {}".format(err))
            # cnx.close()
            return err.errno
        # Create a Cursor object to execute queries.
        cur = cnx.cursor()

        try:
            cur.execute('SELECT * FROM Users WHERE userName=%s',[username])
            result=cur.fetchall()
            if(len(result)==0):
                print("no such user")
                return 2 #code for no such user
            elif (result[0][2]!= password):
                print("wrong password")
                return 3 #code for wring password
            else:
                print("Successul login of"+result[0][1])
                # cur.execute('UPDATE Users SET loggedIn="1" WHERE userName=%s',[username])
                return 1
        except mysql.connector.Error as err:
            print("Something went wrong: {}".format(err))
            cnx.close()
            return err.errno

        cnx.close()
    
# error codes uptill now -- 1045 (cannot access db)
#                        -- 1062 (duplicate entry)




       