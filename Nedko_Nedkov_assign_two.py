import mysql.connector


cnx = mysql.connector.connect(user = 'root',
                              password = 'root',
                              host = 'localhost'
                              )

myCursor = cnx.cursor()

dbName = 'ProgrammingAssignTwo'
