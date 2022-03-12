import mysql.connector

def ensureCreated(db_name, my_cursor):
    my_cursor.execute("SHOW DATABASES")  # Get all databases
    database_exists = False 
    for db in my_cursor:  # See if dbName is in all databases
        if db[0] == dbName.lower():
            database_exists = True
            break
    return database_exists


def createDatabase(db_name, my_cursor):
    my_cursor.execute("CREATE DATABASE " + db_name)  # Creates the database
    print('Successfully created database [' + db_name + ']')
    my_cursor.execute("USE " + db_name)  # Starts using the database
    print('Using database [' + db_name + ']')



cnx = mysql.connector.connect(user = 'root',
                              password = 'root',
                              host = 'localhost'
                              )

myCursor = cnx.cursor()

dbName = 'ProgrammingAssignTwo'

if ensureCreated(dbName, myCursor) is False:
    createDatabase(dbName, myCursor) 
