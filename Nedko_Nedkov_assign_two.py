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


def createTables(myCursor):
    print('Creating tables:')

    myCursor.execute("CREATE TABLE Customers(\
	customerID CHAR(36) NOT NULL,\
	first_name VARCHAR(20) NOT NULL,\
	last_name VARCHAR(20) NOT NULL,\
	personal_number CHAR(13) NOT NULL,\
	email VARCHAR(50) NOT NULL,\
	password CHAR(64) NOT NULL,\
	move_in_date DATE,\
	move_out_date DATE,\
	last_rent_paid DATE,\
	points SMALLINT UNSIGNED NOT NULL,\
	UNIQUE(personal_number),\
	PRIMARY KEY (customerID));")
    print('\t-Sucessfully created table [customers]')
    
    myCursor.execute("CREATE TABLE Employees(\
	employeeID CHAR(36) NOT NULL,\
	first_name VARCHAR(20) NOT NULL,\
	last_name VARCHAR(20) NOT NULL,\
	personal_number CHAR(13) NOT NULL,\
	phone_number VARCHAR(13) NOT NULL,\
	email VARCHAR(50) NOT NULL,\
	hire_date DATE NOT NULL,\
	release_date DATE,\
	salary SMALLINT UNSIGNED NOT NULL,\
	UNIQUE (personal_number),\
	PRIMARY KEY (employeeID));")
    print('\t-Successfully created table [employees]')

    myCursor.execute("CREATE TABLE Properties(\
	propertyID CHAR(36) NOT NULL,\
	address VARCHAR(200) NOT NULL,\
	rooms TINYINT NOT NULL,\
	size DOUBLE NOT NULL,\
	furnitured BOOL NOT NULL,\
	customerID CHAR(36),\
	rent SMALLINT UNSIGNED NOT NULL,\
	description VARCHAR(1000) NOT NULL,\
	employeeID CHAR(36) NOT NULL,\
	PRIMARY KEY(propertyID),\
	FOREIGN KEY (employeeID) REFERENCES Employees(employeeID));")
    print('\t-Successfully created table [properties]')

    myCursor.execute("CREATE TABLE Listings(\
	listingID CHAR(36) NOT NULL,\
	propertyID CHAR(36) NOT NULL,\
	publish_date DATE NOT NULL,\
	last_date DATE NOT NULL,\
	available_from DATE NOT NULL,\
	PRIMARY KEY(listingID),\
	FOREIGN KEY (propertyID) REFERENCES Properties(propertyID));")
    print('\t-Successfully created table [listings]')

    myCursor.execute("CREATE TABLE Listing_Applications(\
	listingID CHAR(36) NOT NULL,\
	customerID CHAR(36) NOT NULL,\
	PRIMARY KEY(ListingID, CustomerId),\
	FOREIGN KEY (listingID) REFERENCES Listings(listingID),\
	FOREIGN KEY (customerID) REFERENCES Customers(customerID));")
    print('\t-Successfully created table [listing_applications]')



cnx = mysql.connector.connect(user = 'root',
                              password = 'root',
                              host = 'localhost'
                              )

my_cursor = cnx.cursor()

db_name = 'ProgrammingAssignTwo'

if ensureCreated(db_name, my_cursor) is False:
    createDatabase(db_name, my_cursor) 
    createTables(my_cursor) 
