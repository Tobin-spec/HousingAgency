import csv
import datetime
import mysql.connector

def ensureCreated(db_name, my_cursor):
    my_cursor.execute("SHOW DATABASES")  # Get all databases
    database_exists = False 
    for db in my_cursor:  # See if db_name is in all databases
        if db[0] == db_name.lower():
            database_exists = True
            break
    return database_exists


def createDatabase(db_name, my_cursor):
    my_cursor.execute("CREATE DATABASE " + db_name)  # Creates the database
    print('Successfully created database [' + db_name + ']')
    my_cursor.execute("USE " + db_name)  # Starts using the database
    print('Using database [' + db_name + ']')


def createTables(my_cursor):
    print('Creating tables:')

    my_cursor.execute("CREATE TABLE Customers(\
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
    
    my_cursor.execute("CREATE TABLE Employees(\
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

    my_cursor.execute("CREATE TABLE Properties(\
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

    my_cursor.execute("CREATE TABLE Listings(\
	listingID CHAR(36) NOT NULL,\
	propertyID CHAR(36) NOT NULL,\
	publish_date DATE NOT NULL,\
	last_date DATE NOT NULL,\
	available_from DATE NOT NULL,\
	PRIMARY KEY(listingID),\
	FOREIGN KEY (propertyID) REFERENCES Properties(propertyID));")
    print('\t-Successfully created table [listings]')

    my_cursor.execute("CREATE TABLE Listing_Applications(\
	listingID CHAR(36) NOT NULL,\
	customerID CHAR(36) NOT NULL,\
	PRIMARY KEY(ListingID, CustomerId),\
	FOREIGN KEY (listingID) REFERENCES Listings(listingID),\
	FOREIGN KEY (customerID) REFERENCES Customers(customerID));")
    print('\t-Successfully created table [listing_applications]')


def populateTables(db_name):
    print('Populating tables:')
    with open('data/Customers.csv', mode ='r')as file:
        csvFile = csv.reader(file)  # Gets the records from a csv file
        next(csvFile)  # Skips the first line of the csv file (field names)
        sql = "INSERT INTO customers (customerID, first_name, last_name, personal_number, email, password, move_in_date, move_out_date, last_rent_paid, points) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"  # Prepare to insert records into table
        for lines in csvFile:  # Gets the values to insert
                customerID = lines[0]
                first_name = lines[1]
                last_name = lines[2]
                personal_number = lines[3]
                email = lines[4]
                password = lines[5]
                move_in_date = datetime.strptime(lines[6], '%m/%d/%Y')  if not lines[6] == 'NONE' else None  # --//--

                move_out_date = datetime.strptime(lines[6], '%m/%d/%Y') if not lines[7] == 'NONE' else None  # --//--
                last_rent_paid = datetime.strptime(lines[6], '%m/%d/%Y') if not lines[8] == 'NONE' else None  # --//--
                points = int(lines[9])

                values = (customerID, first_name, last_name, personal_number, email, password, move_in_date, move_out_date, last_rent_paid, points)  # Puts values into tuple
                my_cursor.execute(sql, values)  # Inserts values into table
        cnx.commit()  # Commits changes to database
        print("\t-Successfully populated table [customers]")

    with open('data/Employees.csv', mode ='r')as file:  # Same as previous
        csvFile = csv.reader(file)
        next(csvFile)
        sql = "INSERT INTO employees (EmployeeID, first_name, last_name, personal_number, phone_number, email, hire_date, release_date, salary) \
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        for lines in csvFile:
                employeeID = lines[0]
                first_name = lines[1] 
                last_name = lines[2]
                personal_number = lines[3]
                phone_number = lines[4]
                email = lines[5]
                hire_date = datetime.strptime(lines[6], '%m/%d/%Y')
                release_date = datetime.strptime(lines[7], '%m/%d/%Y') if not lines[7] == 'NONE' else None
                salary = int(lines[8])
                values = (employeeID, first_name, last_name, personal_number, phone_number, email, hire_date, release_date, salary)
                my_cursor.execute(sql, values)
        cnx.commit()
        print("\t-Successfully populated table [employees]")

    with open('data/Properties.csv', mode ='r')as file:  # Same as previous
        csvFile = csv.reader(file, quoting=csv.QUOTE_NONE)
        next(csvFile)
        sql = "INSERT INTO properties (propertyID, address, rooms, size, furnitured, customerID, rent, description, employeeID) \
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        for lines in csvFile:
                propertyID = lines[0].lstrip('"')
                address = lines[1]
                rooms = int(lines[2])
                size = float(lines[3])
                furnitured = bool(lines[4])
                customerID = lines[5]  if not lines[5] == 'NONE' else None
                rent = int(lines[6])
                description = lines[7].strip('"')
                employeeID = lines[8].rstrip('"')
                values = (propertyID, address, rooms, size, furnitured, customerID, rent, description, employeeID)
                my_cursor.execute(sql, values)
        cnx.commit()
        print("\t-Successfully populated table [properties]")

    with open('data/Listings.csv', mode ='r')as file:  # Same as previous
        csvFile = csv.reader(file)
        next(csvFile)
        sql = "INSERT INTO listings (listingID, propertyID, publish_date, last_date, available_from) \
        VALUES (%s, %s, %s, %s, %s)"
        for lines in csvFile:
                listingID = lines[0]
                propertyID = lines[1] 
                publish_date = datetime.strptime(lines[2], '%m/%d/%Y')
                last_date = datetime.strptime(lines[3], '%m/%d/%Y')
                available_from = datetime.strptime(lines[4], '%m/%d/%Y')
                values = (listingID, propertyID, publish_date, last_date, available_from)
                my_cursor.execute(sql, values)
        cnx.commit()
        print("\t-Successfully populated table [listings]")

    with open('data/ListingApplications.csv', mode ='r')as file:  # Same as previous
        csvFile = csv.reader(file)
        next(csvFile)
        sql = "INSERT INTO listing_applications (listingID, customerID) \
        VALUES (%s, %s)"
        for lines in csvFile:
                listingID = lines[0]
                customerID = lines[1] 
                values = (listingID, customerID)
                my_cursor.execute(sql, values)
        cnx.commit()
        print("\t-Successfully populated table [listing_applications]")

cnx = mysql.connector.connect(user = 'root',
                              password = 'root',
                              host = 'localhost'
                              )

my_cursor = cnx.cursor()

db_name = 'ProgrammingAssignTwo'

if ensureCreated(db_name, my_cursor) is False:
    createDatabase(db_name, my_cursor) 
    createTables(my_cursor) 
    populateTables(my_cursor)
