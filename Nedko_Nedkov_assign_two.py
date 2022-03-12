from datetime import datetime
import mysql.connector
import csv
import msvcrt as m


cnx = mysql.connector.connect(user = 'root',
                              password = 'root',
                              host = 'localhost'
                              )


my_cursor = cnx.cursor()

db_name = 'ProgrammingAssignTwo'


def ensureCreated(db_name, my_cursor):
    my_cursor.execute("SHOW DATABASES")  # Get all databases
    databaseExists = False 
    for db in my_cursor:  # See if db_name is in all databases
        if db[0] == db_name.lower():
            databaseExists = True
            break
    return databaseExists


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


def createViews(my_cursor):
    my_cursor.execute("\
   CREATE VIEW tenant_addresses AS \
        SELECT c.first_name, c.last_name, p.address \
          FROM properties AS p \
          JOIN customers AS c \
            ON p.customerID = c.customerID;")  # Get all tenants and their addresses

    my_cursor.execute("\
   CREATE VIEW employees_losing_tenants AS \
        SELECT e.first_name, e.last_name \
          FROM properties AS p \
          JOIN employees AS e \
            ON p.EmployeeID = e.EmployeeID \
          JOIN customers AS c \
            ON p.CustomerID = c.CustomerID \
         WHERE c.move_out_date IS NOT NULL;")  # Get all employees managing a property,
                                               # which a tenant is moving out of

    my_cursor.execute("\
     CREATE VIEW month_avg_points AS \
          SELECT EXTRACT(month FROM c.move_in_date), AVG(c.points) \
            FROM customers AS c \
           WHERE c.move_in_date IS NOT NULL \
        GROUP BY EXTRACT(month FROM c.move_in_date)")  # Get the average points for all tenants
                                                       # that have moved in during each month

    my_cursor.execute("\
     CREATE VIEW most_interested_customer AS \
          SELECT c.first_name, c.last_name, COUNT(listingID) AS applications \
            FROM listing_applications AS la \
            JOIN customers AS c \
              ON la.customerID = c.customerID \
        GROUP BY c.customerID \
        ORDER BY COUNT(listingID) DESC \
          LIMIT 1;")  # Get the customer that has applied for most properties

    my_cursor.execute("\
     CREATE VIEW most_wanted_property AS \
          SELECT p.Address, COUNT(la.customerID) AS applications\
            FROM listing_applications AS la \
            JOIN listings AS l \
              ON la.listingID = l.listingID \
            JOIN properties AS p \
              ON l.propertyID = p.propertyID \
        GROUP BY la.listingID \
        ORDER BY COUNT(la.customerID) DESC \
           LIMIT 1;")  # Get the property for which most people have applied

    my_cursor.execute("\
        CREATE PROCEDURE \
        get_employee_in_address(IN address VARCHAR(200), OUT first_name VARCHAR(20), OUT last_name VARCHAR(20)) \
        BEGIN  \
        SELECT e.first_name, e.last_name \
        INTO first_name, last_name \
        FROM properties AS p \
        JOIN employees AS e \
        ON p.employeeID = e.employeeID \
        WHERE p.address = address; \
        END")  # Get the employee managing a specific property by address

    my_cursor.execute("\
        CREATE PROCEDURE \
        get_employee_by_tenant(IN personal_number CHAR(13), OUT first_name VARCHAR(20), OUT last_name VARCHAR(20)) \
        BEGIN  \
        SELECT e.first_name, e.last_name \
          INTO first_name, last_name \
          FROM properties AS p \
          JOIN employees AS e \
            ON p.employeeID = e.employeeID \
          JOIN customers AS c \
            ON p.customerID = c.customerID \
         WHERE c.personal_number = personal_number; \
        END")  # Get the employee managing the property of a specific tenant by personal number
               # This query shows the usage of a procedure
    
    my_cursor.execute("\
     CREATE VIEW available_properties_avg_rent AS \
          SELECT AVG(rent) \
            FROM properties AS p \
           WHERE p.customerID IS NULL;")  # Get the average rent for all 
                                          # properties without a tenant

    my_cursor.execute("\
     CREATE VIEW average_points_by_employee AS \
          SELECT e.first_name, e.last_name, AVG(c.points) \
            FROM properties AS p \
            JOIN customers AS c \
              ON p.customerID = c.customerID \
            JOIN employees AS e \
              ON p.employeeID = e.employeeID \
        GROUP BY p.employeeID \
        ORDER BY AVG(c.points) DESC;")  # Get the average points of all 
                                        # customers managed by each employee

    my_cursor.execute("\
        CREATE VIEW average_salary_wanted_employees AS \
        SELECT AVG(e.salary) \
        FROM employees AS e \
        WHERE e.employeeID IN (\
            SELECT p.employeeID \
            FROM properties AS p \
            WHERE p.propertyID IN (\
                SELECT l.propertyID \
                FROM listings AS l \
                WHERE l.listingID IN (\
                    SELECT la.listingID \
                    FROM listing_applications AS la \
                    WHERE la.customerID = (\
                        SELECT c.customerID \
                        FROM listing_applications AS la \
                        JOIN customers AS c \
                        ON la.customerID = c.customerID \
                        GROUP BY c.customerID \
                        ORDER BY COUNT(la.listingID) DESC \
                        LIMIT 1))));")  # AVG salaries of the employees managing the properties,
                                        # for which the most interested customer has applied
                                        # Query is just to show the usage of:
                                        # Select, aggregate functions, where, subquery, join,
                                        # group by, order by and limit all in a view

def waitKeyPress():
    m.getch()  


def getTenantAddresses(my_cursor):
    my_cursor.execute("SELECT * FROM tenant_addresses;")
    
    myresult = my_cursor.fetchall()  
    print('\n\n================ Tenants ================')
    for x in myresult:  
        print(x[0], x[1] + ', ' + x[2])


    print('\nPrinted rows: ' + str(len(myresult)) + '\n=========================================\n\nPress any key to continue...\n')
    waitKeyPress()


def getEmployeesWithLeavingTenants(my_cursor):
    my_cursor.execute("SELECT * FROM employees_losing_tenants;")
    myresult = my_cursor.fetchall() 
    print('\n\n================ Tenant-losing employees ================')
    for x in myresult:
        print(x[0], x[1])

    print('\nPrinted rows: ' + str(len(myresult)) + '\n=========================================\n\nPress any key to continue...\n')
    waitKeyPress()


def getAveragePointsByMoveInMonth(my_cursor):
    my_cursor.execute("SELECT * FROM month_avg_points;")  
    myresult = my_cursor.fetchall() 
    print('\n\n================ Average points by move-in month ================')
    for x in myresult:
        print(x[0], '(', x[1], ')')

    print('\nPrinted rows: ' + str(len(myresult)) + '\n=========================================\n\nPress any key to continue...\n')
    waitKeyPress()


def getMostInterestedCustomer(my_cursor):
    my_cursor.execute("SELECT * FROM most_interested_customer;") 
    myresult = my_cursor.fetchall()  
    print('\n\n================ Most interested customer ================')
    for x in myresult:
        print(x[0], x[1], '(', x[2], ')')

    print('\nPrinted rows: ' + str(len(myresult)) + '\n=========================================\n\nPress any key to continue...\n')
    waitKeyPress()


def getMostWantedProperty(my_cursor):
    my_cursor.execute("SELECT * FROM most_wanted_property;")
    myresult = my_cursor.fetchall()
    print('\n\n================ Most wanted property ================')
    for x in myresult:
        print(x[0], '(', x[1], ')')

    print('\nPrinted rows: ' + str(len(myresult)) + '\n=========================================\n\nPress any key to continue...\n')
    waitKeyPress()


def getEmployeeByAddress(my_cursor):
    address = input("Select property address: ")
    myresult = my_cursor.callproc('get_employee_in_address', [address, 0, 0])

    print('\n\n================ Employee working at ' + address + ' ================')
    print(myresult[1], myresult[2])


    print('\nPrinted rows: 1\n=========================================\n\nPress any key to continue...\n')
    waitKeyPress()


def getEmployeeByTenant(my_cursor):
    tenant_personal_number = input('Select tenant personal number: ')
    myresult = my_cursor.callproc('get_employee_by_tenant', [tenant_personal_number, 0, 0])


    print('\n\n================ Employee supervising tenant ' + tenant_personal_number + ' ================')
    print(myresult[1], myresult[2])

    print('\nPrinted rows: 1\n=========================================\n\nPress any key to continue...\n')
    waitKeyPress()


def getAverageRentForAvailableProperties(my_cursor):
    my_cursor.execute("SELECT * FROM available_properties_avg_rent;")
    myresult = my_cursor.fetchall() 
    print('\n\n================ Available properties AVG rent ================')
    for x in myresult: 
        print(x[0])

    print('\nPrinted rows: ' + str(len(myresult)) + '\n=========================================\n\nPress any key to continue...\n')
    waitKeyPress()


def getAveragePointsByEmployee(my_cursor):
    my_cursor.execute("SELECT * FROM average_points_by_employee;")  
    myresult = my_cursor.fetchall()
    print('\n\n================ Average points by employee ================')
    for x in myresult:
        print(x[0], x[1], '(', x[2], ')')

    print('\nPrinted rows: ' + str(len(myresult)) + '\n=========================================\n\nPress any key to continue...\n')
    waitKeyPress()


def getAverageSalaryForWantedEmployees(my_cursor):
    my_cursor.execute("SELECT * FROM average_salary_wanted_employees;")  
    myresult = my_cursor.fetchall()
    print('\n\n================ Average salary for wanted employees ================')
    for x in myresult:
        print(x[0])
    print('\nPrinted rows: ' + str(len(myresult)) + '\n=========================================\n\nPress any key to continue...\n')
    waitKeyPress()


if ensureCreated(db_name, my_cursor) is False:
    createDatabase(db_name, my_cursor) 
    createTables(my_cursor) 
    populateTables(my_cursor)
    createViews(my_cursor)

cnx = mysql.connector.connect(user = 'root',
                              password = 'root',
                              host = 'localhost',
                              database = db_name)

while(True):
    print('============================== Options ==============================\n\
     1. Get the names and addresses of all customers with a property \n\
     2. Get the names of all employees, who are responsible for a room, which a tenant is leaving \n\
     3. Get the average amount of points per move-in month \n\
     4. Get the customer with most applications for a property \n\
     5. Get the property with most applications by customers \n\
     6. Get the name of the employee working in a specific property \n\
     7. Get the name of the employee responsible for rooms, in which a certain customer lives \n\
     8. Get the average rent of available properties \n\
     9. Get the average points of customers by responsible employee \n\
    10. Get the average salary of employees responsible for the properties the most interested customer wants \n\
    11. Exit')
    print('=====================================================================\n')

    option = int(input("Pick an option (1-11): "))
    my_cursor = cnx.cursor()    
    if option == 1:
        getTenantAddresses(my_cursor)

    if option == 2:
        getEmployeesWithLeavingTenants(my_cursor)

    if option == 3:
        getAveragePointsByMoveInMonth(my_cursor)

    if option == 4:
        getMostInterestedCustomer(my_cursor)

    if option == 5:
        getMostWantedProperty(my_cursor)

    if option == 6:
        getEmployeeByAddress(my_cursor)

    if option == 7:
        getEmployeeByTenant(my_cursor)

    if option == 8:
        getAverageRentForAvailableProperties(my_cursor)

    if option == 9:
        getAveragePointsByEmployee(my_cursor)

    if option == 10:
        getAverageSalaryForWantedEmployees(my_cursor)
    
    if option == 11:
        break
    
    my_cursor.close()
