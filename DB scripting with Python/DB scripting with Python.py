#P.S. Ravi Chandan



#importing the libraries 
import pymysql
from passlib.hash import argon2
import csv

#Function to hash the password using argon2.hash
def argonHP(password):
    HashedPassword = argon2.hash(password)
    return HashedPassword

#Estabilishing a connection with the database
print('---ESTABLISHING A CONNECTION TO PHYMYADMIN-------')
print('**Note: Please change the value of port accordingly**')
connection = pymysql.connect(host = 'localhost', user = 'root', password = "", port=3307, autocommit=True)

#Reading csv data
data = csv.reader(open('data.csv'))

#try block - establishes connection to the database
try:
    with connection.cursor() as cursor:
        #dropping database if already exists with same name
        dropDB = ' DROP DATABASE IF EXISTS lab7'
        cursor.execute(dropDB)
        print("Existed database dropped with similar name if any.")
        
        #creating the database
        CreateDB = 'CREATE DATABASE IF NOT EXISTS lab7'
        cursor.execute(CreateDB)
        print("Database created successfully.")

        #selecting the database
        connection.select_db('lab7')
        print("Established connection to lab7 successfully!")

        #Creating the users table
        CreateTable = "CREATE TABLE IF NOT EXISTS users(ID TEXT, firstname TEXT, lastname TEXT, email TEXT, password TEXT)"
        cursor.execute(CreateTable)
        print("'users' Table created successfully!")

        print("inserting records into the table....")
        
        #Inserting data into the table from CSV
        for row in data:
            cursor.execute("INSERT INTO users(ID, firstname, lastname, email, password) VALUES(%s, %s, %s, %s, %s)", row)
        print("Records from CSV file are successfully inserted into 'users' table.")

        #Selecting passwords and ID from users table
        SelectQuery ="SELECT password, ID FROM users"
        cursor.execute(SelectQuery)
        rows = cursor.fetchall()

        #Hashing the passwords
        for row in rows:
            SQLUpdate = "UPDATE users SET password = %s WHERE ID = %s"
            cursor.execute(SQLUpdate, (argonHP(row[0]), row[1]))
            
        print("All passwords are HASHED successfully!")

#finally block - to close the connection
finally:
    connection.close()
