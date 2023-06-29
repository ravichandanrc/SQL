#Ravi Chandan-
#Python Script and testing


#importing the required libraries
import mysql.connector
from mysql.connector import errorcode
import pandas as pd
import pyodbc

#using try, except, else and finally blocks to successfully make the connection and exit

#try block - establishes connection to the database
try:
    connect = mysql.connector.connect(
        user='root',
        password='',
        host='localhost',
        port=3307,
        database='assignment2')

#except block - prints the error message to user accordingly    
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print('Access Denied.')
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print('Database not found.')
    else:
        print('Cannot connect to database because of:',err)
        
#else block - establishes a connection and inserts data
else:
    #if connection is established successfully
    if connect.is_connected():

        #getting the cursor element for our connection
        new_cursor = connect.cursor()

        #establishing connection to our database
        new_cursor.execute("select database();")
        record = new_cursor.fetchone()
        print("Connection established to the database:",record)

        #reading the file
        data = pd.read_csv(r'F:\1AMOD\Intro. to Databases 5450\Assignment 2\data-large.csv', header=None, low_memory=False, index_col=False, dtype='unicode')
        print("File successfully read.")

        #converting the file to a pandas dataframe
        print("Converting to dataframe....")
        df = pd.DataFrame(data)

        #removing the first two rows as they are not required
        df = df[3:]

        #updating the header(column) names in the dataframe
        df.columns = ["time_in_sec","esp1_discharge_pressure_psia","esp1_intake_pressure_psia","esp1_intake_temperature_K","esp1_motor_temperature_K","esp1_vsdfreqout_Hz","esp1_vsdmotamps_A","esp2_discharge_pressure_psia","esp2_intake_pressure_psia","esp2_intake_temperature_K","esp2_motor_temperature_K","esp2_vsdfreqout_Hz","esp2_vsdmotamps_A","esp3_discharge_pressure_psia","esp3_intake_pressure_psia","esp3_intake_temperature_K","esp3_motor_temperature_K","esp3_vsdfreqout_Hz","esp3_vsdmotamps_A","ic1_choke_pressure_percent","ic1_pressure1_psia","ic1_pressure2_psia","ic1_temperature1_degF","ic1_temperature2_degF","ic1_water_cut_percent","ic1_liquid_rate_bbl_d","ic1_water_rate_bbl_d","ic1_oil_rate_bbl_d","ic2_choke_pressure_percent","ic2_pressure1_psia","ic2_pressure2_psia","ic2_temperature1_degF","ic2_temperature2_degF","ic2_water_cut_percent","ic2_liquid_rate_bbl_d","ic2_water_rate_bbl_d","ic2_oil_rate_bbl_d"]
        print("Data frame created successfully.")

        #checking and droppping if table already exists
        new_cursor.execute('drop table if exists a2')

        #creating the new table with the required columns
        new_cursor.execute('''
                            CREATE TABLE a2(
                                time_in_sec                     NUMERIC(15,0),
                                esp1_discharge_pressure_psia    NUMERIC(15,5),
                                esp1_intake_pressure_psia       NUMERIC(15,5),
                                esp1_intake_temperature_K    	NUMERIC(15,10),
                                esp1_motor_temperature_K     	NUMERIC(15,10),
                                esp1_vsdfreqout_Hz            	NUMERIC(15,10),
                                esp1_vsdmotamps_A            	NUMERIC(15,10),
                                esp2_discharge_pressure_psia    NUMERIC(15,5),
                                esp2_intake_pressure_psia       NUMERIC(15,5),
                                esp2_intake_temperature_K    	NUMERIC(15,10),
                                esp2_motor_temperature_K     	NUMERIC(15,10),
                                esp2_vsdfreqout_Hz            	NUMERIC(15,10),
                                esp2_vsdmotamps_A            	NUMERIC(15,10),
                                esp3_discharge_pressure_psia    NUMERIC(15,5),
                                esp3_intake_pressure_psia       NUMERIC(15,5),
                                esp3_intake_temperature_K    	NUMERIC(15,10),
                                esp3_motor_temperature_K     	NUMERIC(15,10),
                                esp3_vsdfreqout_Hz            	NUMERIC(15,10),
                                esp3_vsdmotamps_A            	NUMERIC(15,10),
                                ic1_choke_pressure_percent 	NUMERIC(4,2),
                                ic1_pressure1_psia         	NUMERIC(10,2),
                                ic1_pressure2_psia         	NUMERIC(10,2),
                                ic1_temperature1_degF      	NUMERIC(3,0),
                                ic1_temperature2_degF      	NUMERIC(3,0),
                                ic1_water_cut_percent      	NUMERIC(4,2),
                                ic1_liquid_rate_bbl_d      	NUMERIC(10,2),
                                ic1_water_rate_bbl_d       	NUMERIC(10,2),
                                ic1_oil_rate_bbl_d         	NUMERIC(10,2),
                                ic2_choke_pressure_percent 	NUMERIC(4,2),
                                ic2_pressure1_psia         	NUMERIC(10,2),
                                ic2_pressure2_psia         	NUMERIC(10,2),
                                ic2_temperature1_degF      	NUMERIC(3,0),
                                ic2_temperature2_degF      	NUMERIC(3,0),
                                ic2_water_cut_percent      	NUMERIC(4,2),
                                ic2_liquid_rate_bbl_d      	NUMERIC(10,2),
                                ic2_water_rate_bbl_d       	NUMERIC(10,2),
                                ic2_oil_rate_bbl_d         	NUMERIC(10,2)
                            )
                           ''')
        print("Table Created Successfully.")
        
        print("Inserting data-large.csv records into our database....")

        #query to insert values into the table 'a2'
        query = '''INSERT INTO a2 (time_in_sec,esp1_discharge_pressure_psia,esp1_intake_pressure_psia,esp1_intake_temperature_K,esp1_motor_temperature_K,esp1_vsdfreqout_Hz,esp1_vsdmotamps_A,esp2_discharge_pressure_psia,esp2_intake_pressure_psia,esp2_intake_temperature_K,esp2_motor_temperature_K,esp2_vsdfreqout_Hz,esp2_vsdmotamps_A,esp3_discharge_pressure_psia,esp3_intake_pressure_psia,esp3_intake_temperature_K,esp3_motor_temperature_K,esp3_vsdfreqout_Hz,esp3_vsdmotamps_A,ic1_choke_pressure_percent,ic1_pressure1_psia,ic1_pressure2_psia,ic1_temperature1_degF,ic1_temperature2_degF,ic1_water_cut_percent,ic1_liquid_rate_bbl_d,ic1_water_rate_bbl_d,ic1_oil_rate_bbl_d,ic2_choke_pressure_percent,ic2_pressure1_psia,ic2_pressure2_psia,ic2_temperature1_degF,ic2_temperature2_degF,ic2_water_cut_percent,ic2_liquid_rate_bbl_d,ic2_water_rate_bbl_d,ic2_oil_rate_bbl_d)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

        #inserting rows after looping the values in df
        for row in df.itertuples():
            values = (row.time_in_sec,row.esp1_discharge_pressure_psia,row.esp1_intake_pressure_psia,row.esp1_intake_temperature_K,row.esp1_motor_temperature_K,row.esp1_vsdfreqout_Hz,row.esp1_vsdmotamps_A,row.esp2_discharge_pressure_psia,row.esp2_intake_pressure_psia,row.esp2_intake_temperature_K,row.esp2_motor_temperature_K,row.esp2_vsdfreqout_Hz,row.esp2_vsdmotamps_A,row.esp3_discharge_pressure_psia,row.esp3_intake_pressure_psia,row.esp3_intake_temperature_K,row.esp3_motor_temperature_K,row.esp3_vsdfreqout_Hz,row.esp3_vsdmotamps_A,row.ic1_choke_pressure_percent,row.ic1_pressure1_psia,row.ic1_pressure2_psia,row.ic1_temperature1_degF,row.ic1_temperature2_degF,row.ic1_water_cut_percent,row.ic1_liquid_rate_bbl_d,row.ic1_water_rate_bbl_d,row.ic1_oil_rate_bbl_d,row.ic2_choke_pressure_percent,row.ic2_pressure1_psia,row.ic2_pressure2_psia,row.ic2_temperature1_degF,row.ic2_temperature2_degF,row.ic2_water_cut_percent,row.ic2_liquid_rate_bbl_d,row.ic2_water_rate_bbl_d,row.ic2_oil_rate_bbl_d)
            new_cursor.execute(query,values)

        #commit the transaction.    
        connect.commit()
        print("Data successfully imported.")
        
finally:
    #disconnecting from the network.
    print("Disconnecting from: ",record)
    connect.close()
    print("Disconnected successfully!")
