# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import os
import pandas as pd
from sqlalchemy import create_engine
import urllib


# Writing MDC data to Postgres Database
#conn_str = "DRIVER={SQL Server};SERVER=<IPAddress here>;UID=<user ID here>;PWD=<password here>;Database=<database name here>"
conn_str = "DRIVER={SQL Server};SERVER=USARRRCHASE2\SQLEXPRESS;Database=<Database name here>; Trusted_Connection=yes"
conn_str = urllib.quote_plus(conn_str) 
conn_str = "mssql+pyodbc:///?odbc_connect=%s" % conn_str

engine = create_engine(conn_str)
#http://docs.sqlalchemy.org/en/latest/dialects/mssql.html#module-sqlalchemy.dialects.mssql.pymssql
#http://pymssql.org/en/latest/

#========================================
# Arizona Shipment History Import Script
#========================================
os.chdir('C:\\Users\\rchase\\Documents\\<working directory here>')
connection = engine.connect()
connection.execute("IF OBJECT_ID('<TABLE NAME HERE>') IS NOT NULL DROP TABLE <TABLE NAME HERE>")
connection.close()

#for every file in the working directory, print the filename and append the data in the file to the dict, df
for f in os.listdir(os.getcwd()):
    print f
    
    #only need to do this if the header is not contained on the sheets.  Otherwise you can specify 0 in the pd.read_excel() function for the header argument 
    col_header = ['<header1_here>', '<header2_here>', '<header3_here>']
       
    #read in each file, f and append to dict, df.
    df = pd.read_excel(f, None, header=None, names=col_header, parse_cols="B:N")
   
    #Iteritems() is a method that you can use on dict objects to create an iterator-generator.  The method .items() used to actually print out each key-value pair and it was more expensive in terms of memory: 
    #From <http://stackoverflow.com/questions/10458437/what-is-the-difference-between-dict-items-and-dict-iteritems> 
    #the key is the sheetname, and the sheet is the data
     #printing the key (i.e. sheetname) as the loop runs so that I can know which file the loop is on / which one it errors out on
    for key, sheet in df.iteritems():
        print key 
        
        #handling NANs
        if key == '<tab name here>' and str(df['<tab name here>'].iloc[0]['Billing Date']) == 'nan':
            sheet = sheet.ix[2:]
    
        #create a column containing the name of the xlsx file in each data object
        sheet['fileName'] = f
        
        sheet.to_sql('<SQL TABLE NAME HERE>, engine,\
                                if_exists='append', index=False)
                                

   
        
       