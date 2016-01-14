# -*- coding: utf-8 -*-
"""
Created on Wed Dec 02 16:34:15 2015

@author: rchase
"""

import os
import pandas as pd
from sqlalchemy import create_engine
import urllib

 #convert to utf-8 using sys (do this if you want to use the mask = df == "nan"; df[mask] = "" bit of code)
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

# Writing MDC data to Postgres Database
#conn_str = "DRIVER={SQL Server};SERVER=10.118.251.84;UID=Inveritas_Writer;PWD=f8truZaq;Database=Inveritas_Sysco_STG"
conn_str = "DRIVER={SQL Server};SERVER=USARRRCHASE2\SQLEXPRESS;Database=<Database name here>; Trusted_Connection=yes"
conn_str = urllib.quote_plus(conn_str) 
conn_str = "mssql+pyodbc:///?odbc_connect=%s" % conn_str

engine = create_engine(conn_str)
#http://docs.sqlalchemy.org/en/latest/dialects/mssql.html#module-sqlalchemy.dialects.mssql.pymssql
#http://pymssql.org/en/latest/
                             
#========================================
# Arizona Item Master Script
#========================================
os.chdir('C:\\Users\\rchase\\Documents\\<working directory here>')
connection = engine.connect()
connection.execute("IF OBJECT_ID('<TABLE NAME 1 HERE>') IS NOT NULL DROP TABLE <TABLE NAME 1 HERE>")
connection.execute("IF OBJECT_ID('<TABLE NAME 2 HERE>') IS NOT NULL DROP TABLE <TABLE NAME 2 HERE>")

connection.close()

#for every file in the working directory, print the filename and append the data in the file to the dict, df
for f in os.listdir(os.getcwd()):
    print f
    
    #read in each file f, and append to dict, df.
    df = pd.read_excel(f, 0, header = 0)
    
    #create a column containing the name of the xlsx file in each data object
    df['fileName'] = f
    
    #convert datatypes to strings (although when the table gets read into SQL it's smart enough to create the appropriate datatypes)
    for col in df:
      df[col] = df[col].astype(str)
      
    #create mask to take our nans: 
    mask = df == "nan" 
    df[mask]= ""
       
    #send the df to SQL
    #the try-except statement allows you to continue the loop even if one of the files has an extra column, etc. 
    #the process will continue by loading it into another table instead
    try:
        df.to_sql('SQL TABLE NAME 1 HERE', engine,\
                                if_exists='append', index=False)
    except:
        try:
            df.to_sql('<SQL TABLE NAME 2 HERE>', engine,\
                                if_exists='append', index=False)
        except:
            print f + ' file failed upload'
            continue
        
        continue