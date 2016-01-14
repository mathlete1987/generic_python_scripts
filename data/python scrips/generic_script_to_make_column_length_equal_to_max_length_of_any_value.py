# -*- coding: utf-8 -*-
"""
Created on Thu Dec 03 21:37:27 2015

"""

import os
import pandas as pd
from sqlalchemy import create_engine
import urllib

#conn_str = "DRIVER={SQL Server};SERVER=<IP Address here>;UID=<User ID here>;PWD=<password here>;Database=<database name here>"
conn_str = "DRIVER={SQL Server};SERVER=USARRRCHASE2\SQLEXPRESS;Database=<database name here>; Trusted_Connection=yes"
conn_str = urllib.quote_plus(conn_str) 
conn_str = "mssql+pyodbc:///?odbc_connect=%s" % conn_str

engine = create_engine(conn_str)

sql = '''   select t.name as tableName, c.name as colName, n.name as colType, c.max_length
            from <SQL TABLE NAME HERE>.sys.tables t
            left join sys.columns c
            	on t.object_id = c.object_id
            left join sys.types n
            	on n.system_type_id = c.system_type_id'''

df = pd.read_sql_query(sql, engine)

for table in df.tableName.unique():
    cols = df[df.tableName == table]    
    #cols.to_clipboard()
    #newCols = pd.DataFrame()        
    sql = "SELECT "        
    
    for idx, row in cols.iterrows():
        colName = row['colName']
        colType = row['colType']
        
        if colType == 'varchar':
            sql_sz = "SELECT MAX(LEN(["+ colName +"])) FROM "+ table + ";"   
            sz = pd.read_sql_query(sql_sz, engine)
            colSize = sz.iloc[0][0].astype(str)
            print colName, colSize            
            
           #newCols = newCols.append([[table, colName, colType, colSize]])
            sql = sql + "CAST([%s] AS VARCHAR(%s)) AS [%s], " % (colName, colSize, colName)
            
        else:
            #newCols = newCols.append([[table, colName, colType]])
            sql = sql + "[%s], " % colName
    
    #newCols.columns = ['table', 'colName', 'colType', 'colSize']
    #print newCols.head()
    
    #Remvoving last comma/space
    sql = sql[:-2]
    newTable = table + "_NEW"
    sql = sql + " INTO %s FROM %s;" % (newTable, table)
    
    print sql
        
    #connection = engine.connect()
    #connection.execute(sql)
    #connection.close()
    

#for idx, row in df.iterrows():
#    tableName = row['tableName']
#    colName = row['colName']
#    
#    sql_sz = "SELECT MAX(LEN(["+ colName +"])) FROM "+ tableName + ";"   
#    #print sql_sz    
#    
#    sz = pd.read_sql_query(sql_sz, engine)
#    
#    colSize = sz.iloc[0][0].astype(str)
#    
#    sql_resz = "ALTER TABLE " + tableName + " ALTER COLUMN [" + colName +"] [varchar](" + colSize + ")"
#    
#    connection = engine.connect()
#    connection.execute(sql_resz)
#    connection.close()    
#    
#    print "%s resized to %s in table %s" % (colName, colSize, tableName)

    

#df.head()

#connection = engine.connect()
#connection.execute("")
#connection.close()