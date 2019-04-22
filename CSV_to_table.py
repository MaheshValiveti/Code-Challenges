# -*- coding: utf-8 -*-
"""
Created on Sat Apr 20 21:51:04 2019

@author: Mahesh
"""

import os, re, csv, sqlite3


def getFileNames():
    
    #list all CSV filenames
    # go thru each CSV 
    try:    
        for root, dirs, files in os.walk('c:\Cyndx_1'):
            for file in files:
                if file.endswith('.csv'):
                    generateTableScript(os.path.join(root, file), file)

    #print (filenames)
    except OSError as err:
        print("OS error: {0}".format(err))
=    except:
        print("Unexpected error:", sys.exc_info()[0])
        raise
    finally:
        cur.close()
        conn.close()
#%%

def generateTableScript(csvFile, fileName):
    
    with open(csvFile, 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter = ',', quotechar = '"')
        strFields, strInsertFields  = '', ''
        
        rows = [x for x in reader]
        row_2 = rows[1]
        
        for index in range(len(row_2)):
            strFields += ''.join(' column' + str(index+1)) 
            strFields += ''.join('\t')
            strFields += ''.join(findMatch(row_2[index]))
            strFields += ''.join(',\n')

            strInsertFields += ''.join(' column' + str(index+1)) +','
            
    
        tableName = fileName.replace('.csv','')
        
        sqlStr = 'create table ' + tableName + ' \n (' + strFields[:-2] + ')'
        sqlInsert = ' INSERT INTO ' + tableName + '(' + strInsertFields[:-2] + ' ) VALUES(' + ','.join(tuple('?' * len(row_2))) + ')'
        
        # To create table in DB
        createDBTable (sqlStr)
       
                #To insert data into table
        insertDataIntoTable(sqlInsert, tuple(rows))
        
    print(sqlStr)
    print(sqlInsert)

    #print(to_db[1])
    csvfile.close
    
#%%
def findMatch(val):
     if re.match(r'\d+',val):
         dataType = 'int'
     elif re.match(r'\d+\.\d+',val):
         dataType = 'float'
     elif type(val) == bool:
         dataType == 'boolean'
     #elif
     #: re.match(r'^\d\d\d\d-(0?[1-9]|1[0-2])-(0?[1-9]|[12][0-9]|3[01]) (00|[0-9]|1[0-9]|2[0-3]):([0-9]|[0-5][0-9]):([0-9]|[0-5][0-9])$')
     else:
         dataType = 'varchar(32)'
         
     return dataType

# 
#%%
def findMatch1(val):  
    if isinstance(val, int):
        dataType = 'int'
    elif isinstance(val, float):
        dataType = 'float'
    elif isinstance(val, bool):
        dataType = 'boolean'
  #  elif isinstance(val, datetime.datetime):
   #     dataType = 'bigint'
    elif isinstance(val, str):
        dataType = 'varchar(500)'
    
    return dataType
        
#%%

conn = sqlite3.connect('database.db')
cur = conn.cursor()

def createDBTable(tblScript):
    cur.execute(tblScript)    
    conn.commit()
    
#%%
def insertDataIntoTable (InsertScript, rows):
    cur.executemany(InsertScript, rows)
    conn.commit()
#%%