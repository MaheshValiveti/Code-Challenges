# -*- coding: utf-8 -*-
"""
Created on Sat Apr 20 21:51:04 2019

@author: Mahesh
"""

import sys, os, re, csv, pyodbc

def main (path):
    global conn, cur
    
    conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=LAPTOP-IAQHKPIQ\MSSQLSERVER2016;'
                      'Database=Test;'
                      'Trusted_Connection=yes;')
    cur = conn.cursor()
    getFileNames(path)

#%%    
def getFileNames(path):
    #list all CSV filenames
    # go thru each CSV 
    try:    
        for root, dirs, files in os.walk(path):
            for file in files:
                if file.endswith('.csv'):
                    createTableAndInsert(os.path.join(root, file), file)
    #print (filenames)
    except OSError as err:
        print("OS error: {0}".format(err))
    except:
        print("Unexpected error:", sys.exc_info()[0])
        raise
    finally:
         cur.close()
         conn.close()
#%%
def createTableAndInsert(csvFile, fileName):
    
    with open(csvFile, 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter = ',', quotechar = '"')
        strFields, strInsertFields  = '', ''
        
        rows = [tuple(x) for x in reader]
        row_2 = rows[1]
        fields = {}
        
        for index in range(len(row_2)):
            fields['column'+str(index+1)] = findMatch(row_2[index])
        
        #print(fields)
        
        for key, value in fields.items():
            strFields += ''.join(key) 
            strFields += ''.join('\t')
            strFields += ''.join(value)
            strFields += ''.join(',\n')
            strInsertFields += ''.join(key) +','
        
        tableName = fileName.replace('.csv','')
        
        sqlStr = 'create table ' + tableName + '\n(' + strFields[:-2] + ')'
        sqlInsert = 'INSERT INTO ' + tableName + '(' + strInsertFields[:-1] + ') VALUES(' + ','.join(tuple('?' * len(row_2))) + ')'
        
        #To create table in DB
        createDBTable (sqlStr)
        #To insert data into table
        insertDataIntoTable(sqlInsert, rows)
        
    csvfile.close

    #print(sqlStr)
    #print(sqlInsert)
    #print(to_db[1])
    
#%%
def findMatch(val):
     if re.match(r'[-+]*\d+$',val):
         dataType = 'int'
     elif re.match(r'[-+]*\d+\.\d+$',val):
         dataType = 'float'
     elif isBoolean(val):
         dataType = 'boolean'
     elif bool(re.match(r'\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}$',val)):
         dataType = 'DateTime'
     else:
         dataType = 'varchar(500)'

     return dataType
     
#%%

def isBoolean(val):
    try:
        return type(eval(val))==bool
    except:
        return False
    
#%%
# =============================================================================
# print (findMatch ('-124'))
# print (findMatch ('+1124.107'))
# print (findMatch ('2019-10-10 10:01:01'))
# print (isBoolean('False'))
# print (findMatch('18000888888'))
# =============================================================================

#%%
def createDBTable(tblScript):
    cur.execute(tblScript)    
    conn.commit()
    
#%%
def insertDataIntoTable (InsertScript, rows):
    cur.executemany(InsertScript, rows)
    conn.commit()
    
#%%
if __name__ == '__main__':
    main(sys.argv[1])
    
#%%
