# -*- coding: utf-8 -*-
"""
Created on Wed Mar  6 21:44:45 2019

@author: Mahesh
"""
#%%    
import csv
import phonenumbers
import pandas as pd
import tldextract
import psycopg2

    #create Contacts data frame
File = "company_contacts.csv"
    
try:
    with open (File,'r') as f:    
        reader = csv.reader(f)
        
        contacts = []    
        
        for row in reader:        
            phonenos = int(float("0{0}".format(str(row[2])).replace(' ','').replace('.','')))
            domain = "{}.{}".format(tldextract.extract(row[0]).domain, tldextract.extract(row[0]).suffix)
            
            for match in phonenumbers.PhoneNumberMatcher(str(phonenos),"US"):
                formattedPhoneno = phonenumbers.format_number(match.number, phonenumbers.PhoneNumberFormat.E164)
            
            contacts.append ((domain, row[0], row[1], formattedPhoneno))
            
        df_contacts = pd.DataFrame(contacts,columns=['domain','homepage_url','email','phone'])
    
    #print(df_contacts.head())

    #Create customers data frame
    
    conn = psycopg2.connect(host="data-eng-challenge.cyndx.io",database="test", user="de_test_user", password="cofDen-fuzhu2-moqreq")
    
    conn.autocommit = True
    
    cur = conn.cursor()
    
    sql = """SELECT * from companies;"""
    
    cur.execute(sql)
    
    rows = cur.fetchall()

    companies = []

    for row in rows:
        companies.append (row)
       
    df_companies = pd.DataFrame(companies,columns=['name','domain'])

    #print(df_companies.head())
    
    #merge both Inner Join
    df_result = pd.merge(df_companies, df_contacts, how='inner', on='domain' )

    #Merge data contacts data to customers
    print(df_result.describe())
   
    #Insert into table
    
    Sql = "Insert into Mahesh_Solution (name, domain, homepage_url, email, phone) values (%s, %s, %s, %s, %s)"
    cur.executemany(Sql, df_result.astype(str).itertuples(index=False))

except OSError as err:
    print("OS error: {0}".format(err))
    f.close()
except:
    print("Unexpected error:", sys.exc_info()[0])
    raise
finally:
    cur.close()
    conn.close()
