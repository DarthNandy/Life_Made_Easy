#!/usr/bin/env python
# coding: utf-8

# In[29]:


import pandas as pd
import re
import csv
import logging
import mysql.connector
import numpy as np
from datetime import datetime
from pandas import ExcelWriter
from pandas import ExcelFile
df = pd.read_csv('C:/Users/Ugly_Executioner/Downloads/cdrs.csv')
regex = re.compile('[@_!#$%^&*()<>?/\|}{~:]+') 
#print("column Headings")
#print(df.columns)
#print(df['human_start_time'])


# In[30]:


Human_End_time=df['human_end_time']


# In[ ]:





# In[31]:


#loop to check if the destination number is less than 5 if yes it will insert it into the csv file
with open('C:/Users/Ugly_Executioner/Desktop/Log/Logfile.csv', 'w') as writeFile:
    writer = csv.writer(writeFile)
    for i in df.index:
            if (regex.search(df['destination_number'][i]) == None):
                n=int(float(df['destination_number'][i]))
                count=0
                while(n>0):
                    count=count+1
                    n=n//10
                if count < 5:
                    writer.writerow([df['human_end_time'][i]]+[df['destination_number'][i]]+[df['billsec'][i]]+[df['caller_id_number'][i]])
                    
                    i+=1
            else:
                i=i+1  
writeFile.close()


# In[32]:


#loop to check if the caller_id_number is greater than 4 if yes it will insert into the csv file
with open('C:/Users/Ugly_Executioner/Desktop/Log/Logfile.csv', 'r') as inp, open('C:/Users/Ugly_Executioner/Desktop/Log/Logfile_edited.csv', 'w') as out:
    writer = csv.writer(out)
    for i in df.index:
        m=int(float(df['caller_id_number'][i]))
        count1=0
        while(m>0):
            count1=count1+1
            m=m//10
        if count1 > 4:
            writer.writerow([df['human_end_time'][i]]+[df['destination_number'][i]]+[df['billsec'][i]]+['\t']+[df['caller_id_number'][i]]+['\t']+['0'])
            i+=1
        else:
            i=i+1
writeFile.close()            


# In[ ]:




    
    


# In[ ]:



  


# In[33]:


#code to enter the log_edited.csv files into the database,log file
csv_file ='C:/Users/Ugly_Executioner/Desktop/Log/Logfile_edited.csv'
txt_file ='C:/Users/Ugly_Executioner/Desktop/Log/Database.log '
with open(txt_file, "w") as my_output_file:
    with open(csv_file, "r") as my_input_file:
        [ my_output_file.write(" ".join(row)+'\n') for row in csv.reader(my_input_file)]
    my_output_file.close()


# In[34]:


#code to insert the data into the database phone_bill inside the table call_details
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  passwd="",
  db="Phone_Bill"
)
data = pd.read_excel('C:/Users/Ugly_Executioner/Desktop/Log/Logfile_edited.xls')
data = data.rename
cursor = mydb.cursor()
cursor.execute("LOAD DATA INFILE 'C:/Users/Ugly_Executioner/Desktop/Log/Logfile.csv' INTO TABLE Call_details FIELDS TERMINATED BY ',';")
mydb.commit()
cursor.close()

