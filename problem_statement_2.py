
# coding: utf-8

# # Problem Statement 13_1

# # 1

# In[174]:


import pandas as pd
import numpy as np
import sqlite3 as db
from pandasql import sqldf


# In[177]:


conn = db.connect('sqladb1.db')
curr = conn.cursor()


# In[178]:


curr.execute('DROP TABLE adult;');


# In[179]:


curr.execute("""CREATE TABLE adult ( 
             age int, 
             workclass varchar(40), 
             fnlwgt int, 
             education varchar(40), 
             education_num int, 
             marital_status varchar(40), 
             occupation varchar(20), 
             relationship varchar(40), 
             race varchar(20), 
             sex varchar(10),
             capital_gain int,
             capital_loss int,
             hours_per_week int,
             native_country varchar(20),
             salary varchar(10));""")


# In[180]:


curr.execute('SELECT * FROM adult;').fetchall()


# In[181]:


column_names = ['age','workclass','fnlwgt','education','education_num','marital_status','occupation','relationship','race','sex','capital_gain','capital_loss','hours_per_week','native_country','salary']
df = pd.read_csv('https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.data',names=column_names)


# In[182]:


df.head(10)


# In[185]:


df.to_sql('adult',conn,if_exists='append',index=False)


# In[186]:


conn.execute('SELECT * FROM adult LIMIT 10;').fetchall()


# In[187]:


curr.execute("PRAGMA table_info('adult')")
print(curr.fetchall())


# # 2. average hours per week of all men who are working in private sector

# In[192]:


curr.execute("SELECT avg(hours_per_week) FROM adult where sex=' Male' and workclass=' Private';").fetchall()


# # 3. Frequency tables for education , occupation , relationship

# In[196]:


educational_values = pd.Series(curr.execute("SELECT education FROM adult;").fetchall())
educational_values.value_counts()


# In[197]:


occupational_values = pd.Series(curr.execute("SELECT occupation FROM adult;").fetchall())
occupational_values.value_counts()


# In[198]:


relationship_values = pd.Series(curr.execute("SELECT relationship FROM adult;").fetchall())
relationship_values.value_counts()


# # 4. PEOPLE WHO ARE MARRIED AND WORKING PRIVATE SECTOR WITH MASTER DEGREE

# In[203]:


curr.execute("SELECT * FROM adult where relationship in (' Husband',' Wife') and workclass=' Private' and education=' Masters';").fetchall()


# # 5. average , minimum and maximum age groups of different sectors

# In[209]:


data_frame = pd.DataFrame(curr.execute("SELECT workclass as class,avg(age) as Average,min(age) as Minimum,max(age) as Maximum FROM adult GROUP BY workclass;").fetchall(),columns=['Class','Average','Minimum','Maximum'])
data_frame


# # 6. Age distribution by Country

# In[212]:


age_data = pd.DataFrame(curr.execute("SELECT native_country, avg(age) FROM adult GROUP BY native_country;").fetchall(),columns=['Country','Average Age'])
age_data


# # 7. Net capital gain using capital-gain and capital-loss

# In[215]:


net_capital_gain = pd.DataFrame(curr.execute("SELECT capital_gain-capital_loss from adult;").fetchall(),columns=['Net-Capital-Gain'])
net_capital_gain

