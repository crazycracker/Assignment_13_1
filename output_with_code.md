
# Problem Statement 13_1

# 1


```python
import pandas as pd
import numpy as np
import sqlite3 as db
from pandasql import sqldf
```


```python
conn = db.connect('sqladb1.db')
curr = conn.cursor()
```


```python
curr.execute('DROP TABLE adult;');
```


    ---------------------------------------------------------------------------

    OperationalError                          Traceback (most recent call last)

    <ipython-input-178-819c4d8516b3> in <module>()
    ----> 1 curr.execute('DROP TABLE adult;');
    

    OperationalError: no such table: adult



```python
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
```




    <sqlite3.Cursor at 0x1d2038a35e0>




```python
curr.execute('SELECT * FROM adult;').fetchall()
```




    []




```python
column_names = ['age','workclass','fnlwgt','education','education_num','marital_status','occupation','relationship','race','sex','capital_gain','capital_loss','hours_per_week','native_country','salary']
df = pd.read_csv('https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.data',names=column_names)
```


```python
df.head(10)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>age</th>
      <th>workclass</th>
      <th>fnlwgt</th>
      <th>education</th>
      <th>education_num</th>
      <th>marital_status</th>
      <th>occupation</th>
      <th>relationship</th>
      <th>race</th>
      <th>sex</th>
      <th>capital_gain</th>
      <th>capital_loss</th>
      <th>hours_per_week</th>
      <th>native_country</th>
      <th>salary</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>39</td>
      <td>State-gov</td>
      <td>77516</td>
      <td>Bachelors</td>
      <td>13</td>
      <td>Never-married</td>
      <td>Adm-clerical</td>
      <td>Not-in-family</td>
      <td>White</td>
      <td>Male</td>
      <td>2174</td>
      <td>0</td>
      <td>40</td>
      <td>United-States</td>
      <td>&lt;=50K</td>
    </tr>
    <tr>
      <th>1</th>
      <td>50</td>
      <td>Self-emp-not-inc</td>
      <td>83311</td>
      <td>Bachelors</td>
      <td>13</td>
      <td>Married-civ-spouse</td>
      <td>Exec-managerial</td>
      <td>Husband</td>
      <td>White</td>
      <td>Male</td>
      <td>0</td>
      <td>0</td>
      <td>13</td>
      <td>United-States</td>
      <td>&lt;=50K</td>
    </tr>
    <tr>
      <th>2</th>
      <td>38</td>
      <td>Private</td>
      <td>215646</td>
      <td>HS-grad</td>
      <td>9</td>
      <td>Divorced</td>
      <td>Handlers-cleaners</td>
      <td>Not-in-family</td>
      <td>White</td>
      <td>Male</td>
      <td>0</td>
      <td>0</td>
      <td>40</td>
      <td>United-States</td>
      <td>&lt;=50K</td>
    </tr>
    <tr>
      <th>3</th>
      <td>53</td>
      <td>Private</td>
      <td>234721</td>
      <td>11th</td>
      <td>7</td>
      <td>Married-civ-spouse</td>
      <td>Handlers-cleaners</td>
      <td>Husband</td>
      <td>Black</td>
      <td>Male</td>
      <td>0</td>
      <td>0</td>
      <td>40</td>
      <td>United-States</td>
      <td>&lt;=50K</td>
    </tr>
    <tr>
      <th>4</th>
      <td>28</td>
      <td>Private</td>
      <td>338409</td>
      <td>Bachelors</td>
      <td>13</td>
      <td>Married-civ-spouse</td>
      <td>Prof-specialty</td>
      <td>Wife</td>
      <td>Black</td>
      <td>Female</td>
      <td>0</td>
      <td>0</td>
      <td>40</td>
      <td>Cuba</td>
      <td>&lt;=50K</td>
    </tr>
    <tr>
      <th>5</th>
      <td>37</td>
      <td>Private</td>
      <td>284582</td>
      <td>Masters</td>
      <td>14</td>
      <td>Married-civ-spouse</td>
      <td>Exec-managerial</td>
      <td>Wife</td>
      <td>White</td>
      <td>Female</td>
      <td>0</td>
      <td>0</td>
      <td>40</td>
      <td>United-States</td>
      <td>&lt;=50K</td>
    </tr>
    <tr>
      <th>6</th>
      <td>49</td>
      <td>Private</td>
      <td>160187</td>
      <td>9th</td>
      <td>5</td>
      <td>Married-spouse-absent</td>
      <td>Other-service</td>
      <td>Not-in-family</td>
      <td>Black</td>
      <td>Female</td>
      <td>0</td>
      <td>0</td>
      <td>16</td>
      <td>Jamaica</td>
      <td>&lt;=50K</td>
    </tr>
    <tr>
      <th>7</th>
      <td>52</td>
      <td>Self-emp-not-inc</td>
      <td>209642</td>
      <td>HS-grad</td>
      <td>9</td>
      <td>Married-civ-spouse</td>
      <td>Exec-managerial</td>
      <td>Husband</td>
      <td>White</td>
      <td>Male</td>
      <td>0</td>
      <td>0</td>
      <td>45</td>
      <td>United-States</td>
      <td>&gt;50K</td>
    </tr>
    <tr>
      <th>8</th>
      <td>31</td>
      <td>Private</td>
      <td>45781</td>
      <td>Masters</td>
      <td>14</td>
      <td>Never-married</td>
      <td>Prof-specialty</td>
      <td>Not-in-family</td>
      <td>White</td>
      <td>Female</td>
      <td>14084</td>
      <td>0</td>
      <td>50</td>
      <td>United-States</td>
      <td>&gt;50K</td>
    </tr>
    <tr>
      <th>9</th>
      <td>42</td>
      <td>Private</td>
      <td>159449</td>
      <td>Bachelors</td>
      <td>13</td>
      <td>Married-civ-spouse</td>
      <td>Exec-managerial</td>
      <td>Husband</td>
      <td>White</td>
      <td>Male</td>
      <td>5178</td>
      <td>0</td>
      <td>40</td>
      <td>United-States</td>
      <td>&gt;50K</td>
    </tr>
  </tbody>
</table>
</div>




```python
df.to_sql('adult',conn,if_exists='append',index=False)
```


```python
pd.DataFrame(conn.execute('SELECT * FROM adult LIMIT 10;').fetchall(),columns=column_names)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>age</th>
      <th>workclass</th>
      <th>fnlwgt</th>
      <th>education</th>
      <th>education_num</th>
      <th>marital_status</th>
      <th>occupation</th>
      <th>relationship</th>
      <th>race</th>
      <th>sex</th>
      <th>capital_gain</th>
      <th>capital_loss</th>
      <th>hours_per_week</th>
      <th>native_country</th>
      <th>salary</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>39</td>
      <td>State-gov</td>
      <td>77516</td>
      <td>Bachelors</td>
      <td>13</td>
      <td>Never-married</td>
      <td>Adm-clerical</td>
      <td>Not-in-family</td>
      <td>White</td>
      <td>Male</td>
      <td>2174</td>
      <td>0</td>
      <td>40</td>
      <td>United-States</td>
      <td>&lt;=50K</td>
    </tr>
    <tr>
      <th>1</th>
      <td>50</td>
      <td>Self-emp-not-inc</td>
      <td>83311</td>
      <td>Bachelors</td>
      <td>13</td>
      <td>Married-civ-spouse</td>
      <td>Exec-managerial</td>
      <td>Husband</td>
      <td>White</td>
      <td>Male</td>
      <td>0</td>
      <td>0</td>
      <td>13</td>
      <td>United-States</td>
      <td>&lt;=50K</td>
    </tr>
    <tr>
      <th>2</th>
      <td>38</td>
      <td>Private</td>
      <td>215646</td>
      <td>HS-grad</td>
      <td>9</td>
      <td>Divorced</td>
      <td>Handlers-cleaners</td>
      <td>Not-in-family</td>
      <td>White</td>
      <td>Male</td>
      <td>0</td>
      <td>0</td>
      <td>40</td>
      <td>United-States</td>
      <td>&lt;=50K</td>
    </tr>
    <tr>
      <th>3</th>
      <td>53</td>
      <td>Private</td>
      <td>234721</td>
      <td>11th</td>
      <td>7</td>
      <td>Married-civ-spouse</td>
      <td>Handlers-cleaners</td>
      <td>Husband</td>
      <td>Black</td>
      <td>Male</td>
      <td>0</td>
      <td>0</td>
      <td>40</td>
      <td>United-States</td>
      <td>&lt;=50K</td>
    </tr>
    <tr>
      <th>4</th>
      <td>28</td>
      <td>Private</td>
      <td>338409</td>
      <td>Bachelors</td>
      <td>13</td>
      <td>Married-civ-spouse</td>
      <td>Prof-specialty</td>
      <td>Wife</td>
      <td>Black</td>
      <td>Female</td>
      <td>0</td>
      <td>0</td>
      <td>40</td>
      <td>Cuba</td>
      <td>&lt;=50K</td>
    </tr>
    <tr>
      <th>5</th>
      <td>37</td>
      <td>Private</td>
      <td>284582</td>
      <td>Masters</td>
      <td>14</td>
      <td>Married-civ-spouse</td>
      <td>Exec-managerial</td>
      <td>Wife</td>
      <td>White</td>
      <td>Female</td>
      <td>0</td>
      <td>0</td>
      <td>40</td>
      <td>United-States</td>
      <td>&lt;=50K</td>
    </tr>
    <tr>
      <th>6</th>
      <td>49</td>
      <td>Private</td>
      <td>160187</td>
      <td>9th</td>
      <td>5</td>
      <td>Married-spouse-absent</td>
      <td>Other-service</td>
      <td>Not-in-family</td>
      <td>Black</td>
      <td>Female</td>
      <td>0</td>
      <td>0</td>
      <td>16</td>
      <td>Jamaica</td>
      <td>&lt;=50K</td>
    </tr>
    <tr>
      <th>7</th>
      <td>52</td>
      <td>Self-emp-not-inc</td>
      <td>209642</td>
      <td>HS-grad</td>
      <td>9</td>
      <td>Married-civ-spouse</td>
      <td>Exec-managerial</td>
      <td>Husband</td>
      <td>White</td>
      <td>Male</td>
      <td>0</td>
      <td>0</td>
      <td>45</td>
      <td>United-States</td>
      <td>&gt;50K</td>
    </tr>
    <tr>
      <th>8</th>
      <td>31</td>
      <td>Private</td>
      <td>45781</td>
      <td>Masters</td>
      <td>14</td>
      <td>Never-married</td>
      <td>Prof-specialty</td>
      <td>Not-in-family</td>
      <td>White</td>
      <td>Female</td>
      <td>14084</td>
      <td>0</td>
      <td>50</td>
      <td>United-States</td>
      <td>&gt;50K</td>
    </tr>
    <tr>
      <th>9</th>
      <td>42</td>
      <td>Private</td>
      <td>159449</td>
      <td>Bachelors</td>
      <td>13</td>
      <td>Married-civ-spouse</td>
      <td>Exec-managerial</td>
      <td>Husband</td>
      <td>White</td>
      <td>Male</td>
      <td>5178</td>
      <td>0</td>
      <td>40</td>
      <td>United-States</td>
      <td>&gt;50K</td>
    </tr>
  </tbody>
</table>
</div>




```python
curr.execute("PRAGMA table_info('adult')")
print(curr.fetchall())
```

    [(0, 'age', 'int', 0, None, 0), (1, 'workclass', 'varchar(40)', 0, None, 0), (2, 'fnlwgt', 'int', 0, None, 0), (3, 'education', 'varchar(40)', 0, None, 0), (4, 'education_num', 'int', 0, None, 0), (5, 'marital_status', 'varchar(40)', 0, None, 0), (6, 'occupation', 'varchar(20)', 0, None, 0), (7, 'relationship', 'varchar(40)', 0, None, 0), (8, 'race', 'varchar(20)', 0, None, 0), (9, 'sex', 'varchar(10)', 0, None, 0), (10, 'capital_gain', 'int', 0, None, 0), (11, 'capital_loss', 'int', 0, None, 0), (12, 'hours_per_week', 'int', 0, None, 0), (13, 'native_country', 'varchar(20)', 0, None, 0), (14, 'salary', 'varchar(10)', 0, None, 0)]
    

# 2. average hours per week of all men who are working in private sector


```python
curr.execute("SELECT avg(hours_per_week) FROM adult where sex=' Male' and workclass=' Private';").fetchall()
```




    [(42.22122591006424,)]



# 3. Frequency tables for education , occupation , relationship


```python
educational_values = pd.Series(curr.execute("SELECT education FROM adult;").fetchall())
educational_values.value_counts()
```




    ( HS-grad,)         10501
    ( Some-college,)     7291
    ( Bachelors,)        5355
    ( Masters,)          1723
    ( Assoc-voc,)        1382
    ( 11th,)             1175
    ( Assoc-acdm,)       1067
    ( 10th,)              933
    ( 7th-8th,)           646
    ( Prof-school,)       576
    ( 9th,)               514
    ( 12th,)              433
    ( Doctorate,)         413
    ( 5th-6th,)           333
    ( 1st-4th,)           168
    ( Preschool,)          51
    dtype: int64




```python
occupational_values = pd.Series(curr.execute("SELECT occupation FROM adult;").fetchall())
occupational_values.value_counts()
```




    ( Prof-specialty,)       4140
    ( Craft-repair,)         4099
    ( Exec-managerial,)      4066
    ( Adm-clerical,)         3770
    ( Sales,)                3650
    ( Other-service,)        3295
    ( Machine-op-inspct,)    2002
    ( ?,)                    1843
    ( Transport-moving,)     1597
    ( Handlers-cleaners,)    1370
    ( Farming-fishing,)       994
    ( Tech-support,)          928
    ( Protective-serv,)       649
    ( Priv-house-serv,)       149
    ( Armed-Forces,)            9
    dtype: int64




```python
relationship_values = pd.Series(curr.execute("SELECT relationship FROM adult;").fetchall())
relationship_values.value_counts()
```




    ( Husband,)           13193
    ( Not-in-family,)      8305
    ( Own-child,)          5068
    ( Unmarried,)          3446
    ( Wife,)               1568
    ( Other-relative,)      981
    dtype: int64



# 4. PEOPLE WHO ARE MARRIED AND WORKING PRIVATE SECTOR WITH MASTER DEGREE


```python
married_private_master = pd.DataFrame(curr.execute("SELECT * FROM adult where relationship in (' Husband',' Wife') and workclass=' Private' and education=' Masters';").fetchall(),columns=column_names)
married_private_master.head(10)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>age</th>
      <th>workclass</th>
      <th>fnlwgt</th>
      <th>education</th>
      <th>education_num</th>
      <th>marital_status</th>
      <th>occupation</th>
      <th>relationship</th>
      <th>race</th>
      <th>sex</th>
      <th>capital_gain</th>
      <th>capital_loss</th>
      <th>hours_per_week</th>
      <th>native_country</th>
      <th>salary</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>37</td>
      <td>Private</td>
      <td>284582</td>
      <td>Masters</td>
      <td>14</td>
      <td>Married-civ-spouse</td>
      <td>Exec-managerial</td>
      <td>Wife</td>
      <td>White</td>
      <td>Female</td>
      <td>0</td>
      <td>0</td>
      <td>40</td>
      <td>United-States</td>
      <td>&lt;=50K</td>
    </tr>
    <tr>
      <th>1</th>
      <td>33</td>
      <td>Private</td>
      <td>202051</td>
      <td>Masters</td>
      <td>14</td>
      <td>Married-civ-spouse</td>
      <td>Prof-specialty</td>
      <td>Husband</td>
      <td>White</td>
      <td>Male</td>
      <td>0</td>
      <td>0</td>
      <td>50</td>
      <td>United-States</td>
      <td>&lt;=50K</td>
    </tr>
    <tr>
      <th>2</th>
      <td>76</td>
      <td>Private</td>
      <td>124191</td>
      <td>Masters</td>
      <td>14</td>
      <td>Married-civ-spouse</td>
      <td>Exec-managerial</td>
      <td>Husband</td>
      <td>White</td>
      <td>Male</td>
      <td>0</td>
      <td>0</td>
      <td>40</td>
      <td>United-States</td>
      <td>&gt;50K</td>
    </tr>
    <tr>
      <th>3</th>
      <td>31</td>
      <td>Private</td>
      <td>99928</td>
      <td>Masters</td>
      <td>14</td>
      <td>Married-civ-spouse</td>
      <td>Prof-specialty</td>
      <td>Wife</td>
      <td>White</td>
      <td>Female</td>
      <td>0</td>
      <td>0</td>
      <td>50</td>
      <td>United-States</td>
      <td>&lt;=50K</td>
    </tr>
    <tr>
      <th>4</th>
      <td>34</td>
      <td>Private</td>
      <td>142897</td>
      <td>Masters</td>
      <td>14</td>
      <td>Married-civ-spouse</td>
      <td>Exec-managerial</td>
      <td>Husband</td>
      <td>Asian-Pac-Islander</td>
      <td>Male</td>
      <td>7298</td>
      <td>0</td>
      <td>35</td>
      <td>Taiwan</td>
      <td>&gt;50K</td>
    </tr>
    <tr>
      <th>5</th>
      <td>62</td>
      <td>Private</td>
      <td>270092</td>
      <td>Masters</td>
      <td>14</td>
      <td>Married-civ-spouse</td>
      <td>Prof-specialty</td>
      <td>Husband</td>
      <td>White</td>
      <td>Male</td>
      <td>0</td>
      <td>0</td>
      <td>40</td>
      <td>United-States</td>
      <td>&gt;50K</td>
    </tr>
    <tr>
      <th>6</th>
      <td>41</td>
      <td>Private</td>
      <td>445382</td>
      <td>Masters</td>
      <td>14</td>
      <td>Married-civ-spouse</td>
      <td>Exec-managerial</td>
      <td>Husband</td>
      <td>White</td>
      <td>Male</td>
      <td>0</td>
      <td>1977</td>
      <td>65</td>
      <td>United-States</td>
      <td>&gt;50K</td>
    </tr>
    <tr>
      <th>7</th>
      <td>33</td>
      <td>Private</td>
      <td>208405</td>
      <td>Masters</td>
      <td>14</td>
      <td>Married-civ-spouse</td>
      <td>Prof-specialty</td>
      <td>Husband</td>
      <td>White</td>
      <td>Male</td>
      <td>0</td>
      <td>0</td>
      <td>50</td>
      <td>United-States</td>
      <td>&gt;50K</td>
    </tr>
    <tr>
      <th>8</th>
      <td>49</td>
      <td>Private</td>
      <td>192776</td>
      <td>Masters</td>
      <td>14</td>
      <td>Married-civ-spouse</td>
      <td>Exec-managerial</td>
      <td>Husband</td>
      <td>White</td>
      <td>Male</td>
      <td>0</td>
      <td>1977</td>
      <td>45</td>
      <td>United-States</td>
      <td>&gt;50K</td>
    </tr>
    <tr>
      <th>9</th>
      <td>51</td>
      <td>Private</td>
      <td>410114</td>
      <td>Masters</td>
      <td>14</td>
      <td>Married-civ-spouse</td>
      <td>Craft-repair</td>
      <td>Husband</td>
      <td>White</td>
      <td>Male</td>
      <td>0</td>
      <td>0</td>
      <td>40</td>
      <td>United-States</td>
      <td>&gt;50K</td>
    </tr>
  </tbody>
</table>
</div>



# 5. average , minimum and maximum age groups of different sectors


```python
data_frame = pd.DataFrame(curr.execute("SELECT workclass as class,avg(age) as Average,min(age) as Minimum,max(age) as Maximum FROM adult GROUP BY workclass;").fetchall(),columns=['Class','Average','Minimum','Maximum'])
data_frame
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Class</th>
      <th>Average</th>
      <th>Minimum</th>
      <th>Maximum</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>?</td>
      <td>40.960240</td>
      <td>17</td>
      <td>90</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Federal-gov</td>
      <td>42.590625</td>
      <td>17</td>
      <td>90</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Local-gov</td>
      <td>41.751075</td>
      <td>17</td>
      <td>90</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Never-worked</td>
      <td>20.571429</td>
      <td>17</td>
      <td>30</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Private</td>
      <td>36.797585</td>
      <td>17</td>
      <td>90</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Self-emp-inc</td>
      <td>46.017025</td>
      <td>17</td>
      <td>84</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Self-emp-not-inc</td>
      <td>44.969697</td>
      <td>17</td>
      <td>90</td>
    </tr>
    <tr>
      <th>7</th>
      <td>State-gov</td>
      <td>39.436055</td>
      <td>17</td>
      <td>81</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Without-pay</td>
      <td>47.785714</td>
      <td>19</td>
      <td>72</td>
    </tr>
  </tbody>
</table>
</div>



# 6. Age distribution by Country


```python
age_data = pd.DataFrame(curr.execute("SELECT native_country, avg(age) FROM adult GROUP BY native_country;").fetchall(),columns=['Country','Average Age'])
age_data
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Country</th>
      <th>Average Age</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>?</td>
      <td>38.725557</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Cambodia</td>
      <td>37.789474</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Canada</td>
      <td>42.545455</td>
    </tr>
    <tr>
      <th>3</th>
      <td>China</td>
      <td>42.533333</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Columbia</td>
      <td>39.711864</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Cuba</td>
      <td>45.768421</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Dominican-Republic</td>
      <td>37.728571</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Ecuador</td>
      <td>36.642857</td>
    </tr>
    <tr>
      <th>8</th>
      <td>El-Salvador</td>
      <td>34.132075</td>
    </tr>
    <tr>
      <th>9</th>
      <td>England</td>
      <td>41.155556</td>
    </tr>
    <tr>
      <th>10</th>
      <td>France</td>
      <td>38.965517</td>
    </tr>
    <tr>
      <th>11</th>
      <td>Germany</td>
      <td>39.255474</td>
    </tr>
    <tr>
      <th>12</th>
      <td>Greece</td>
      <td>46.206897</td>
    </tr>
    <tr>
      <th>13</th>
      <td>Guatemala</td>
      <td>32.421875</td>
    </tr>
    <tr>
      <th>14</th>
      <td>Haiti</td>
      <td>38.272727</td>
    </tr>
    <tr>
      <th>15</th>
      <td>Holand-Netherlands</td>
      <td>32.000000</td>
    </tr>
    <tr>
      <th>16</th>
      <td>Honduras</td>
      <td>33.846154</td>
    </tr>
    <tr>
      <th>17</th>
      <td>Hong</td>
      <td>33.650000</td>
    </tr>
    <tr>
      <th>18</th>
      <td>Hungary</td>
      <td>49.384615</td>
    </tr>
    <tr>
      <th>19</th>
      <td>India</td>
      <td>38.090000</td>
    </tr>
    <tr>
      <th>20</th>
      <td>Iran</td>
      <td>39.418605</td>
    </tr>
    <tr>
      <th>21</th>
      <td>Ireland</td>
      <td>36.458333</td>
    </tr>
    <tr>
      <th>22</th>
      <td>Italy</td>
      <td>46.424658</td>
    </tr>
    <tr>
      <th>23</th>
      <td>Jamaica</td>
      <td>35.592593</td>
    </tr>
    <tr>
      <th>24</th>
      <td>Japan</td>
      <td>38.241935</td>
    </tr>
    <tr>
      <th>25</th>
      <td>Laos</td>
      <td>34.722222</td>
    </tr>
    <tr>
      <th>26</th>
      <td>Mexico</td>
      <td>33.290824</td>
    </tr>
    <tr>
      <th>27</th>
      <td>Nicaragua</td>
      <td>33.617647</td>
    </tr>
    <tr>
      <th>28</th>
      <td>Outlying-US(Guam-USVI-etc)</td>
      <td>38.714286</td>
    </tr>
    <tr>
      <th>29</th>
      <td>Peru</td>
      <td>35.258065</td>
    </tr>
    <tr>
      <th>30</th>
      <td>Philippines</td>
      <td>39.444444</td>
    </tr>
    <tr>
      <th>31</th>
      <td>Poland</td>
      <td>43.116667</td>
    </tr>
    <tr>
      <th>32</th>
      <td>Portugal</td>
      <td>40.297297</td>
    </tr>
    <tr>
      <th>33</th>
      <td>Puerto-Rico</td>
      <td>40.508772</td>
    </tr>
    <tr>
      <th>34</th>
      <td>Scotland</td>
      <td>40.416667</td>
    </tr>
    <tr>
      <th>35</th>
      <td>South</td>
      <td>38.750000</td>
    </tr>
    <tr>
      <th>36</th>
      <td>Taiwan</td>
      <td>33.823529</td>
    </tr>
    <tr>
      <th>37</th>
      <td>Thailand</td>
      <td>34.944444</td>
    </tr>
    <tr>
      <th>38</th>
      <td>Trinadad&amp;Tobago</td>
      <td>41.315789</td>
    </tr>
    <tr>
      <th>39</th>
      <td>United-States</td>
      <td>38.655674</td>
    </tr>
    <tr>
      <th>40</th>
      <td>Vietnam</td>
      <td>34.059701</td>
    </tr>
    <tr>
      <th>41</th>
      <td>Yugoslavia</td>
      <td>38.812500</td>
    </tr>
  </tbody>
</table>
</div>



# 7. Net capital gain using capital-gain and capital-loss


```python
net_capital_gain = pd.DataFrame(curr.execute("SELECT capital_gain-capital_loss from adult;").fetchall(),columns=['Net-Capital-Gain'])
net_capital_gain
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Net-Capital-Gain</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2174</td>
    </tr>
    <tr>
      <th>1</th>
      <td>0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>0</td>
    </tr>
    <tr>
      <th>5</th>
      <td>0</td>
    </tr>
    <tr>
      <th>6</th>
      <td>0</td>
    </tr>
    <tr>
      <th>7</th>
      <td>0</td>
    </tr>
    <tr>
      <th>8</th>
      <td>14084</td>
    </tr>
    <tr>
      <th>9</th>
      <td>5178</td>
    </tr>
    <tr>
      <th>10</th>
      <td>0</td>
    </tr>
    <tr>
      <th>11</th>
      <td>0</td>
    </tr>
    <tr>
      <th>12</th>
      <td>0</td>
    </tr>
    <tr>
      <th>13</th>
      <td>0</td>
    </tr>
    <tr>
      <th>14</th>
      <td>0</td>
    </tr>
    <tr>
      <th>15</th>
      <td>0</td>
    </tr>
    <tr>
      <th>16</th>
      <td>0</td>
    </tr>
    <tr>
      <th>17</th>
      <td>0</td>
    </tr>
    <tr>
      <th>18</th>
      <td>0</td>
    </tr>
    <tr>
      <th>19</th>
      <td>0</td>
    </tr>
    <tr>
      <th>20</th>
      <td>0</td>
    </tr>
    <tr>
      <th>21</th>
      <td>0</td>
    </tr>
    <tr>
      <th>22</th>
      <td>0</td>
    </tr>
    <tr>
      <th>23</th>
      <td>-2042</td>
    </tr>
    <tr>
      <th>24</th>
      <td>0</td>
    </tr>
    <tr>
      <th>25</th>
      <td>0</td>
    </tr>
    <tr>
      <th>26</th>
      <td>0</td>
    </tr>
    <tr>
      <th>27</th>
      <td>0</td>
    </tr>
    <tr>
      <th>28</th>
      <td>0</td>
    </tr>
    <tr>
      <th>29</th>
      <td>0</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
    </tr>
    <tr>
      <th>32531</th>
      <td>0</td>
    </tr>
    <tr>
      <th>32532</th>
      <td>0</td>
    </tr>
    <tr>
      <th>32533</th>
      <td>0</td>
    </tr>
    <tr>
      <th>32534</th>
      <td>0</td>
    </tr>
    <tr>
      <th>32535</th>
      <td>0</td>
    </tr>
    <tr>
      <th>32536</th>
      <td>0</td>
    </tr>
    <tr>
      <th>32537</th>
      <td>0</td>
    </tr>
    <tr>
      <th>32538</th>
      <td>15020</td>
    </tr>
    <tr>
      <th>32539</th>
      <td>0</td>
    </tr>
    <tr>
      <th>32540</th>
      <td>0</td>
    </tr>
    <tr>
      <th>32541</th>
      <td>0</td>
    </tr>
    <tr>
      <th>32542</th>
      <td>0</td>
    </tr>
    <tr>
      <th>32543</th>
      <td>0</td>
    </tr>
    <tr>
      <th>32544</th>
      <td>0</td>
    </tr>
    <tr>
      <th>32545</th>
      <td>0</td>
    </tr>
    <tr>
      <th>32546</th>
      <td>0</td>
    </tr>
    <tr>
      <th>32547</th>
      <td>0</td>
    </tr>
    <tr>
      <th>32548</th>
      <td>1086</td>
    </tr>
    <tr>
      <th>32549</th>
      <td>0</td>
    </tr>
    <tr>
      <th>32550</th>
      <td>0</td>
    </tr>
    <tr>
      <th>32551</th>
      <td>0</td>
    </tr>
    <tr>
      <th>32552</th>
      <td>0</td>
    </tr>
    <tr>
      <th>32553</th>
      <td>0</td>
    </tr>
    <tr>
      <th>32554</th>
      <td>0</td>
    </tr>
    <tr>
      <th>32555</th>
      <td>0</td>
    </tr>
    <tr>
      <th>32556</th>
      <td>0</td>
    </tr>
    <tr>
      <th>32557</th>
      <td>0</td>
    </tr>
    <tr>
      <th>32558</th>
      <td>0</td>
    </tr>
    <tr>
      <th>32559</th>
      <td>0</td>
    </tr>
    <tr>
      <th>32560</th>
      <td>15024</td>
    </tr>
  </tbody>
</table>
<p>32561 rows × 1 columns</p>
</div>


