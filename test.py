#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().system(' rm -rf *.zip')


# In[2]:


get_ipython().system(' wget https://resources.lendingclub.com/LoanStats_2018Q4.csv.zip')


# In[3]:


get_ipython().system(' unzip LoanStats_2018Q4.csv.zip -y')


# In[4]:


#! wc -l LoanStats_2018Q4.csv


# In[5]:


get_ipython().system(" sed '1d' LoanStats_2018Q4.csv > LoanStats_2018Q4_1.csv")


# In[6]:


get_ipython().system(' hdfs dfs -rm ./LoanStats_2018Q4_1.csv')


# In[7]:


get_ipython().system(' hdfs dfs -put LoanStats_2018Q4_1.csv ./')


# In[8]:


raw_df = spark.read.format('csv').option('header','true').option('mode','DROPMALFORMED').load('./LoanStats_2018Q4_1.csv')


# In[9]:


raw_df.printSchema()


# In[10]:


#raw_df.select('loan_status').distinct().show()


# In[11]:


#raw_df.count()


# In[12]:


raw_df.registerTempTable('loan_all')


# In[13]:


spark.sql('select loan_status,count(*) as num from loan_all group by loan_status').show()


# In[14]:


loan_status_count_df = spark.sql('select loan_status,count(*) as num from loan_all group by loan_status')


# In[15]:


loan_status_count_df.registerTempTable('loan_status_count')


# In[16]:


spark.sql('select * from loan_status_count where num > 100000').show()


# In[17]:


loan_status_count_df.write.mode("overwrite").saveAsTable("loan_status_count_hive")


# In[18]:


get_ipython().system(' hive -e "show tables"')


# In[ ]:




