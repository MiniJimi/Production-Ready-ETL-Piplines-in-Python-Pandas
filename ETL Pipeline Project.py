#!/usr/bin/env python
# coding: utf-8

# In[16]:


import boto3
import pandas as pd 
from io import StringIO, BytesIO
from datetime import datetime, timedelta


# In[17]:


# Adapter Layer
# read csv file from source, covert to data frame

def read_csv_to_df(bucket, key, decoding = 'utf-8', seperator=','):
    csv_obj = bucket.Object(key=key).get().get('Body').read().decode(decoding)
    data = StringIO(csv_obj)
    df = pd.read_csv(data, delimiter=seperator)
    return df

#write to S3
def write_df_to_s3(bucket, df, key):
    out_buffer=BytesIO()
    df.to_parquet(out_buffer, index=False)
    bucket.put_object(Body=out_buffer.getvalue(), Key=key)
    return True

#return objects
def return_objects(bucket, arg_date):
    min_date = datetime.strptime(arg_date, src_format).date() - timedelta(days=1) #dt=day time
    objects = [obj.key for obj in bucket.objects.all() if datetime.strptime(obj.key.split('/')[0],src_format).date() >=min_date]
    return objects

#the rest of the code is part of the application layer


# In[ ]:


#Application Layer

def extract(bucket, objects):
    df_all = pd.concat([csv_to_df(bucket, obj) for obj in objects], ignore_index=True)
    return df

def transform_report1(df, columns, arg_date):
    df = df.loc[:, columns]
    df.dropna(inplace=True)
    df['opening_price']=df.sort_values(by=['Time']).groupby(['ISIN','Date'])['StartPrice'].transform('first')
    df['closing_price']=df.sort_values(by=['Time']).groupby(['ISIN','Date'])['StartPrice'].transform('last')
    df=df.groupby(['ISIN','Date'], as_index=False).agg(opening_price_eur=('opening_price', 'min'), closing_price_eur=('closing_price', 'min'), minimum_price_eur=('MinPrice', 'min'), maximum_price_eur=('MaxPrice', 'max'), daily_traded_volume=('TradedVolume','sum'))
    
    df['prev_closing_price'] = df.sort_values(by=['Date']).groupby(['ISIN'])['closing_price_eur'].shift(1)
    df['change_prev_closing_%'] = (df['closing_price_eur'] - df['prev_closing_price']) / df['prev_closing_price'] * 100
    df.drop(columns=['prev_closing_price'], inplace=True)
    df = df.round(decimals=2)
    df= df[df.Date>= arg_date]
    return df

def load(bucket, df, trg_key, trg_format):
    key = trg_key + datetime.today().strftime("%Y%m%d_%H%M%S") + trg_format
    write_df_to_s3(bucket, df, key)
    return True

def etl_report1(src_bucket,trg_bucket, objects, columns, arg_date, trg_key, trg_format):
    df=extract(src_bucket, objects)
    df=transform_report1(df, columns, arg_date)
    load(trg_bucket, df, trg_key, trg_format)
    return True

    
    


# In[ ]:


trg_key='xetra_daily_report_'
trg_format= '.parquet'


# In[10]:


arg_date= '2021-05-09'
src_format= '%Y-%m-%d'
src_bucket='deutsche-boerse-xetra-pds'
trg_bucket= 'xerta-1234'
columns = ['ISIN', 'Date', 'Time', 'StartPrice', 'MaxPrice', 'MinPrice', 'EndPrice', 'TradedVolume']


# In[11]:





# In[12]:


s3= boto3.resource('s3')
bucket = s3.Bucket(src_bucket)


# In[4]:





# In[ ]:





# ## Reading the uploaded file

# In[ ]:


for obj in bucket_target.objects.all():
    print(obj.key)


# In[ ]:


prq_obj= bucket_target.Object(key='xetra_daily_report_20210510_143346.parquet').get().get('Body').read()
data=BytesIO(prq_obj)
df_report=pd.read_parquet(data)


# In[ ]:


df_report

