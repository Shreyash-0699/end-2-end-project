import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from dotenv import load_dotenv
import boto3
from boto3.session import Session
from utils import (upload_to_aws, get_df)
import datetime
import io
import os

load_dotenv()

access_key = os.environ.get('access')
secret_key = os.environ.get('secret')
bucket = os.environ.get('bucket')

s3 = boto3.client('s3', aws_access_key_id = access_key, aws_secret_access_key = secret_key)

df_original = get_df(bucket, 'original.csv')
df_customer = get_df(bucket, 'customer.csv')
df_transaction = get_df(bucket, 'transaction.csv')

df_original['date'] = pd.to_datetime(df_original['date']).dt.date

for i in range(0, len(df_transaction)):
    string = df_transaction['product_name'][i]
    new_string = ''.join([j for j in string if not j.isdigit()])
    df_transaction['product_name'][i] = new_string.strip()

customer_info = pd.merge(df_original, df_customer, how = "left", on = "customer_id")
customer_detail = pd.merge(customer_info, df_transaction[['tx_id', 'product_name']], how = "left", on = "tx_id")
customer_detail.set_index(['customer_id'], inplace=True)

product_detail = customer_detail[['product_name','price', 'quantity']].groupby('product_name').sum()

df_original.to_csv('original_clean.csv')
df_customer.to_csv('customer_clean.csv')
df_transaction.to_csv('transaction_clean.csv')
customer_detail.to_csv('customer_detail.csv')
product_detail.to_csv('product_detail.csv')

upload_to_aws('original_clean.csv', bucket, 'original_clean.csv', access_key, secret_key)
upload_to_aws('customer_clean.csv', bucket, 'customer_clean.csv', access_key, secret_key)
upload_to_aws('transaction_clean.csv', bucket, 'transaction_clean.csv', access_key, secret_key)
upload_to_aws('customer_detail.csv', bucket, 'customer_detail.csv', access_key, secret_key)
upload_to_aws('product_detail.csv', bucket, 'product_detail.csv', access_key, secret_key)
