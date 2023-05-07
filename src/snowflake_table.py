from utils import (create_connection, get_schema, get_df) 
import pandas
import numpy
import matplotlib
import boto3
import os
from botocore.exceptions import NoCredentialsError

account = os.environ.get('account_name')
user_key = os.environ.get('user_name')
pass_key = os.environ.get('pass_key')
warehouse = os.environ.get('warehouse')

access_key = os.environ.get('access')
secret_key = os.environ.get('secret')
bucket = os.environ.get('bucket')

s3 = boto3.client('s3', aws_access_key_id = access_key, aws_secret_access_key = secret_key)

df_original = get_df(bucket, 'original_clean.csv', s3)
df_customer = get_df(bucket, 'customer_clean.csv', s3)
df_transaction = get_df(bucket, 'transaction_clean.csv', s3)
df_product_details = get_df(bucket, 'product_detail.csv', s3)
df_customer_details = get_df(bucket, 'customer_detail.csv', s3)

def create_s3_snowflake_table(account_name, user_name, password, warehouse_name, df, table_name):
  conn, cursor = create_connection(account, user_name, password, warehouse_name)
  
  df.drop(columns=['Unnamed: 0'], inplace=True, errors = 'ignore')
  col_type, values = get_schema(df)
  
  cursor.execute("USE clv")
  cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
  cursor.execute(f"CREATE TABLE {table_name} ({col_type})")
  
#  for index, row in df.iterrows():
#    sql = f"INSERT INTO {table_name} VALUES ({values})"
#    cursor.execute(sql, tuple(row))
#    conn.commit() 
  
  conn.commit()
    
  cursor.close()
  conn.close()
  
  
create_s3_snowflake_table(account, user_key, pass_key, warehouse, df_original, "ORIGINAL")

create_s3_snowflake_table(account, user_key, pass_key, warehouse, df_customer, "CUSTOMER")

create_s3_snowflake_table(account, user_key, pass_key, warehouse, df_transaction, "TRANSACTION")

create_s3_snowflake_table(account, user_key, pass_key, warehouse, df_product_details, "PRODUCT_DETAILS")

create_s3_snowflake_table(account, user_key, pass_key, warehouse, df_customer_details, "CUSTOMER_DETAILS")
