import pandas as pd
import numpy as np
import argparse
import snowflake.connector
from pathlib import Path
from typing import List, Tuple
from datetime import datetime
import boto3
from utils import (create_connection, upload_to_aws, create_database, 
                   get_schema, get_data)
from botocore.exceptions import NoCredentialsError
import os
from dotenv import load_dotenv
from snowflake_connector import DataExtractor as snowflake

load_dotenv()

account = os.environ.get('account_name')
user_key = os.environ.get('user_name')
pass_key = os.environ.get('pass_key')
warehouse = os.environ.get('warehouse')

access_key = os.environ.get('access')
secret_key = os.environ.get('secret')
bucket = os.environ.get('bucket')

parser = argparse.ArgumentParser()
parser.add_argument('-db', '--create_db', default = False, type = bool)
parser.add_argument('-t', '--create_table', type = str)
parser.add_argument('-i', '--input_data', type = str)
parser.add_argument('-s3', '--upload_to_s3', type = str)
parser.add_argument('-si', '--s3_input', type = str)

args = parser.parse_args()
    
if args.create_db:
    conn, cursor = create_connection(account, user_key, pass_key, warehouse)
    create_database('clv', cursor)
    
if args.upload_to_s3:
    file_name = args.upload_to_s3
    csv_path = args.s3_input
    upload_to_aws(csv_path, bucket, file_name, access_key, secret_key)
    
if args.create_table: 
    conn, cursor = create_connection(account, user_key, pass_key, warehouse)
    csv_path = args.input_data
    table = args.create_table
    
    df = get_data(csv_path)
    df.drop(columns=['Unnamed: 0'], inplace=True, errors = 'ignore')
    col_type, values = get_schema(df)
    print(col_type, values)
    
    cursor.execute("USE clv")
    cursor.execute(f"DROP TABLE IF EXISTS {table}")
    cursor.execute(f"CREATE TABLE {table} ({col_type})")      
    
    for index, row in df.iterrows():
      success, n_rows = snowflake().put(data = df, table_name = table)
    #     sql = f"INSERT INTO {table_name} VALUES ({values})"
    #     cursor.execute(sql, tuple(row))
    #     conn.commit()
    
        
    cursor.close()
    conn.close()
