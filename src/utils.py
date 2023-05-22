import pandas as pd 
import numpy as np
import snowflake.connector
from pathlib import Path
from typing import List, Tuple
from datetime import datetime
import boto3
from botocore.exceptions import NoCredentialsError

def create_connection(account_name, user_name, passkey, warehouse_name):
    
    conn_params = {
        "account": account_name,
        "user": user_name,
        "password": passkey,
        "warehouse": warehouse_name
    }
    
    # connect to snowflake
    conn = snowflake.connector.connect(**conn_params)
    
    # cursor object
    mycursor = conn.cursor()
    
    return conn, mycursor

def upload_to_aws(local_file, bucket, s3_file, access, secret):
    '''
    
    local_file : Local file path as input to the function
    s3_file : Name of the AWS file to be created
    
    '''
    s3 = boto3.client('s3', aws_access_key_id = access, aws_secret_access_key = secret)
    try: 
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False
    
def create_database(name, cursor):
    cursor.execute(f"CREATE OR REPLACE DATABASE {name}")
    cursor.execute(f"CREATE OR REPLACE SCHEMA DEV")            # adding dev schema to the created database
    
def get_schema(df:pd.DataFrame) -> Tuple[str,str]:
    
    # Deterime the SQL data type for each column in the DataFrame
    types = []
    for i in df.dtypes:
        if i == 'datetime64':
            types.append('DATE')
        elif i == 'object':
            types.append('VARCHAR(255)')
        elif i == 'float64':
            types.append('FLOAT')
        elif i == 'int64':
            types.append('INT')
        
    # combine column names and data types into a string of SQL schema
    col_type = list(zip(df.columns.values, types))
    col_type = tuple([" ".join(i) for i in col_type])
    col_type = ", ".join(col_type)
    
    # create a string of placeholder values for SQL queries
    values = ', '.join(['%s' for i in range(len(df.columns))])
    
                                            # returns a string with the combination of column names and their data types based on SQL datatypes
    return col_type, values                 # returns ('%s') corresponding to the number of columns in the dataframe   

def get_data(path:str) -> pd.DataFrame:
    
    # Loads data from a csv file into a Pandas DataFrame
    
    df = pd.read_csv(path)
    # drop the 'Unnamed : 0' column, if it exists
    df.drop(columns = ['Unnamed : 0'], inplace = True, errors = 'ignore')
    return df

def get_df(bucket, file_name, s3):
    '''
    Loads DataFrame from a bucket, provided the file name.
    s3 : boto3.client connection established 
    
    '''
    obj = s3.get_object(Bucket = bucket, Key = file_name)
    df = pd.read_csv(obj['Body'])
    
    # drop the 'Unnamed: 0' column, if it exists
    df.drop(columns = ['Unnamed: 0'], inplace = True, errors = 'ignore')
    return df

    
    
