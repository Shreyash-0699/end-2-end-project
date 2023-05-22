# to handle database connection 
import snowflake.connector
import os 
from snowflake.connector.pandas_tools import write_pandas

class DataExtractor: 
  """
  Extract data from snowflake table 
  """ 
  def __init__(self):
    """
    
    Parameters 
    ----------
    table_name: TYPE
      DESCRIPTION.
    column_list: TYPE
      DESCRIPTION.
    
    Returns 
    -------
    None. 
    
    """
    
    # Connection string for database
    self.ctx = snowflake.connector.connect(
      user = os.environ.get('user_name')
      password = os.environ.get('pass_key')
      account = os.environ.get('account_name')
      warehouse = os.environ.get('warehouse')
      database = os.environ.get('database_name')
      schema = os.environ.get('schema_name')
    )
    
  def get(self, table_name, column_name):
    
    query = '''SELECT {} FROM {}'''.format(', '.join(column_name), table_name)
    try: 
      cs = self.ctx.cursor()
      data = cs.execute(query).fetch_pandas_all()
      cs.close()
      self.ctx.close()
      return data
    except:
      print("Error in query execution")
      
  def execute_query(self, query):
    """ 
    
    Parameters 
    ----------
    query: TYPE 
      DESCRIPTION. 
      
    Returns
    -------
    None. 
    
    """ 
    cs = self.ctx.cursor()
    data = cs.execute(query).fetch_pandas_all()
    cs.close()
    self.ctx.close()
    return data
  
  def put(self, data, table_name):
    """ 
    to write the data into snowflake
    """ 
    success, nchunks, nrows, _ = write_pandas(self.ctx, data, table_name)
    return success, nrows
  
  
