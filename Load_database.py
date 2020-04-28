import sqlite3
import pathlib
import urllib.parse
import pandas as pd
import numpy as np 

from sqlite3 import Error

# Location of database **Test**
path = r"C:\Users\Loz\github\auto_generated_results\19139ed52d8840a6b04242428e6a1f23\ETHOSCOPE_191\2020-03-05_17-53-47\2020-03-05_17-53-47_19139ed52d8840a6b04242428e6a1f23.db"

def path_to_uri(path):
    """ change path to URI compatability
        path = location of database """

    path = pathlib.Path(path)
    if path.is_absolute():
        return path.as_uri()
    return 'file:' + urllib.parse.quote(path.as_posix(), safe=':/')

# Make path read/write only so a database won't be created in error
path = path_to_uri(path) + '?mode=rw'

def create_connection(db_file):
    """ create a database connection to a SQLite database """
    
    try:
        conn = sqlite3.connect(path, uri = True)
        print("Connection successful")
    
    except Error as e:
        print("An error occurred: ", e)  
    
    return conn   
 
if __name__ == '__main__':
    con = create_connection(path)

df = pd.read_sql_query('SELECT * table_name FROM database_name', con) #index_col = 'roi_idx')
print(df.head())

con.close()
    
 
 