import sqlite3
import pathlib
import urllib.parse
import pandas as pd
import numpy as np 

from sqlite3 import Error

# Location of database **Test**
path = r"C:\Users\Loz\github\Test DB\2016-01-25_21-46-14_014.db"

def _path_to_uri(path):
    """ change path to URI compatability
        path = location of database """
    path = pathlib.Path(path)
    if path.is_absolute():
        return path.as_uri()
    return 'file:' + urllib.parse.quote(path.as_posix(), safe=':/')

# Make path read/write only so a database won't be created in error
path = _path_to_uri(path) + '?mode=rw'

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

cur = con.cursor()

cur.execute("select * from ROI_4")

print(cur.fetchall())

con.close()
    
 
 