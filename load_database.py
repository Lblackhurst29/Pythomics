import sqlite3
import pathlib
import urllib.parse
import pandas as pd
import numpy as np 

from sqlite3 import Error

# Location of database **Test**
#path = r"C:\Users\lab\Documents\ethoscope_databases\auto_generated_data\ethoscope_results\sd_data.db"

def path_to_uri(path):
    """ change path to URI compatability
        path = location of database """

    path = pathlib.Path(path)
    if path.is_absolute():
        return path.as_uri()
    return 'file:' + urllib.parse.quote(path.as_posix(), safe=':/')

# Make path read/write only so a database won't be created in error
uri_path = path_to_uri(path) + '?mode=rw'

def create_connection(db_path, ethoscope = None, roi = None):
    """ create a database connection to a SQLite database """
    
    try:
        conn = sqlite3.connect(db_path, uri = True)
        if ethoscope == None:
            print("Loading data")
        else:
            print("Loading data from " + str(ethoscope) + ": ROI number " + str(roi))
    
    except Error as e:
        print("An error occurred: ", e)  
    
    return conn   
 
#if __name__ == '__main__':
    #con = create_connection(uri_path)

    
 
 