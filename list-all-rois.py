import sqlite3
import pathlib
import urllib.parse
import pandas as pd
import numpy as np 
import time

from sqlite3 import Error

# Location of database **Test**

def list_all_rois(FILE):
    """ Returns the number of regions of interest from the queried database file
        FILE = local location of database """
    
    def _path_to_uri(path):
        """ change path to URI compatability
            path = location of database """

        path = pathlib.Path(path)
        if path.is_absolute():
            return path.as_uri()
        return 'file:' + urllib.parse.quote(path.as_posix(), safe=':/')

    # Make path read/write only so a database won't be created in error
    path = _path_to_uri(FILE) + '?mode=rw'

    def create_connection(db_file):
        """ create a database connection to a SQLite database """
        
        try:
            conn = sqlite3.connect(path, uri = True)
        
        except Error as e:
            print("An error occurred: ", e)  
        
        return conn   

    con = create_connection(path)
    con.row_factory = lambda cursor, row: row[0]
    roi = con.execute('SELECT roi_value FROM ROI_MAP').fetchall()

    return roi