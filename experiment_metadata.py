import sqlite3
import pathlib
import urllib.parse
import pandas as pd

from sqlite3 import Error

# Location of database **Test**
db = r"C:\Users\Loz\github\auto_generated_results\19139ed52d8840a6b04242428e6a1f23\ETHOSCOPE_191\2020-03-05_17-53-47\2020-03-05_17-53-47_19139ed52d8840a6b04242428e6a1f23.db"

def experiment_metadata(FILE):
    """ Returns the metadata details from the METADATA table in
        the given .db
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
            print("Connection successful")
        
        except Error as e:
            print("An error occurred: ", e)  
        
        return conn   

    con = create_connection(path)

    df = pd.read_sql_query('SELECT * FROM METADATA', con)
    print(df)

experiment_metadata(db)