import pandas as pd
import time
import datetime
import sqlite3
from load_database import path_to_uri
from load_database import create_connection

path = r"C:\Users\Loz\github\auto_generated_results\19139ed52d8840a6b04242428e6a1f23\ETHOSCOPE_191\2020-03-05_17-53-47\2020-03-05_17-53-47_19139ed52d8840a6b04242428e6a1f23.db"

def experiment_metadata(FILE):
    """ Returns the metadata details from the METADATA table in
        the given .db, parse date_time to GMT

        FILE = local location of database """
    conn = sqlite3.connect(FILE)
    exp_meta = pd.read_sql_query('SELECT * FROM METADATA', conn)
    conn.close()

    # isolate date_time string and parse to GMT with format YYYY-MM-DD HH-MM-SS
    exp_meta['value'].loc[exp_meta['field'] == 'date_time'] = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(float(exp_meta['value'].loc[exp_meta['field'] == 'date_time'])))
    
    return exp_meta