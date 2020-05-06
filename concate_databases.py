import sqlite3
import pathlib
import urllib.parse
import pandas as pd
import numpy as np 
import time

from sqlite3 import Error
from link_meta_index import link_meta_index
from load_database import path_to_uri
from load_database import create_connection

meta = r"C:\Users\Loz\github\Test_DB\2020-03-05_cameratest.csv"

index = r"C:\Users\Loz\github\auto_generated_results\index2.txt"

user_dir = r"C:\Users\Loz\github\auto_generated_results"

metadata = link_meta_index(meta, index, user_dir)
metadata_test = metadata.iloc[0:4,:]

start_time = time.perf_counter()

data = pd.DataFrame()

# iterate over the ROI of each ethoscope in the metadata df
for i in range(len(metadata_test.index)):
    # create a URI compatiple path to unifrom input, add rw mode so as not to create db where wrong path provided
    uri_path = path_to_uri(metadata_test['path'][i]) + '?mode_rw'
    # create connection to db using sqlite3
    try: 
        conn = create_connection(uri_path, ethoscope = metadata_test['machine_name'][i], roi = metadata_test['region_id'][i])
        # genertae query using the region_id
        query = 'SELECT * FROM ROI_' + str(metadata['region_id'][i])
        # read query in 10,000 row chunks to reduce load on memory
        chunks = []
        t0 = time.time()
        for chunk in pd.read_sql_query(query, conn, index_col = None, chunksize = 10000):
            chunks.append(chunk)
        t1 = time.time()
        print(t1-t0)
    finally:    
        conn.close()
    t3 = time.time()
    roi = pd.concat(chunks, ignore_index = True)
    t4 = time.time()
    print(t4-t3)
    # replace the id column with the id's given in the metadata df
    roi.loc[:,'id'] = metadata_test['id'][i]
    # append to empty df
    t5 = time.time()
    data = data.append(roi, ignore_index= True)
    t6 = time.time()
    print(t6-t5)

print('Elapsed time: {:6.3f} seconds for {:d} rows'.format(time.perf_counter() - start_time, len(data.index)))
print(data.head())

data.to_pickle('cached_data.pkl')



    
