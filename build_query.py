import pandas as pd 
import numpy as np 
import os
import sqlite3
from time import perf_counter

#start_time = time.perf_counter()


FILE = r"C:\Users\Loz\github\auto_generated_results\19139ed52d8840a6b04242428e6a1f23\ETHOSCOPE_191\2020-03-05_17-53-47\2020-03-05_17-53-47_19139ed52d8840a6b04242428e6a1f23.db"

time = []

for i in range(10):

    time_start = perf_counter()

    conn = sqlite3.connect(FILE)
    query = 'SELECT * FROM ROI_1 UNION ALL SELECT * FROM ROI_2 UNION ALL SELECT * FROM ROI_3 UNION ALL SELECT * FROM ROI_4'
    roi_df = pd.read_sql_query(query, conn)
    #print(roi_df.head())

    time_stop = perf_counter()
    time_taken = time_stop - time_start
    time.append(time_taken)

time = np.array(time)
print(np.mean(time))

#print('Elapsed time: {:6.3f} seconds for {:d} rows'.format(time.perf_counter() - start_time, len(roi_df.index)))