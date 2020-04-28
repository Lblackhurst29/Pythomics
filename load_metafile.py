import pandas as pd
import numpy as np 


path = r"C:\Users\Loz\github\Test_DB\2020-03-05_cameratest.csv"

def read_meta(path):
    """ """
    try:
        meta = pd.read_csv(path)
    
    except Exception as e:
        print("An error occurred: ", e)

    meta = meta.drop_duplicates(subset = 'machine_name')
    meta = meta[['machine_name', 'date']]


meta_df = read_meta(path)
print(meta_df)  



