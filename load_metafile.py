import pandas as pd 
import os.path

def read_meta(path):
    """ """
    if os.access(path, os.R_OK):
        try:
            meta_df = pd.read_csv(path)
        
        except Exception as e:
            print("An error occurred: ", e)

    else:
        print("Given file path is not readable")

    return meta_df



