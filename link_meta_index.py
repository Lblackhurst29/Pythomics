import pandas as pd
import numpy as np 
import os
from validate_datetime import validate_datetime
import sys

def link_meta_index(metadata, index_file, user_dir):
    """ """
    #load metadata csv file
    #check csv path is real and read to pandas df
    if os.access(metadata, os.R_OK):
        try:
            meta_df = pd.read_csv(metadata)  
                  
        except Exception as e:
            print("An error occurred: ", e)
    else:
        sys.exit("File path is not readable")
    
    meta_df = validate_datetime(meta_df)

    #read, isolate .db files and retain path column, ie. first column
    db_files = pd.read_csv(index_file, header = None)
    db_files = db_files[0][db_files[0].str.endswith(".db")]

    #split the series using '\', convert to a pd_df
    split_db_files = db_files.str.split('/', expand = True)
    split_db_files.columns = ['machine_id', 'machine_name', 'date_time', 'file_name'] 
 
    #split the date_time column and add back to df
    split_db_files[['date', 'time']] = split_db_files.date_time.str.split('_', expand = True)
    split_db_files.drop(columns = ["date_time"], inplace = True)

    # merge df's 
    merge_df = pd.merge(split_db_files, meta_df)

    # convert df to list and cross-reference to 'index' csv/txt to find stored paths
    path_name = merge_df['file_name'].values.tolist()
    db_files = pd.DataFrame(db_files)

    # intialise path_list and populate with paths from previous
    path_list = []
    for path in path_name:
        path_list.append(db_files[db_files[0].str.contains(path)].values.tolist())


    #join the db path name with the users directory 
    full_path_list = []
    for j in range(len(path_list)):
        full_path_list.append(user_dir + "\\" + path_list[j][0][0])
    
    #create a unique id for each row, consists of first 25 char of file_name and region_id, inserted at index 0
    merge_df.insert(0, 'path', full_path_list)
    merge_df.insert(0, 'id', merge_df['file_name'].str.slice(0,26,1) + '|' + merge_df['region_id'].map('{:02d}'.format))
    
    return merge_df

#meta = r"C:\Users\Loz\github\Test_DB\2020-03-05_cameratest.csv"
#index = r"C:\Users\Loz\github\auto_generated_results\index.txt"
#user = r"C:\Users\Loz\github\auto_generated_results"


