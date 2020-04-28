import pandas as pd
import numpy as np 
from load_metafile import read_meta

pd.options.display.max_colwidth = 50

def list_paths(metadata, index_file, user_dir):
    """ """
    #load metadata csv file
    meta_df = read_meta(metadata)

    #read, isolate .db files and retain path column, ie. first column
    db_files = pd.read_csv(index_file, header = None)
    db_files = db_files[db_files[0].str.endswith(".db")]
    db_files = db_files.iloc[:,0]

    #split the series using '\', convert to a pd_df
    split_db_files = pd.DataFrame(db_files.str.split("\\").tolist(), columns = ["machine_id", "machine_name","date/time","file_name"])

    #split the date_time column and add back to df
    date_time = split_db_files[["date/time"]]
    date_time = date_time["date/time"].str.split("_", expand = True)
    split_db_files["date"] = date_time[0]
    split_db_files["time"] = date_time[1]
    split_db_files.drop(columns = ["date/time"], inplace = True)
    

    # merge df's and isolate file_name column as the identifer 
    merge_df = pd.merge(split_db_files, meta_df)
    path_name = merge_df[['file_name']]

    # convert df to list and cross-reference to 'index' csv/txt to find stored paths
    path_name = path_name['file_name'].values.tolist()
    db_files = pd.read_csv(index_file, header = None)
    db_files = db_files[db_files[0].str.endswith(".db")]
    db_files = pd.DataFrame(db_files.iloc[:,0])

    # intialise path_list and populate with paths from previous
    path_list = []
    for i in path_name:
        path_list.append(db_files[db_files[0].str.contains(i)].values.tolist())

    #join the db path name with the users directory 
    full_path_list = []
    for j in range(len(path_list)):
        full_path_list.append(user_dir + "\\" + path_list[j][0][0])
    
    merge_df.insert(0, 'path', full_path_list)
    merge_df.insert(0, 'id', merge_df['file_name'].str.slice(0,26,1) + '|' + merge_df['region_id'].map('{:02d}'.format))
    
    return merge_df

meta = r"C:\Users\Loz\github\Test_DB\2020-03-05_cameratest.csv"

index = r"C:\Users\Loz\github\auto_generated_results\index2.txt"

user_dir = r"C:\Users\Loz\github\auto_generated_results"

print(list_paths(meta, index, user_dir))
