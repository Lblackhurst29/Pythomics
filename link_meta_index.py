import pandas as pd
import numpy as np 
import ftplib
import os
import io
from validate_datetime import validate_datetime
import sys
from urllib.parse import urlparse

def link_meta_index(metadata, remote_dir, user_dir, index_file = None):
    """Takes a csv metadata file that must contain machine_name and data of experiment
        if index_file == None, the file will be retrieved from the remote directory location
        user directory is the local that the database files are saved following the same directory tree
        as the original remote server
        returns a metadata table containing the csv file information and corresponding path for each entry in the csv """
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
    if index_file is None:
        parse = urlparse(remote_dir)
        ftp = ftplib.FTP(parse.netloc)
        ftp.login()
        ftp.cwd(parse.path)

        download_file = io.BytesIO()
        ftp.retrbinary('RETR ' + 'index.txt', download_file.write)
        download_file.seek(0)
        contents = download_file.read()
        download_file.seek(0)
        db_files = pd.read_csv(download_file, engine='python', header = None)
            
        ftp.quit()
    
    else:
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
        win_path = path_list[j][0][0].replace('/', '\\')
        full_path_list.append(user_dir + "\\" + win_path)
    
    #create a unique id for each row, consists of first 25 char of file_name and region_id, inserted at index 0
    merge_df.insert(0, 'path', full_path_list)
    merge_df.insert(0, 'id', merge_df['file_name'].str.slice(0,26,1) + '|' + merge_df['region_id'].map('{:02d}'.format))
    
    return merge_df

