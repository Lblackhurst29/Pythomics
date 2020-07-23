import ftplib
import os
import pandas as pd 
import numpy as np
import sys

from datetime import datetime
from validate_datetime import validate_datetime
from functools import partial
from urllib.parse import urlparse

# example locations
# meta = r"C:\Users\Loz\github\Test_DB\2020-03-05_cameratest.csv"
# index = r"C:\Users\Loz\github\auto_generated_results\index.txt"
# local_direc = r"C:\Users\Loz\github"
# remote_direc = 'ftp://nas.lab.gilest.ro/auto_generated_data/ethoscope_results/'

def download_from_remote_dir(meta, index, remote_dir, local_dir):
    """ Takes metadata csv file and cross references the ethoscope name and date/time with those in the index, matched ethoscope data will
        be downloaded from the remote server and saved locally """

    #check csv path is real and read to pandas df
    if os.access(meta, os.R_OK):
        try:
            meta_df = pd.read_csv(meta)         
        except Exception as e:
            print("An error occurred: ", e)
    else:
        sys.exit("File path is not readable")

    # check and tidy df, removing un-needed columns and duplicated machine names
    if 'machine_name' not in meta_df.columns and 'date' not in meta_df.columns:
        sys.exit("Column(s) 'machine_name' and/or 'date' missing from metadata file")
    meta_df = meta_df[['machine_name', 'date']]
    meta_df.drop_duplicates(subset = ['machine_name'], keep = 'first', inplace = True, ignore_index = False)

    #check the date format is YYYY-MM-DD, without this format the df merge will return empty
    validate_datetime(meta_df)

    # can add in a time criteria soon
    #if col_names.count('time') == 0:
        #meta_df['time'] = np.nan

    def split_index(index_file):
        """ """
        #read, isolate .db files and retain path column, ie. first column
        db_files = pd.read_csv(index_file, header = None)
        db_files = db_files[db_files[0].str.endswith(".db")]
        db_files = db_files.iloc[:,0]
        db_files_index = db_files.index.tolist()

        #split the series using '\', convert to a pd_df
        split_db_files = pd.DataFrame(db_files.str.split("/").tolist(), columns = ["machine_id", "machine_name","date/time","file_name"])
        #split the date_time column and add back to df
        date_time = split_db_files[["date/time"]]
        date_time = date_time["date/time"].str.split("_", expand = True)
        split_db_files["date"] = date_time[0]
        split_db_files["time"] = date_time[1]
        split_db_files.drop(columns = ["date/time"], inplace = True)
        split_db_files['copy_index'] = db_files_index
        split_db_files.set_index('copy_index', inplace = True)
        return split_db_files

    #if index != None:
    index_df = split_index(index) 
   
        
    # merge df's on the machine_name and date columns to find subset of .db
    merge_df = pd.merge(index_df, meta_df, on = ['machine_name', 'date'], right_index = True)

    # retain index for use later
    path_index = merge_df.index.tolist()
    index_files = pd.read_csv(index, header = None)
    index_files = index_files[index_files[0].str.endswith(".db")]
    paths = index_files.loc[path_index, 0].tolist()

    # split path into path and file name
    basename = lambda x: os.path.split(x)
    list_basenames = list(map(basename, paths)); list_basenames


    def grabFile(remote_dir, work_dir, local_dir, file_name):
        """ Connects to remote FTP server and saves to desginated local path, retains file name """

        ftp = ftplib.FTP(remote_dir)
        ftp.login()
        ftp.cwd(work_dir)

        path = os.path.join(local_dir, file_name)
        localfile = open(path, 'wb')
        ftp.retrbinary('RETR ' + file_name, localfile.write)
            
        ftp.quit()
        localfile.close()

    # call grabFile function with remote_dir and local_dir 
    # iterate through nested list
    parse = urlparse(remote_dir)
    download = partial(grabFile, remote_dir=parse.netloc, local_dir=local_dir)
    for j in list_basenames:
        download(work_dir=parse.path+j[0], file_name=j[1])
    
    
