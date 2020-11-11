import ftplib
import os
import io
import pandas as pd 
import numpy as np
import sys
import errno

from datetime import datetime
from validate_datetime import validate_datetime
from functools import partial
from urllib.parse import urlparse

def download_from_remote_dir(meta, remote_dir, local_dir, index = None):
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
    if 'machine_name' not in meta_df.columns or 'date' not in meta_df.columns:
        sys.exit("Column(s) 'machine_name' and/or 'date' missing from metadata file")
    meta_df = meta_df[['machine_name', 'date']]
    meta_df.drop_duplicates(subset = ['machine_name'], keep = 'first', inplace = True, ignore_index = False)

    # check the date format is YYYY-MM-DD, without this format the df merge will return empty
    # will correct to YYYY-MM-DD in a select few cases
    validate_datetime(meta_df)

    # can add in a time criteria soon
    #if col_names.count('time') == 0:
        #meta_df['time'] = np.nan

    def split_index(index_file):
        """if index_file = None it will search the remote directory for a index.txt file to read
        The paths in the index file are split into their contituents and date_time seperated"""
        #read, isolate .db files and retain path column, ie. first column
        if index is None:
            parse = urlparse(remote_dir)
            ftp = ftplib.FTP(parse.netloc)
            ftp.login()
            ftp.cwd(parse.path)

            download_file = io.BytesIO()
            ftp.retrbinary('RETR ' + 'index.txt', download_file.write)
            download_file.seek(0)
            contents = download_file.read()
            download_file.seek(0)
            index_files = pd.read_csv(download_file, engine='python', header = None)
                
            ftp.quit()

        else:
            index_files = pd.read_csv(index_file, header = None)
        
        db_files = index_files[0][index_files[0].str.endswith(".db")]

        #split the series using '\', convert to a pd_df
        split_db_files = db_files.str.split('/', n = 3, expand = True)
        split_db_files.columns = ['machine_id', 'machine_name', 'date_time', 'file_name']
        
        #split the date_time column
        split_db_files[['date', 'time']] = split_db_files.date_time.str.split('_', expand = True)
        split_db_files.drop(columns = ["date_time"], inplace = True)
        return split_db_files, index_files

    index_df, index_files = split_index(index) 

    # merge df's on the machine_name and date columns to find subset of .db
    merge_df = pd.merge(index_df, meta_df, on = ['machine_name', 'date'], right_index = True)

    # retain index for use later
    path_index = merge_df.index.tolist()
    index_files = index_files[index_files[0].str.endswith(".db")]
    paths = index_files.loc[path_index, 0].tolist()

    # split path into path and file name
    basename = lambda x: os.path.split(x)
    list_basenames = list(map(basename, paths)); list_basenames


    def download_database(remote_dir, work_dir, local_dir, file_name):
        """ Connects to remote FTP server and saves to designated local path, retains file name """
        
        #create local copy of directory tree from ftp server
        os.chdir(local_dir)
        path = (local_dir + work_dir)
        try:
            os.makedirs(path)
        except OSError as exc:
            if exc.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else:
                raise

        ftp = ftplib.FTP(remote_dir)
        ftp.login()
        ftp.cwd(work_dir)

        win_path = (local_dir + work_dir.replace('/', '\\'))
        file_path = os.path.join(win_path, file_name)
        localfile = open(file_path, 'wb')
        ftp.retrbinary('RETR ' + file_name, localfile.write)
            
        ftp.quit()
        localfile.close()

    # call grabFile function with remote_dir and local_dir 
    # iterate through nested list
    parse = urlparse(remote_dir)
    download = partial(download_database, remote_dir=parse.netloc, local_dir=local_dir)
    for j in list_basenames:
        download(work_dir=parse.path+j[0], file_name=j[1])
    
    
