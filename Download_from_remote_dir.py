import ftplib
import os
import io
import pandas as pd 
import numpy as np
import sys
import errno
import time 

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

        #split the series using '\', convert to a pd_df
        split_db_files = index_files[0].str.split('/', n = 3, expand = True)
        split_db_files.columns = ['machine_id', 'machine_name', 'date_time', 'file_name']
        split_db_files['file_size'] = index_files[1]
        
        #split the date_time column
        split_db_files[['date', 'time']] = split_db_files.date_time.str.split('_', expand = True)
        split_db_files.drop(columns = ["date_time"], inplace = True)
        return split_db_files, index_files

    index_df, index_files = split_index(index) 

    # merge df's on the machine_name and date columns to find subset of .db
    merge_df = pd.merge(index_df, meta_df, on = ['machine_name', 'date'], right_index = True)

    # retain index for use later
    path_index = merge_df.index.tolist()
    paths = np.array(index_files.loc[path_index, :].apply(list), dtype = 'object')

    def download_database(remote_dir, work_dir, local_dir, file_name, file_size):
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

        win_path = (local_dir + work_dir.replace('/', '\\'))
        file_path = os.path.join(win_path, file_name)

        if os.access(file_path, os.R_OK):
            if os.path.getsize(file_path) < file_size:
                ftp = ftplib.FTP(remote_dir)
                ftp.login()
                ftp.cwd(work_dir)

                localfile = open(file_path, 'wb')
                ftp.retrbinary('RETR ' + file_name, localfile.write)
                    
                ftp.quit()
                localfile.close()

        else:
            ftp = ftplib.FTP(remote_dir)
            ftp.login()
            ftp.cwd(work_dir)

            localfile = open(file_path, 'wb')
            ftp.retrbinary('RETR ' + file_name, localfile.write)
                
            ftp.quit()
            localfile.close()

    # call grabFile function with remote_dir and local_dir 
    # iterate through nested list
    parse = urlparse(remote_dir)
    download = partial(download_database, remote_dir=parse.netloc, local_dir=local_dir)
    counter = 1
    times = np.array([])

    for j in paths:
        print('Downloading {}... {}/{}'.format(j[0].split('/')[1], counter, len(merge_df)))
        if counter == 1:
            start = time.time()
            p = os.path.split(j[0])
            download(work_dir = parse.path+p[0], file_name = p[1], file_size = j[1])
            stop = time.time()
            t = stop - start
            times = np.append(times, t)
            counter += 1
        else:
            av_time = round((times.mean()/60) * len(merge_df))
            print('Estimated finish time: {} mins'.format(av_time)) 
            start = time.time()
            p = os.path.split(j[0])
            download(work_dir=parse.path+p[0], file_name=p[1], file_size = j[1])
            stop = time.time()
            t = stop - start
            times = np.append(times, t)
            counter += 1