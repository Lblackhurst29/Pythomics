import pandas as pd
import numpy as np 
import os
from load_metafile import read_meta
from datetime import datetime
import sys

def link_meta_index(metadata, index_file, user_dir):
    """ """
    #load metadata csv file
    def read_meta(path):
        """ check csv path is real and read to pandas df"""
        if os.access(path, os.R_OK):
            try:
                meta_df = pd.read_csv(path)         
            except Exception as e:
                print("An error occurred: ", e)
        else:
            print("File path is not readable")

        return meta_df

    meta_df = read_meta(metadata)

    #check the date format is YYYY-MM-DD, without this format the df merge will return empty
    def validate_datetime(dataframe):
        """ Checks the date column of a csv file for the format YYYY-MM-DD, corrects formats DD-MM-YYYY
            and DD/MM/YYYY, raise error message for other formats
            Args: csv file containing a column tited 'data' """
        date_list = dataframe['date'].values.tolist()
        new_date_list = []
        for i, date in enumerate(date_list):
                try:
                    date == datetime.strptime(date, '%Y-%m-%d').strftime('%Y-%m-%d')
                except ValueError:
                    try:
                        if date == datetime.strptime(date, '%d-%m-%Y').strftime('%d-%m-%Y'):
                            date = datetime.strptime(date, '%d-%m-%Y').strftime('%Y-%m-%d')
                            new_date_list.append(date)
                    except ValueError:
                        try:
                            if date == datetime.strptime(date, '%d/%m/%Y').strftime('%d/%m/%Y'):
                                date = datetime.strptime(date, '%d/%m/%Y').strftime('%Y-%m-%d')
                                new_date_list.append(date)
                        except ValueError:
                            sys.exit("Incorrect data format, should be YYYY-MM-DD for row: " + str(i+1))
        if len(new_date_list) == 0:
            return dataframe
        else:
            dataframe['date'] = new_date_list   
            return dataframe

    
    meta_df = validate_datetime(meta_df)

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
    
    #create a unique id for each row, consists of first 25 char of file_name and region_id, inserted at index 0
    merge_df.insert(0, 'path', full_path_list)
    merge_df.insert(0, 'id', merge_df['file_name'].str.slice(0,26,1) + '|' + merge_df['region_id'].map('{:02d}'.format))
    
    return merge_df
