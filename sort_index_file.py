import pandas as pd
import numpy as np 

pd.options.display.max_colwidth = 200

data = r"C:\Users\Loz\github\auto_generated_results\index2.txt"

path = r"C:\Users\Loz\github\Test_DB\2020-03-05_cameratest.csv"

def read_meta(path):
    """ """
    try:
        meta = pd.read_csv(path)
    
    except Exception as e:
        print("An error occurred: ", e)

    meta = meta.drop_duplicates(subset = 'machine_name', ignore_index = True)
    meta = meta[['machine_name', 'date']]

    return(meta)


def read_index(index_file):
    """ """
    #read, isolate .db files and retain path column, ie. first column
    db_files = pd.read_csv(index_file, header = None)
    db_files = db_files[db_files[0].str.endswith(".db")]
    db_files = db_files.iloc[:,0]

    #split the series using '\', convert to pd df
    split_db_files = pd.DataFrame(db_files.str.split("\\").tolist(), columns = ["machine_id", "machine_name","date/time","file_name"])

    #split the date_time column and add back to df
    date_time = split_db_files[["date/time"]]
    date_time = date_time["date/time"].str.split("_", expand = True)
    split_db_files["date"] = date_time[0]
    split_db_files["time"] = date_time[1]
    split_db_files.drop(columns = ["date/time"], inplace = True)

    return(split_db_files)

# merge the meta and index df to find required db, cross-reference to index file and produce list 
# of database paths
meta_df = read_meta(path)
index_df = read_index(data)

def merge_files(index, meta):
    """ """
    merge_df = pd.merge(index, meta)

    path_name = merge_df[['file_name']]

    path_name = path_name['file_name'].values.tolist()
    db_files = pd.read_csv(data, header = None)
    db_files = db_files[db_files[0].str.endswith(".db")]
    db_files = pd.DataFrame(db_files.iloc[:,0])

    path_list = []

    for i in path_name:
        path_list.append(db_files[db_files[0].str.contains(i)].values.tolist())

    return path_list

path_list = merge_files(index_df, meta_df)

#join the db path name with the users directory 
inital_path = r"C:\Users\Loz\github\auto_generated_results"
full_path_list = []

for i in range(len(path_list)):
    full_path_list.append(inital_path + "\\" + path_list[i][0][0])

print(full_path_list[0])
print(full_path_list[1])
print(full_path_list[2])
print(full_path_list[3])

