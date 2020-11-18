import pandas as pd
import numpy as np 
import warnings

def check_conform(data, metadata = None):

    def format_Warning(message, category, filename, lineno, line=''):
        return str(filename) + ':' + str(lineno) + ': ' + category.__name__ + ': ' +str(message) + '\n'
    # formats warming method to not double print and allow string formatting
    warnings.formatwarning = format_Warning

    if isinstance(data, pd.DataFrame) is not True:
        warnings.warn('Data input is not a pandas dataframe')
        exit()

    if metadata is not None: 
        if isinstance(metadata, pd.DataFrame) is not True:
            warnings.warn('Metadata input is not a pandas dataframe')
            exit()

        metadata_id_list = metadata.index.tolist()
        data_id_list = set(data.index.tolist())
        # checks if all id's of data are in the metadata dataframe
        check_data = all(elem in metadata_id_list for elem in data_id_list)
        if check_data is not True:
            warnings.warn("There are ID's in the data not in the metadata, please check")
            exit()