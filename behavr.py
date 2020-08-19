import pandas as pd
import numpy as np 
import warnings
from check_conform import check_conform
from behavp_class import Behavpy
from format_warnings import format_Warning

def set_behavpy(metadata, data):
    """ Takes two data frames, one metadata and the other the recorded values.
        Both added as attributes of an empty class 'behavpy' for easier sorting filtering
        must both contain an 'id' column with matching ids """
        
    warnings.formatwarning = format_Warning


    if metadata.index.name != 'id':
        try:
            metadata.set_index('id', inplace = True)
        except:
            warnings.warn('There is no "id" as a column or index in the metadata')
            exit()

    if data.index.name != 'id':
        try:
            data.set_index('id', inplace = True)
        except:
            warnings.warn('There is no "id" as a column or index in the data')
            exit()

    check_conform(data, metadata)

    if 'path' in metadata.columns:
        metadata.drop('path', axis = 1, inplace = True)
    if 'file_name' in metadata.columns:
        metadata.drop('file_name', axis = 1, inplace = True)

    df = Behavpy(metadata, data)

    return df


    