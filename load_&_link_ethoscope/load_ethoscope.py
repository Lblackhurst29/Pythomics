import pandas as pd
import numpy as np 
import os.path
from read_single_roi import read_single_roi

def load_ethoscope(metadata, min_time = 0 , max_time = float('inf'), reference_hour = None, cache =  False, FUN = None):
    """ """  
    if cache is True:
        file_name = 'cached_data_{}_{}.pkl'.format(metadata['date'].iloc[0], metadata['time'].iloc[0])
        if os.path.exists(file_name) is True:
            data = pd.read_pickle(file_name)
            return data

    data = pd.DataFrame()

    # iterate over the ROI of each ethoscope in the metadata df
    for i in range(len(metadata.index)):
        roi_1 = read_single_roi(FILE = metadata['path'][i],
                                region_id = metadata['region_id'][i],
                                min_time = min_time,
                                max_time = max_time,
                                reference_hour = reference_hour,
                                FUN = FUN
                                )       
        roi_1.insert(0, 'id', metadata['id'][i]) 
        data = data.append(roi_1, ignore_index= True)

    if cache is True:
        file_name = 'cached_data_{}_{}.pkl'.format(metadata['date'].iloc[0], metadata['time'].iloc[0])
        data.to_pickle(file_name)

    return data



    
