import pandas as pd
import numpy as np 
from read_single_roi import read_single_roi

def load_ethoscope(metadata, min_time = 0 , max_time = float('inf'), reference_hour = None, cache =  None):
    """ """  
    if cache != None:
        file_name = 'cached_data_{}_{}.pkl'.format(data['date'].iloc[0], data['time'].iloc[0])
        data = pd.read_pickle(file_name)
        return data

    data = pd.DataFrame()

    # iterate over the ROI of each ethoscope in the metadata df
    for i in range(len(metadata.index)):
        roi_1 = read_single_roi(FILE = metadata['path'][i],
                                region_id = metadata['region_id'][i],
                                min_time = min_time,
                                max_time = max_time,
                                reference_hour = reference_hour
                                )       
        roi_1.insert(0, 'id', metadata['id'][i]) 
        data = data.append(roi_1, ignore_index= True)

    if cache != None:
        file_name = 'cached_data_{}_{}.pkl'.format(data['date'].iloc[0], data['time'].iloc[0])
        data.to_pickle(file_name, cache)
        
    metadata.set_index('id')
    data.set_index('id')

    return metadata, data



    
