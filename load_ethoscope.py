import pandas as pd
import numpy as np 
import os.path
from read_single_roi import read_single_roi
from sleep_annotation import sleep_annotation

def load_ethoscope(metadata, min_time = 0 , max_time = float('inf'), reference_hour = None, cache =  False, FUN = None):
    """metadata = metadata df returned from link_meta_index function"""  
    if cache is True:
        file_name = 'cached_data_{}_{}.pkl'.format(metadata['date'].iloc[0], metadata['time'].iloc[0])
        if os.path.exists(file_name) is True:
            data = pd.read_pickle(file_name)
            return data

    data = pd.DataFrame()

    # iterate over the ROI of each ethoscope in the metadata df
    for i in range(len(metadata.index)):
        print('Loading ROI_{} from {}'.format(metadata['region_id'][i], metadata['machine_name'][i]))
        roi_1 = read_single_roi(FILE = metadata['path'][i],
                                region_id = metadata['region_id'][i],
                                min_time = min_time,
                                max_time = max_time,
                                reference_hour = reference_hour,
                                FUN = FUN
                                )

        if roi_1 is None:
            print('ROI_{} from {} was unable to load'.format(metadata['region_id'][i], metadata['machine_name'][i]))
            continue      
        roi_1.insert(0, 'id', metadata['id'][i]) 
        data = data.append(roi_1, ignore_index= True)

    if cache is True:
        file_name = 'cached_data_{}_{}.pkl'.format(metadata['date'].iloc[0], metadata['time'].iloc[0])
        data.to_pickle(file_name)

    return data

#metadata = pd.read_pickle('cached_metadata.pkl')
#data = load_ethoscope(metadata, reference_hour=9.0, cache = False)
#data.to_pickle('cached_data_2020-03-05_17-53-47_no_sleep.pkl')


#print(metadata)
#print(metadata['path'][23])
#print(metadata['region_id'][23])
#print(read_single_roi(FILE = metadata['path'][22],
#                                region_id = metadata['region_id'][22],
 #                               min_time = 0,
 #                               max_time = float('inf'),
 #                               reference_hour = 9.0,
 #                               FUN = sleep_annotation
#                                ))  


    
