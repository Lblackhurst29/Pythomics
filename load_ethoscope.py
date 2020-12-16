import pandas as pd
import numpy as np 
import os.path
from read_single_roi import read_single_roi

pd.options.mode.chained_assignment = None

def load_ethoscope(metadata, min_time = 0 , max_time = float('inf'), reference_hour = None, cache = None, FUN = None):
    """metadata = metadata df returned from link_meta_index function"""  

    data = pd.DataFrame()

    # iterate over the ROI of each ethoscope in the metadata df
    for i in range(len(metadata.index)):
        print('Loading ROI_{} from {}'.format(metadata['region_id'][i], metadata['machine_name'][i]))
        roi_1 = read_single_roi(file = metadata.iloc[i,:],
                                min_time = min_time,
                                max_time = max_time,
                                reference_hour = reference_hour,
                                cache = cache
                                )

        if roi_1 is None:
            print('ROI_{} from {} was unable to load'.format(metadata['region_id'][i], metadata['machine_name'][i]))
            continue

        if FUN is not None:
            if 'has_interacted' not in roi_1.columns:
                roi_1 = FUN(roi_1, masking_duration = 0)

            else:
                roi_1 = FUN(roi_1) 

        if roi_1 is None:
            print('ROI_{} from {} was unable to load'.format(metadata['region_id'][i], metadata['machine_name'][i]))
            continue

        roi_1.insert(0, 'id', metadata['id'][i]) 
        data = data.append(roi_1, ignore_index= True)

    return data
