import pandas as pd
import numpy as np 
from motion_detectors import max_velocity_detector
from rle import rle


def sleep_annotation(data, 
                     time_window_length = 10,
                     min_time_immobile = 300,
                     motion_detector_FUN = max_velocity_detector,
                     masking_duration = 6
                     ):
    """ """
    #columns_to_keep = ['t', 'x', 'y', 'max_velocity', 'interactions',
                       #'beam_crosses', 'moving','asleep', 'is_interpolated']
    
    if len(data.index) < 100:
        return None
    
    d_small = motion_detector_FUN(data, time_window_length, masking_duration = masking_duration)

    if len(d_small.index) < 100:
        return None

    time_map = pd.Series(range(d_small.t.iloc[0], 
                           d_small.t.iloc[-1] + time_window_length, 
                           time_window_length
                           ), name = 't')

    missing_values = time_map[~time_map.isin(d_small['t'].tolist())]
    d_small = d_small.merge(time_map, how = 'right', on = 't', copy = False).sort_values(by=['t'])
    d_small['is_interpolated'] = np.where(d_small['t'].isin(missing_values), True, False)
    d_small['moving'] = np.where(d_small['is_interpolated'] == True, False, d_small['moving'])


    def sleep_contiguous(moving, fs, min_valid_time = 300):
        """ fs = sampling frequency (Hz), min_valid_time = min amount immobile time that counts as sleep (i.e 5 mins) """
        min_len = fs * min_valid_time
        r_sleep =  rle(np.logical_not(moving)) 
        valid_runs = r_sleep[2] >= min_len 
        r_sleep_mod = valid_runs & r_sleep[0]
        r_small = []
        counter = 0
        for i in r_sleep_mod:
            r_small += ([i] * r_sleep[2][counter])
            counter += 1

        return r_small

    d_small['asleep'] = sleep_contiguous(d_small['moving'], 1/time_window_length, min_valid_time = min_time_immobile)
    
    return d_small

