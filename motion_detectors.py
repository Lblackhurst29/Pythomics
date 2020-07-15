import pandas as pd
import numpy as np
import warnings
import copy
from math import floor

def max_velocity_detector(data,
                          time_window_length,
                          velocity_correction_coef = 3e-3,
                          masking_duration = 6,
                          optional_columns = 'has_interacted'
                         ):
    """ """
    dt = prep_data_motion_detector(data,
                                   needed_columns = ['t', 'x', 'xy_dist_log10x1000'],
                                   time_window_length = time_window_length,
                                   optional_columns = optional_columns)
    
    dt['deltaT'] = dt.t.diff()
    dt['dist'] = 10 ** (dt.xy_dist_log10x1000 / 1000)
    dt['velocity'] = dt.dist / dt.deltaT

    a = velocity_correction_coef

    dt['beam_cross'] = abs(np.sign(0.5 - dt['x']).diff())
    dt['beam_cross'] = np.where(dt['beam_cross'] == 2.0, True, False)

    if 'has_interacted' not in dt.columns:
        if masking_duration > 0:
            warnings.warn("Data does not contain a `has_interacted` column. Cannot apply masking! Set `masking_duration = 0` to ignore masking")
        dt['has_interacted'] = 0

    dt['interaction_id'] = dt['has_interacted'].cumsum()
    dt['mask'] = dt.groupby('interaction_id')['t'].apply(lambda x: pd.Series(np.where(x < (x.min() + masking_duration), True, False), index=x.index))

    dt.loc[(dt.mask == True) & (dt.interaction_id != 0), 'velocity'] = 0
    dt['beam_cross'] = dt['beam_cross'] & ~dt['mask'] # ~ = opposite of dt['mask']
    dt = dt.drop('interaction_id', 1)
    dt = dt.drop('mask', 1)
    # end of masking

    dt['velocity_corrected'] = dt.velocity * dt.deltaT / a

    d_small = dt.groupby('t_round').agg(
    x = pd.NamedAgg(column='x', aggfunc='mean'),
    max_velocity = pd.NamedAgg(column='velocity_corrected', aggfunc='max'),
    interactions = pd.NamedAgg(column='has_interacted', aggfunc='sum'),
    beam_crosses = pd.NamedAgg(column='beam_cross', aggfunc= 'sum')
    )

    d_small['moving'] = np.where(d_small['max_velocity'] > 1, True, False)
    d_small.rename_axis('t', inplace = True)
    d_small.reset_index(level=0, inplace=True)

    return d_small


def prep_data_motion_detector(data,
                              needed_columns,
                              time_window_length = 10, #for now
                              optional_columns = None 
                              ):
    """ bin all points of time into windows of 'x' """
    if all(elem in data.columns.values for elem in needed_columns) is not True:
        warnings.warn('data from ethoscope should have columns named {}!'.format(needed_columns))
        exit()

    # check optional columns input are column headings
    if optional_columns != None:
    
        if isinstance(optional_columns, str):
            check_optional_columns = set(data.columns.tolist()).intersection(list([optional_columns]))
            needed_columns = set(list(check_optional_columns) + needed_columns) 
        else:
            check_optional_columns = set(data.columns.tolist()).intersection(optional_columns)
            needed_columns = set(list(check_optional_columns) + needed_columns)

    dc = copy.deepcopy(data[needed_columns])
    dc['t_round'] = dc['t'].map(lambda t: time_window_length * floor(t / time_window_length)) 
    
    def curate_sparse_roi_data(data,
                               window = 60,
                               min_p = 20
                               ):
        """ Remove rows from table when there are not enough data points per 1 minute bin """
        data['t_w'] = data['t'].map(lambda t: 60 * floor(t / 60))
        data['n_points'] = data.groupby(['t_w'])['t_w'].transform('count')
        data = data[data.n_points > min_p]
        data.drop(columns = ['t_w', 'n_points'], inplace = True)
        return data

    dc = curate_sparse_roi_data(dc)

    return dc