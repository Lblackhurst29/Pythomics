import pandas as pd
import numpy as np 
import sqlite3
import time
import warnings
import sys
from experiment_metadata import experiment_metadata

def read_single_roi(FILE, region_id, min_time = 0, max_time = float('inf'), reference_hour = None, FUN = None):
    """  """


    experiment_info = experiment_metadata(FILE)

    if min_time > max_time:
        sys.exit('Error: min_time is larger than max_time')

    try:
        conn = sqlite3.connect(FILE)
        roi_df = pd.read_sql_query('SELECT * FROM ROI_MAP', conn)
        roi_row = roi_df[roi_df['roi_idx'] == region_id]
        var_df = pd.read_sql_query('SELECT * FROM VAR_MAP', conn)
        
        if len(roi_row.index) == 0:
            warnings.warn('ROI {} does not exist, skipping'.format(region_id))
            return None

        if max_time == float('inf'):
            max_time_condtion =  ''
        else:
            max_time_condtion = 'AND t < {}'.format(max_time * 1000) 
        
        min_time = min_time * 1000

        sql_query = 'SELECT * FROM ROI_{} WHERE t >= {} {}'.format(region_id, min_time, max_time_condtion)
        data = pd.read_sql_query(sql_query, conn)
        
        
        if 'id' in data.columns:
            data = data.drop('id', 1)

        if reference_hour != None:
            t = experiment_info['value'].loc[experiment_info['field'] == 'date_time']
            t = t.iloc[0].split(' ')
            hh, mm , ss = map(int, t[1].split(':'))
            hour_start = hh + mm/60 + ss/3600
            t_after_ref = ((hour_start - reference_hour) % 24) * 3600 * 1e3
            data.t = (data.t + t_after_ref) / 1e3

        else:
            data.t = data.t / 1e3

        roi_width = max(roi_row['w'].values[0], roi_row['h'].values[0])
        for var_n in var_df['var_name']:
            if var_df['functional_type'].loc[var_df['var_name'] == var_n].values[0] == 'distance':
                normalise = lambda x: x / roi_width
                data[var_n] = data[var_n].map(normalise)

            if var_df['sql_type'].loc[var_df['var_name'] == var_n].values[0] == 'BOOLEAN':
                boolean = lambda x: bool(x) 
                data[var_n] = data[var_n].map(boolean)  

        if 'is_inferred' and 'has_interacted' in data.columns:
            data = data[(data['is_inferred'] == False) | (data['has_interacted'] == True)]
            # check if has_interacted is all false / 0, drop if so
            interacted_list = data['has_interacted'].to_numpy()
            if (0 == interacted_list[:]).all() == True:
                data = data.drop('has_interacted', 1)
                data = data.drop('is_inferred', 1)

        elif 'is_inferred' in data.columns:
            data = data[data['is_inferred'] == False]
            data = data.drop('is_inferred', 1)

        if FUN is not None:
            if 'has_interacted' not in data.columns:
                data = FUN(data, masking_duration = 0)

            else:
                data = FUN(data)

        # add functionailty to check if function returns full dataset
        return data

    finally:
        conn.close()
