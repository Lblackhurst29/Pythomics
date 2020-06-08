import numpy as np 
import pandas as pd 
import sys 

def curate_dead_animals(data, moving_var = 'moving', time_window = 24, prop_immobile = 0.01, resolution = 24):
    time_window = 3600 * time_window
    if moving_var not in data.columns.tolist():
        print('variable name entered, {}, is not a column heading!'.format(var_name))
        sys.exit()

    d = data[['t', 'moving']]
    target_t = np.array(list(range(d.t.min(), d.t.max(), int(86400 / 24))))

    local_means = np.array([d[d['t'].between(i, i + 86400)]['moving'].mean() for i in target_t])

    first_death_point = np.where((local_means < prop_immobile) | (local_means == prop_immobile), True, False)
    first_death_point[15] = True

    if any(first_death_point) is False:
        return data

    last_valid_point = target_t[first_death_point]

    curated_data = data[data['t'].between(data.t.min(), last_valid_point[0])]

    return curated_data




