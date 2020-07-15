import pandas as pd 
import numpy as np 
import warnings
from math import floor
import copy

import plotly as py 
import plotly.graph_objs as go 

from behavp_class import Behavpy
from behavr import set_behavpy
from motion_detectors import max_velocity_detector

data = pd.read_pickle('prob_multi_test.pkl')
data.reset_index(level=0, inplace=True)

metadata = pd.read_pickle('cached_metadata.pkl')

df = set_behavpy(metadata, data)
df = df.xmv('camera', 'old')
df = df.data()

prob_df = copy.deepcopy(df[['t', 'moving']])
#prob_df['t'] = df.t % (24*60*60)
prob_df['bin'] = prob_df['t'].map(lambda t: 60 * floor(t / 60))
prob_df['moving'] = prob_df['moving'].astype(int)

prob_gb = prob_df.groupby([prob_df.index,'bin']).agg(**{
            'moving' : ('moving', 'mean')
            })

prob_gb['moving'] = np.where(prob_gb['moving'] == 0, 0, 1)
prob_gb.reset_index(level = 1, inplace = True)

def probability(data):

    p_wake_list = [np.nan]*30
    p_doze_list = [np.nan]*30

    for i in range(0, (len(data.index)-30)):

        mov_avg = data.iloc[i:i+30,:]
        mov_avg['diff'] = mov_avg.moving.diff()
        values = mov_avg['moving'].value_counts(dropna=False).keys().tolist()
        counts = mov_avg['moving'].value_counts(dropna=False).tolist()
        active_count_dict = dict(zip(values, counts))
        
        inactive = active_count_dict.get(0)
        active = active_count_dict.get(1)

        values_diff = mov_avg['diff'].value_counts(dropna=False).keys().tolist()
        counts_diff = mov_avg['diff'].value_counts(dropna=False).tolist()
        diff_count_dict = dict(zip(values_diff, counts_diff))

        try:
            p_wake = diff_count_dict.get(1) / inactive
        except:
            p_wake = np.nan

        try:
            p_doze = diff_count_dict.get(-1) / active
        except:
            p_doze = np.nan

        p_wake_list.append(p_wake)
        p_doze_list.append(p_doze)


    data['P(w)'] = p_wake_list
    data['P(d)'] = p_doze_list

    return data

prob_new = prob_gb.groupby(prob_gb.index).apply(probability)
prob_new.to_pickle('prob_comp.pkl')


def probability_pwake(data):
    wake = 0
    for i in data['diff']:
        if i == 1:
            wake += 1

    values = data['moving'].value_counts(dropna=False).keys().tolist()
    counts = data['moving'].iloc[:-1].value_counts(dropna=False).tolist()
    active_count_dict = dict(zip(values, counts))

    try:
        pwake = wake / active_count_dict.get(0)
        return pwake
    except:
        return np.nan

#prob_gb = pd.DataFrame(one_day_prob.groupby('cut').apply(probability_pwake), columns = ['pwake'])
#print(prob_gb)

def probability_pdoze(data):
    doze = 0
    for i in data['diff']:
        if i == -1: 
            doze += 1

    values = data['moving'].value_counts(dropna=False).keys().tolist()
    counts = data['moving'].iloc[:-1].value_counts(dropna=False).tolist()
    active_count_dict = dict(zip(values, counts))

    try:
        pdoze = doze / active_count_dict.get(1)
        return pdoze
    except:
        return np.nan

#prob_gb['pdoze'] = prob_df.groupby('cut').apply(probability_pdoze)
#prob_gb.reset_index(level=0, inplace=True)
#day = 60*60*24
#one_day_a = prob_gb['pwake'].iloc[:day]

#one_day_d = prob_gb['pdoze'].iloc[:day]

#plot.figure()
#one_day_d.plot(style = 'og', markersize = 3)
#plot.show()