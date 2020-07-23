import pickle
import pandas as pd  
import numpy as np 
import warnings

import plotly as py 
import plotly.graph_objs as go 

from hmmlearn import hmm
from math import floor

from behavp_class import Behavpy
from behavr import set_behavpy

from datetime import datetime

counts = pd.read_csv('counts.csv')
flies = pd.read_csv('flies.csv')
timestamps = pd.read_csv('timestamps.csv')
cs_meta = flies[flies['GENOTYPE'] == 'CS']
id_list = cs_meta['ID'].tolist()
cs_data = counts[id_list]
cs_meta = cs_meta[cs_meta['SEX'] == 'M']
id_list = cs_meta['ID'].tolist()
cs_data = counts[id_list]

cs_data = cs_data.applymap(lambda x: np.where(x >= 1, 1, x))

timestamps['t'] = timestamps['X1'].map(lambda t: datetime.strptime(t, '%d-%b-%Y %H:%M:%S'))
timestamps['s'] = timestamps['t'].map(lambda t: (t.hour * 60 + t.minute) * 60 + t.second)

cs_data = cs_data.T

x = cs_data.values

def format_array(nested_list):

    len_list = []

    for i in nested_list:
        len_list.append(len(i))

    return len_list

#len_seq = format_array(x)

with open("hmm_toy.pkl", "rb") as file: 
    h = pickle.load(file)

print(h.transmat_)
print(h.startprob_)
print(h.emissionprob_) 
"""

def decode_array(nested_list):

    logprob_list = []
    states_list = []

    for i in range(len(nested_list)):
        #length = len(i)
        seq = np.array(nested_list[i])
        seq = seq.reshape(-1, 1)

        logprob, states = h.decode(seq)

        logprob_list.append(logprob)
        states_list.append(states)
        
    return logprob_list, states_list

log, states = decode_array(x)

def hmm_pct_state(state_array, time, total_states, avg_window = 300):
    
    states_dict = {}

    def moving_average(a, n) :
        ret = np.cumsum(a, dtype=float)
        ret[n:] = ret[n:] - ret[:-n]
        return ret[n - 1:] / n

    for i in total_states:
        states_dict['state_{}'.format(i)] = moving_average(np.where(state_array == i, 1, 0), n = avg_window)

    adjusted_time = time[avg_window-1:]

    df = pd.DataFrame.from_dict(states_dict)
    df.insert(0, 't', adjusted_time)
                        
    return df

#df = map(hmm_pct_state, states, counts.index.values, [0,1,2,3], avg_window = 30)

df_list = pd.DataFrame()
counter = 1

for l in states:
    df = hmm_pct_state(l, counts.index.values, [0,1,2,3], avg_window = 30)
    #index = pd.Index([counter] * len(df.index), name = 'id')
    df.insert(0, 'id', counter) 
    df_list = df_list.append(df, ignore_index= True)
    counter += 1


gb = df_list.groupby('t').agg(**{
            'mean_0' : ('state_0', 'mean'), 
            'mean_1' : ('state_1', 'mean'),
            'mean_2' : ('state_2', 'mean'),
            'mean_3' : ('state_3', 'mean')
       })


layout = go.Layout(
    title = 'Hidden States',
    yaxis = dict(
        title = 'state',
        range = [0, 1]
    ),
    xaxis = dict(
        title = 'Time (seconds)'#,
        #range = [0,24],
        #tick0 = 0,
        #dtick = 6

    )
)

trace_1 = go.Scatter(
    x = gb.index.values,
    y = gb['mean_0'],
    mode = 'lines',
    name = 'Deep Sleep',
    line = dict(
        shape = 'spline',
        color='blue'
    )
)

trace_2 = go.Scatter(
    x = gb.index.values,
    y = gb['mean_1'],
    mode = 'lines',
    name = 'Light Sleep',
    line = dict(
        shape = 'spline',
        color='green'
    )
)

trace_3 = go.Scatter(
    x = gb.index.values,
    y = gb['mean_2'],
    mode = 'lines',
    name = 'Light Awake',
    line = dict(
        shape = 'spline',
        color='yellow'
    )
)

trace_4 = go.Scatter(
    x = gb.index.values,
    y = gb['mean_3'],
    mode = 'lines',
    name = 'Full Awake',
    line = dict(
        shape = 'spline',
        color='purple'
    )
)

fig = go.Figure(data = [trace_1, trace_2, trace_3, trace_4], layout = layout)
fig.show()
"""