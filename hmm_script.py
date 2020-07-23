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

warnings.filterwarnings('ignore')

with open("hmm_velocity_60.pkl", "rb") as file: 
   h = pickle.load(file)

data = pd.read_pickle('data_60sec.pkl') 
data.reset_index(level=0, inplace=True)

metadata = pd.read_pickle('cached_metadata.pkl')
df = set_behavpy(metadata, data)
df1 = df.xmv('camera', 'old')
df2 = df.xmv('code', 'altered')
data = df1.data()
data2 = df2.data()
data = data.append(data2)


#data = data[(data['t'] >= (24*60*60)) & (data['t'] < (2*24*60*60))]

data['moving'] = np.where(data['moving'] == True, 1, 0)

gb = np.array(data.groupby('id')['moving'].apply(list).tolist(), dtype = 'object')


def decode_array(nested_list):

    logprob_list = []
    states_list = []

    for i in range(len(nested_list)):
        #length = len(i)
        seq = np.array(gb[i])
        seq = seq.reshape(-1, 1)

        logprob, states = h.decode(seq)

        logprob_list.append(logprob)
        states_list.append(states)
        
    return logprob_list, states_list

log, states = decode_array(gb)


gb2 = np.array(data.groupby('id')['t'].apply(list).tolist(), dtype = 'object')


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

df_list = pd.DataFrame()

for l, t in zip(states, gb2):
    df = hmm_pct_state(l, t, [0,1,2,3], avg_window = 300)
    df_list = df_list.append(df, ignore_index= True)

df_list['t'] = df_list['t'].map(lambda t: t % 86400)
df_list['t'] = df_list['t'] / (60*60)



gb = df_list.groupby('t').agg(**{
            'mean_0' : ('state_0', 'mean'), 
            'mean_1' : ('state_1', 'mean'),
            'mean_2' : ('state_2', 'mean'),
            'mean_3' : ('state_3', 'mean')
       })

layout = go.Layout(
    title = 'Hidden States',
    yaxis = dict(
        title = 'Pct time in state',
        range = [0, 1]
    ),
    xaxis = dict(
        title = 'Time (ZT hours)',
        range = [0,24],
        tick0 = 0,
        dtick = 6

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