import pickle
import pandas as pd  
import numpy as np 
import warnings

import plotly as py 
import plotly.graph_objs as go 

from hmmlearn import hmm
from math import floor
from colour import Color
from scipy.stats import mode
from scipy.stats import pearsonr
from scipy.stats import spearmanr

from behavp_class import Behavpy
from behavr import set_behavpy

from datetime import datetime

pd.options.mode.chained_assignment = None

with open("hmm_arousal.pkl", "rb") as file: 
   h = pickle.load(file)

data = pd.read_pickle('arousal_moving.pkl')
metadata = pd.read_pickle('arousal_meta.pkl')

df = set_behavpy(metadata, data)

data['t'] = data['t'].map(lambda t: 60 * floor(t / 60))
bin_gb = data.groupby(['id','t']).agg(**{
                'moving' : ('moving', 'max')
        })

bin_gb.reset_index(level = 1, inplace = True)

gb = np.array(bin_gb.groupby(bin_gb.index)['moving'].apply(list).tolist(), dtype = 'object')

def decode_array(nested_list):

    logprob_list = []
    states_list = []

    for i in range(len(nested_list)):
        seq = np.array(nested_list[i])
        seq = seq.reshape(-1, 1)

        logprob, states = h.decode(seq)

        logprob_list.append(logprob)
        states_list.append(states)
        
    return logprob_list, states_list

log, states = decode_array(gb)


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

gb2 = np.array(bin_gb.groupby(bin_gb.index)['t'].apply(list).tolist(), dtype = 'object')
#gb2 = np.array(data.groupby(data.index)['t'].apply(list).tolist(), dtype = 'object')

df_list = pd.DataFrame()
counter = 1

for l, t in zip(states, gb2):
    df = hmm_pct_state(l, t, [0,1,2,3], avg_window = 30)
    df.insert(0, 'id', counter) 
    df_list = df_list.append(df, ignore_index= True)
    counter += 1

df_list['t'] = df_list['t'].map(lambda t: (60*60) * floor(t / (60*60)))
df_list['t'] = df_list['t'].map(lambda t: t % 86400)
df_list['t'] = df_list['t'] / (60*60)

def pop_std(array):
    return np.std(array, ddof = 0)

gb = df_list.groupby('t').agg(**{
            'mean_0' : ('state_0', 'mean'), 
            'SD_0' : ('state_0', pop_std),
            'count_0' : ('state_0', 'count'),
            'mean_1' : ('state_1', 'mean'),
            'SD_1' : ('state_1', pop_std),
            'count_1' : ('state_1', 'count'),
            'mean_2' : ('state_2', 'mean'),
            'SD_2' : ('state_2', pop_std),
            'count_2' : ('state_2', 'count'),
            'mean_3' : ('state_3', 'mean'),
            'SD_3' : ('state_3', pop_std),
            'count_3' : ('state_3', 'count')
        })


mean_deep = gb['mean_0']


data = pd.read_pickle('arousal_puff.pkl')
metadata = pd.read_pickle('arousal_meta.pkl')

df = set_behavpy(metadata, data)
df.xmv('odour', 'air_300', inplace = True)
data = df.data()
data = data[data['t_rel'] == 0]
data['bin'] = data['interaction_t'].map(lambda t: t % 86400)
data['bin'] = data['bin'].map(lambda t: 1 * floor(t / (60*60)))
bin_gb = data.groupby('bin').agg(**{
                'prop_respond' : ('has_responded', 'mean')
        })

mean_air = bin_gb['prop_respond']

c_df = pd.DataFrame({'air' : mean_air, 'deep' : mean_deep})
c_df.sort_values(by = ['air'], inplace = True)
p = pearsonr(c_df['air'], c_df['deep'])
s = spearmanr(c_df['air'], c_df['deep'])
print(p)
print(s)

fig = go.Figure()

trace = go.Scatter(
x = c_df['air'],
y = c_df['deep'],
mode = 'lines',
name = 'air',
line = dict(
    shape = 'spline',
    color = 'red'
    )
)
fig.add_trace(trace)



fig.show()
