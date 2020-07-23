from hmmlearn import hmm
import pandas as pd
import numpy as np
import warnings
import pickle

from behavp_class import Behavpy
from behavr import set_behavpy

import plotly as py 
import plotly.graph_objs as go 

warnings.filterwarnings('ignore')

np.random.seed(42)

states = ['s1', 's2', 'w1', 'w2']
n_states = len(states)

observables = ['inactive', 'active']
n_observations = len(observables)

em = [0, 1]
e_em = len(em)

start_prob = np.array([0.25, 0.25, 0.25, 0.25])

t_prob = np.array([[0.85, 0.10, 0.05, 0.00],
                    [0.20, 0.60, 0.20, 0.00],
                    [0.00, 0.30, 0.40, 0.30],
                    [0.00, 0.20, 0.00, 0.80]])

em_prob =  np.array([[1, 0],
                    [1,0],
                    [0.5, 0.5],
                    [0.2, 0.8]])

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

#gb = [x for x in gb if len(x) == 8640]

def format_array(nested_list):

    len_list = []

    for i in nested_list:
        len_list.append(len(i))

    return len_list

len_seq = format_array(gb)

seq = np.concatenate(gb, 0)
#seq = np.array(gb[0])
seq = seq.reshape(-1, 1)


h = hmm.MultinomialHMM(n_components=4, n_iter = 10, params = 'ste', init_params = 's')
h.startprob_ = start_prob
h.transmat_ = t_prob
h.n_features = 2 # number of emission states
h.emissionprob_ = em_prob

h.fit(seq, len_seq)

print(h.transmat_)
print(h.startprob_)
print(h.emissionprob_)

print(h.monitor_.converged)

with open("hmm_velocity_60.pkl", "wb") as file: pickle.dump(h, file)

"""
 log_prob, decode = h.decode(seq, [len(seq)], algorithm = 'viterbi')

gb2 = np.array(data.groupby('id')['t'].apply(list).tolist(), dtype = 'object')
time = gb2[0]

layout = go.Layout(
    title = 'Hidden States',
    yaxis = dict(
        title = 'state'
        #range = [0, 1]
    ),
    xaxis = dict(
        title = 'Time (s)'#,
        #range = [0,24],
        #tick0 = 0,
        #dtick = 6

    )
)

trace = go.Scatter(
    x = time,
    y = decode,
    mode = 'lines',
    name = 'states',
    line = dict(
        #shape = 'spline',
        color='rgb(0,57,20)'
    )
)

fig = go.Figure(data = [trace], layout = layout)
fig.show() """