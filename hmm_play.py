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

data = pd.read_csv('counts.csv')
flies = pd.read_csv('flies.csv')
timestamps = pd.read_csv('timestamps.csv')

cs_data = data.applymap(lambda x: np.where(x >= 1, 1, x))

cs_data = cs_data.T

x = cs_data.values

def format_array(nested_list):

    len_list = []

    for i in nested_list:
        len_list.append(len(i))

    return len_list

len_seq = format_array(x)

seq = np.concatenate(x, 0)
seq = seq.reshape(-1, 1)

h = hmm.MultinomialHMM(n_components=4, n_iter = 10, params = 'ste', init_params = 's')
#h.startprob_ = start_prob
h.transmat_ = t_prob
h.n_features = 2 # number of emission states
h.emissionprob_ = em_prob

h.fit(seq, len_seq)

print(h.monitor_.converged)

with open("hmm_toy2.pkl", "wb") as file: pickle.dump(h, file)

print(h.transmat_)
print(h.startprob_)
print(h.emissionprob_) 

