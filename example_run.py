# First you'll need to clone my github reposistory 

# Part 1: Download the database files (db) to your local computer, run this once!
from download_from_remote_dir import download_from_remote_dir

# Needs a csv metadata file that contains machine name and date of experiment, meta = r'C:\Users\YOUR_NAME\Documents\sd_metadata.csv'
# All windows paths need r in front or they cant be read due to the back slashes "\"
# Remote_dir = the gilestro remote server - 'ftp://etho-node.lab.gilest.ro/auto_generated_data/ethoscope_results/' - you'll need to be connected to the imperial VPN
# Local_dir = the directory you want to save the db to, the function copies the directory tree of the source directory
# i.e. - r'C:\Users\YOUR_NAME\Documents\ethoscope_databases'

meta = r'C:\Users\lab\Documents\MRes_thesis\metadata\test.csv' # replace with your own
remote = 'ftp://etho-node.lab.gilest.ro/auto_generated_data/ethoscope_results/'
local = r'C:\Users\lab\Documents\ethoscope_databases' # replace with your own

download_from_remote_dir(meta, remote, local)

#--------------------------------------------------------------------------------------------------------------------

# Part 2: download the .db file into a dataframe to work with

from link_meta_index import link_meta_index

# same variables as part 1 with an added local cache location
cache = r'C:\Users\lab\Documents\ethoscope_databases\ethoscope_cache' # replace with your own

metadata = link_meta_index(meta, remote, local)

import pickle
from load_ethoscope import load_ethoscope
from functools import partial
from motion_detectors import max_velocity_detector

# reference hour = when the light is turned on, it's usually 9am
# FUN = a chosen function you want to apply to the data as it loads, use partial() function when you need to enter or change arguments, i.e window length
# two main functions, sleep_annotation (which have a boolean asleep column) and max_velocity_detector (just shows movement and is a part of sleep_annotation)
# if cache = a local direcotry, will save the dataframe as pickle file for quick storage and retrieval

df = load_ethoscope(metadata, reference_hour = 9.0, cache = cache, FUN = partial(max_velocity_detector, time_window_length = 60))

# Save the concated dataframe to a pickle file to use in the future
df.to_pickle('example_dataframe.pkl')

#--------------------------------------------------------------------------------------------------------------------

# Part 3: create a behavr object and call the HMM method

import pandas as pd 
import numpy as np
from behavr import set_behavpy

meta = link_meta_index(meta, remote, local)
data = pd.read_pickle('example_dataframe.pkl')
df = set_behavpy(meta, data)

# the hidden states 
states = ['Deep_sleep', 'Light_sleep', 'Light_awake', 'Full_awake']

# the movement observables
obs = ['inactive', 'active']

# intialised transmission probabilities
t_prob = np.array([[0.85, 0.10, 0.05, 0.00],
                    [0.20, 0.60, 0.20, 0.00],
                    [0.00, 0.30, 0.40, 0.30],
                    [0.00, 0.20, 0.00, 0.80]])

# intialised emission probabilities
em_prob =  np.array([[1, 0],
                    [1,0],
                    [0.5, 0.5],
                    [0.2, 0.8]])

# Call the method with the above variables, see behavpy_class for the what you can chain, uses the hmm_learn package
# If cache = True it will make a pickle file with an object with the trained probabilites
df.hmm_train(states, obs, t_prob, em_prob, cache = True)

#--------------------------------------------------------------------------------------------------------------------

# Part 4 - Create a 24 hour wrapped figure of the 4 hidden states, example

import pickle
import pandas as pd  
import numpy as np 
import warnings

import plotly as py 
import plotly.graph_objs as go 

from hmmlearn import hmm
from math import floor
from colour import Color

from behavp_class import Behavpy
from behavr import set_behavpy

from datetime import datetime

pd.options.mode.chained_assignment = None

with open("hmm_2016-04-04_17-41-32.pkl", "rb") as file: 
   h = pickle.load(file)

data = pd.read_pickle('sd_data_full_curated.pkl')
data.reset_index(inplace=True)
metadata = pd.read_pickle('sd_meta_full.pkl')
df = set_behavpy(metadata, data)
df.xmv('sex', 'M', inplace=True)
data = df.data()[df.data()['t'] < 96*60*60]

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

df_list = pd.DataFrame()
counter = 1

for l, t in zip(states, gb2):
    df = hmm_pct_state(l, t, [0,1,2,3], avg_window = 30)
    df.insert(0, 'id', counter) 
    df_list = df_list.append(df, ignore_index= True)
    counter += 1

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

layout = go.Layout(
    yaxis = dict(
        zeroline = False,
        color = 'black',
        linecolor = 'black',
        title = dict(
            text = 'Proportion of time in sleep state',
            font = dict(
                size = 22,
            )
        ),
        range = [-0.025, 1], 
        tick0 = 0,
        dtick = 0.2,
        ticks = 'outside',
        tickwidth = 2,
        tickfont = dict(
            size = 18
        ),
        linewidth = 4
    ),
    xaxis = dict(
        zeroline = False,
        color = 'black',
        linecolor = 'black',
        gridcolor = 'black',
        title = dict(
            text = 'ZT Time (Hours)',
            font = dict(
                size = 26,
                color = 'black'
            )
        ),
        range = [-0.25,24.25],
        tick0 = 0,
        dtick = 6,
        ticks = 'outside',
        tickwidth = 2,
        tickfont = dict(
            size = 22
        ),
        linewidth = 4
    ),
    plot_bgcolor = 'white',
    yaxis_showgrid=False,
    xaxis_showgrid = False,
    legend = dict(
        bgcolor = 'rgba(201, 201, 201, 1)',
        bordercolor = 'grey',
        font = dict(
            size = 16
        ),
        x = 0.75,
        y = 0.99
    )
)

fig = go.Figure(layout = layout)

colours = ['blue', 'green', 'orange', 'red']
transparent_col = ['rgba(0, 0, 255, 0.2)', 'rgba(0, 128, 0, 0.2)', 'rgba(255, 165, 0, 0.2)', 'rgba(255, 0, 0, 0.2)']
label = ['Deep Sleep', 'Light Sleep', 'Light Awake', 'Full Awake']

for i, c, n, t in zip(range(0,4), colours, label, transparent_col):
    mean = 'mean_{}'.format(i)
    sd = 'SD_{}'.format(i)
    count = 'count_{}'.format(i)

    gb['SE_{}'.format(i)] = (1.96*gb[sd]) / np.sqrt(gb[count])
    gb['y_max_{}'.format(i)] = gb[mean] + gb['SE_{}'.format(i)]
    gb['y_min_{}'.format(i)] = gb[mean] - gb['SE_{}'.format(i)]

    y = gb[mean]
    y_upper = gb['y_max_{}'.format(i)]
    y_lower = gb['y_min_{}'.format(i)]
    x = gb.index.values

    upper_bound = go.Scatter(
    showlegend = False,
    x = x,
    y = y_upper,
    mode='lines',
    marker=dict(color="#444"),
    line=dict(width=0,
            shape = 'spline'
            ),
    )
    fig.add_trace(upper_bound)

    trace = go.Scatter(
    x = x,
    y = y,
    mode = 'lines',
    name = n,
    line = dict(
        shape = 'spline',
        color = c
        ),
    fillcolor = t,
    fill = 'tonexty'
    )
    fig.add_trace(trace)

    lower_bound = go.Scatter(
    showlegend = False,
    x = x,
    y = y_lower,
    mode='lines',
    marker=dict(color="#444"),
    line=dict(width = 0,
            shape = 'spline'
            ),
    fillcolor = t,
    fill = 'tonexty'
    )  
    fig.add_trace(lower_bound)

# Light-Dark annotaion bars
fig.update_layout(
    shapes=[
            dict(type="rect", x0=0, y0=-0.025, x1=12, y1=0, 
            line=dict(color="black", width=3) ,fillcolor="white"),
            dict(type="rect", x0=12, y0=-0.025, x1=24, y1=0, 
            line=dict(color="black", width=3) ,fillcolor="black")
            ])

#location = r'C:\Users\lab\Documents\MRes_thesis\figures\fig1\fig1b.pdf'
#fig.write_image(location, width=900, height=650)

fig.show()