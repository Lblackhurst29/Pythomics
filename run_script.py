import pickle
import pandas as pd  
import numpy as np 
import warnings

import plotly as py 
import plotly.graph_objs as go 
from plotly.subplots import make_subplots

from hmmlearn import hmm
from math import floor
from colour import Color

from behavr import set_behavpy

pd.options.mode.chained_assignment = None

data = pd.read_pickle('hannah_data.pkl')
meta = pd.read_pickle('hannah_meta.pkl')
df = set_behavpy(meta, data)
df.xmv('sex', 'female', inplace = True)
df.xmv('mating_status', 'mated', inplace = True)

df_c = df.xmv('treatment', False)
data_c = df_c.data()
df_t =  df.xmv('sleep_deprivation', 'dynamic')
df_t = df_t.xmv('treatment', True)

data_t = df_t.data()

with open("hannah_hmm_all.pkl", "rb") as file: 
   h = pickle.load(file)


fig = make_subplots(
    rows=2, 
    cols=2,
    shared_xaxes=True, 
    shared_yaxes=True, 
    vertical_spacing=0.02,
    horizontal_spacing=0.02
    )

data = [data_c, data_t]
label = ['control', 'sleep deprived']
colours = [0, 1]

for d, n, c in zip(data, label, colours):
    colour_range_dict = {}
    for q in range(0,4):
        colours_dict = {'start' : ['#b2d8ff', '#8df086', '#eda866', '#ed776d'], 'end' : ['#00264c', '#086901', '#8a4300', '#700900']}
        start_color = colours_dict.get('start')[q]
        end_color = colours_dict.get('end')[q]
        N = 2
        colour_range_dict[q] = [x.hex for x in list(Color(start_color).range_to(Color(end_color), N))]

    df_new = d
    df_new = df_new[(df_new['t'] >= (24*60*60)) & (df_new['t'] < (96*60*60))]

    df_new['t'] = df_new['t'].map(lambda t: 60 * floor(t / 60))
    bin_gb = df_new.groupby(['id','t']).agg(**{
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
        df_hmm = hmm_pct_state(l, t, [0,1,2,3], avg_window = 30)
        df_hmm.insert(0, 'id', counter) 
        df_list = df_list.append(df_hmm, ignore_index= True)
        counter += 1

    df_list['t'] = df_list['t'].map(lambda t: t - (24*60*60))
    df_list['t'] = df_list['t'] / (60*60)

    gb = df_list.groupby('t').agg(**{
                'mean_0' : ('state_0', 'mean'), 
                'mean_1' : ('state_1', 'mean'),
                'mean_2' : ('state_2', 'mean'),
                'mean_3' : ('state_3', 'mean')
            })
    trace_0 = go.Scatter(
    x = gb.index.values,
    y = gb['mean_0'],
    mode = 'lines',
    name = n,
    line = dict(
        shape = 'spline',
        color= colour_range_dict.get(0)[c],
        width = 2
        ),
    showlegend = False
    )
    fig.add_trace(trace_0, row=1, col=1)

    trace_1 = go.Scatter(
    x = gb.index.values,
    y = gb['mean_1'],
    mode = 'lines',
    name = n,
    line = dict(
        shape = 'spline',
        color= colour_range_dict.get(1)[c],
        width = 2
        ),
    showlegend = False
    )
    fig.add_trace(trace_1, row=1, col=2)

    trace_2 = go.Scatter(
    x = gb.index.values,
    y = gb['mean_2'],
    mode = 'lines',
    name = n,
    line = dict(
        shape = 'spline',
        color= colour_range_dict.get(2)[c],
        width = 2
        ),
    showlegend = False
    )
    fig.add_trace(trace_2, row=2, col=1)

    trace_3 = go.Scatter(
    x = gb.index.values,
    y = gb['mean_3'],
    mode = 'lines',
    name = n,
    line = dict(
        shape = 'spline',
        color= colour_range_dict.get(3)[c],
        width = 2
        ),
    showlegend = False
    )
    fig.add_trace(trace_3, row=2, col=2)

fig.update_xaxes(
    zeroline = False,
    color = 'black',
    linecolor = 'black',
    gridcolor = 'black',
    range = [-0.25,72.25],
    tick0 = 0,
    dtick = 6,
    ticks = 'outside',
    tickwidth = 2,
    tickfont = dict(
        size = 18
    ),
    linewidth = 2,
    showgrid = False
)
fig.update_yaxes(
    zeroline = False, 
    color = 'black',
    linecolor = 'black',
    range = [-0.05, 1], 
    tick0 = 0,
    dtick = 0.2,
    ticks = 'outside',
    tickwidth = 2,
    tickfont = dict(
        size = 18
    ),
    linewidth = 4,
    showgrid = True
)

fig.update_layout(
    plot_bgcolor = 'white',
    legend = dict(
        bgcolor = 'rgba(201, 201, 201, 1)',
        bordercolor = 'grey',
        font = dict(
            size = 26
        ),
        x = 1.005,
        y = 0.5
    )
)

fig.update_layout(
    annotations=[
        go.layout.Annotation({
            'font': {'size': 22, 'color' : 'black'},
            'showarrow': False,
            'text': 'ZT Time (Hours)',
            'x': 0.5,
            'xanchor': 'center',
            'xref': 'paper',
            'y': 0,
            'yanchor': 'top',
            'yref': 'paper',
            'yshift': -30
        }),
        go.layout.Annotation({
            'font': {'size': 22, 'color' : 'black'},
            'showarrow': False,
            'text': 'Proportion of time in sleep state',
            'x': 0,
            'xanchor': 'left',
            'xref': 'paper',
            'y': 0.5,
            'yanchor': 'middle',
            'yref': 'paper',
            'xshift': -85,
            'textangle' : -90
        })
    ]
)

fig.update_layout(
    shapes=[
            dict(type="rect", xref="x1", yref="y1", x0=0, y0=-0.05, x1=12, y1=-0.020, 
            line=dict(color="black", width=3) ,fillcolor="white"),
            dict(type="rect", xref="x1", yref="y1", x0=12, y0=-0.05, x1=24, y1=-0.020, 
            line=dict(color="black", width=3) ,fillcolor="black"),
            dict(type="rect", xref="x1", yref="y1", x0=24, y0=-0.05, x1=36, y1=-0.020, 
            line=dict(color="black", width=3) ,fillcolor="white"),
            dict(type="rect", xref="x1", yref="y1", x0=36, y0=-0.05, x1=48, y1=-0.020, 
            line=dict(color="black", width=3) ,fillcolor="black"),
            dict(type="rect", xref="x1", yref="y1", x0=48, y0=-0.05, x1=60, y1=-0.020, 
            line=dict(color="black", width=3) ,fillcolor="white"),
            dict(type="rect", xref="x1", yref="y1", x0=60, y0=-0.05, x1=72, y1=-0.020, 
            line=dict(color="black", width=3) ,fillcolor="black"),
            dict(type="rect", xref="x2", yref="y2", x0=0, y0=-0.05, x1=12, y1=-0.020, 
            line=dict(color="black", width=3) ,fillcolor="white"),
            dict(type="rect", xref="x2", yref="y2", x0=12, y0=-0.05, x1=24, y1=-0.020, 
            line=dict(color="black", width=3) ,fillcolor="black"),
            dict(type="rect", xref="x2", yref="y2", x0=24, y0=-0.05, x1=36, y1=-0.020, 
            line=dict(color="black", width=3) ,fillcolor="white"),
            dict(type="rect", xref="x2", yref="y2", x0=36, y0=-0.05, x1=48, y1=-0.020, 
            line=dict(color="black", width=3) ,fillcolor="black"),
            dict(type="rect", xref="x2", yref="y2", x0=48, y0=-0.05, x1=60, y1=-0.020, 
            line=dict(color="black", width=3) ,fillcolor="white"),
            dict(type="rect", xref="x2", yref="y2", x0=60, y0=-0.05, x1=72, y1=-0.020, 
            line=dict(color="black", width=3) ,fillcolor="black"),
            dict(type="rect", xref="x3", yref="y3", x0=0, y0=-0.05, x1=12, y1=-0.020, 
            line=dict(color="black", width=3) ,fillcolor="white"),
            dict(type="rect", xref="x3", yref="y3", x0=12, y0=-0.025, x1=24, y1=-0.020, 
            line=dict(color="black", width=3) ,fillcolor="black"),
            dict(type="rect", xref="x3", yref="y3", x0=24, y0=-0.025, x1=36, y1=-0.020, 
            line=dict(color="black", width=3) ,fillcolor="white"),
            dict(type="rect", xref="x3", yref="y3", x0=36, y0=-0.025, x1=48, y1=-0.020, 
            line=dict(color="black", width=3) ,fillcolor="black"),
            dict(type="rect", xref="x3", yref="y3", x0=48, y0=-0.025, x1=60, y1=-0.020, 
            line=dict(color="black", width=3) ,fillcolor="white"),
            dict(type="rect", xref="x3", yref="y3", x0=60, y0=-0.025, x1=72, y1=-0.020, 
            line=dict(color="black", width=3) ,fillcolor="black"),
            dict(type="rect", xref="x4", yref="y4", x0=0, y0=-0.025, x1=12, y1=-0.020, 
            line=dict(color="black", width=3) ,fillcolor="white"),
            dict(type="rect", xref="x4", yref="y4", x0=12, y0=-0.025, x1=24, y1=-0.020, 
            line=dict(color="black", width=3) ,fillcolor="black"),
            dict(type="rect", xref="x4", yref="y4", x0=24, y0=-0.025, x1=36, y1=-0.020, 
            line=dict(color="black", width=3) ,fillcolor="white"),
            dict(type="rect", xref="x4", yref="y4", x0=36, y0=-0.025, x1=48, y1=-0.020, 
            line=dict(color="black", width=3) ,fillcolor="black"),
            dict(type="rect", xref="x4", yref="y4", x0=48, y0=-0.025, x1=60, y1=-0.020, 
            line=dict(color="black", width=3) ,fillcolor="white"),
            dict(type="rect", xref="x4", yref="y4", x0=60, y0=-0.025, x1=72, y1=-0.020, 
            line=dict(color="black", width=3) ,fillcolor="black"),
            ])

location = r'C:\Users\lab\Documents\MRes_thesis\figures\fig4\fig4a.pdf'
fig.write_image(location, width=1000, height=600)

