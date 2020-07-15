import pandas as pd
import numpy as np

data = pd.read_pickle('bout_test.pkl')
print(data['t'])

def bin_data(data, coloumn, bin_mins = 5):
    """ Bin data by time finding mean of input column per bin
        bin is entered as minutes """

    bin_window = bin_mins * 60


    breaks = list(range(data.t.min(), data.t.max() + bin_window, bin_window))
    #print(breaks)

    bout_cut = pd.DataFrame(pd.cut(data.t, breaks, right = False, labels = breaks[:-1]))
    data['bin'] = bout_cut
    bout_gb = data.groupby('bin').agg(
    mean = pd.NamedAgg(column='max_velocity', aggfunc='mean')
    )
    bout_gb.rename_axis('bins', inplace = True)
    bout_gb.reset_index(level=0, inplace=True)

    return bout_gb


