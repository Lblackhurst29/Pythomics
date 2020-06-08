import pandas as pd 
import numpy as np 
import copy

from rle import rle 

#data = pd.read_pickle('bout_test.pkl')

def bout_analysis(data):
    """ """
    var_name = input('Enter name of variable: ')

    if var_name not in data.columns.tolist():
        print('variable name entered, {}, is not a column heading!'.format(var_name))
        exit()

    dt = copy.deepcopy(data[['t',var_name]])
    dt['deltaT'] = dt.t.diff()
    bout_rle = rle(data[var_name])
    vals = bout_rle[0]
    bout_range = list(range(1,len(vals)+1))

    bout_id = []
    counter = 0
    for i in bout_range:
        bout_id += ([i] * bout_rle[2][counter])
        counter += 1

    bout_df = pd.DataFrame({'bout_id' : bout_id, 'deltaT' : dt['deltaT']})
    bout_times = bout_df.groupby('bout_id').agg(
    duration = pd.NamedAgg(column='deltaT', aggfunc='sum')
    )
    bout_times[var_name] = vals
    time = list(np.cumsum(pd.Series(0).append(bout_times['duration'].iloc[:-1])) + dt.t.iloc[0])
    bout_times['t'] = time
    bout_times.reset_index(level=0, inplace=True)
    bout_times.drop(columns = ['bout_id'], inplace = True)
    
    
    return bout_times

#bt = bout_analysis(data)

#fil = bt[bt['asleep'] == True]

def bout_hist(data, relative = True, bins = 30):
    breaks = list(range(0, bins*60, 60))
    bout_cut = pd.DataFrame(pd.cut(data.duration, breaks, right = False, labels = breaks[1:]))
    bout_gb = bout_cut.groupby('duration').agg(
    count = pd.NamedAgg(column='duration', aggfunc='count')
    )
    bout_gb['prob'] = bout_gb['count'] / bout_gb['count'].sum()
    bout_gb.rename_axis('bins', inplace = True)
    bout_gb.reset_index(level=0, inplace=True)

    return bout_gb

#bout_hist = bout_hist(fil)

#x_smooth = np.linspace(bout_hist['bins'].min(), bout_hist['bins'].max(), 200)
#y_smooth = interp1d(bout_hist['bins'], bout_hist['prob'], x_smooth)
#a = interpolate.make_interp_spline(bout_hist['bins'], bout_hist['prob'])
#y_new = a(x_smooth)
#cs = CubicSpline(bout_hist['bins'], bout_hist['prob'])

#plt.plot(x_smooth, y_new, color = 'green')
#plt.plot('bins', 'prob', data = bout_hist, color = 'green')
#axes = plt.gca()
#axes.set_xlim([300,1800])
#plt.show()




