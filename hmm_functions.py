import pandas as pd  
import numpy as np 

def hmm_pct_transition(state_array, total_states):
    """State_array =  1D numpy array produced from a HMM decoder
        Total_states = numerical array denoting the states in 'state_array'
        Finds the proportion of instances of each state per array/fly"""
    from rle import rle

    v, s, l = rle(state_array)

    states_dict = {}

    def average(a):
        total = a.sum()
        count = len(a)
        av = total / count
        return av

    for i in total_states:
        states_dict['state_{}'.format(i)] = average(np.where(v == i, 1, 0))

    state_list = [states_dict]
    df = pd.DataFrame(state_list)

    return df

def hmm_mean_length(state_array, total_states, delta_t = 1):
    """State_array =  1D numpy array produced from a HMM decoder
        Total_states = numerical array denoting the states in 'state_array'
        Finds the mean length of each hidden state per array/fly """
    from rle import rle

    v, s, l = rle(state_array)

    df = pd.DataFrame(data = zip(v, l), columns = ['state', 'length'])
    df['length_adjusted'] = df['length'].map(lambda l: l * delta_t )
    gb_bout = df.groupby('state').agg(**{
                        'mean_length' : ('length_adjusted', 'mean')
    })
    gb_bout.reset_index(inplace = True)

    return gb_bout

def hmm_pct_state(state_array, time, total_states, avg_window = 30):
    """Takes a window of n and finds what proportion of each state is withing that window"""
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