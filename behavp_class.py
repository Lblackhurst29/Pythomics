import pandas as pd
import numpy as np 
import warnings
import copy
from check_conform import check_conform

class Behavpy(pd.DataFrame):
    """
    """

    def format_Warning(message, category, filename, lineno, line=''):
        return str(filename) + ':' + str(lineno) + ': ' + category.__name__ + ': ' +str(message) + '\n'
    #formats warming method to not double print for user and allows string formatting
    warnings.formatwarning = format_Warning

    def __init__(self, metadata, data):
        self._metadata = metadata
        self._data = data

    def __repr__(self):
        return '\n ==== METADATA ====\n\n{}\n ====== DATA ======\n\n{}'.format(self._metadata, self._data)
         

    def xmv(self, column, metavariable, inplace = False):
        """Expand metavariable from the behavpy object
        returns a pandas dataframe matched to the entered metavariable"""

        if column not in self._metadata.columns:
            warnings.warn('Column heading "{}" is not in the metadata table'.format(column))
            exit()

        
        if metavariable not in self._metadata[column].tolist():
            warnings.warn('Metavariablle "{}" is not in the column'.format(metavariable))
            exit()


        index_list = self._metadata[self._metadata[column] == metavariable].index.tolist()

        if inplace is True:
            self._metadata = self._metadata.loc[index_list]
            self._data = self._data.loc[index_list]
        
        else:
            xmv_df = Behavpy(self._metadata.loc[index_list], self._data.loc[index_list])
            
            return xmv_df
            
    def meta(self):
        """return metadata dataframe"""
        
        return self._metadata

    def data(self):
        """return data dataframe"""
        
        return self._data

    def rejoin(self, new_column, inplace = False):
        """joins the data of a Behavpy table to its own metadata"""

        check_conform(new_column)

        if inplace is True:
            self._metadata = self._metadata.join(new_column, on = 'id')


        else:
            rejoin_df = Behavpy(self._metadata.join(new_column, on = 'id'), self._data)
    
            return rejoin_df

    def pivot(self, group, function):
        """ wrapper for the groupby pandas method
            will always group by the id on the data df attribute
            takes a column input to perform a function on
            functin can be standard "mean", "max".... ect
            can also be a user defined function
            returns a new dataframe """

        if group not in self._data.columns:
            warnings.warn('Column heading "{}", is not in the data table'.format(group))
            exit()
            
        parse_name = '{}_{}'.format(group, function) # create new column name
        
        pivot = self._data.groupby(self._data.index).agg(**{
            parse_name : (group, function)    
        })

        return pivot

    def sleep_bout_analysis(self, sleep_var = 'asleep', as_hist = False, relative = True, min_bins = 30, asleep = True):
        """ takes a column with boolean values that represents sleep
            returns a Behavpy pandas DataFrame object with duration and t start
            of each boolean sequence 
            arguments relative and min_bins only used if as_hist = True"""

        from rle import rle

        if sleep_var not in self._data.columns:
            warnings.warn('Column heading "{}", is not in the data table'.format(sleep_var))
            exit()

        def wrapped_bout_analysis(data, var_name = sleep_var, as_hist = as_hist, relative = relative, min_bins = min_bins, asleep = asleep):

            index_name = data.index[0]
            
            dt = copy.deepcopy(data[['t',var_name]])
            dt['deltaT'] = dt.t.diff()
            bout_rle = rle(dt[var_name])
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
            old_index = pd.Index([index_name] * len(bout_times.index), name = 'id')
            bout_times.set_index(old_index, inplace =True)

            if as_hist is True:

                if asleep is True:
                    filtered = bout_times[bout_times[var_name] == True]
                if asleep is False:
                    filtered = bout_times[bout_times[var_name] == False]

                breaks = list(range(0, min_bins*60, 60))
                bout_cut = pd.DataFrame(pd.cut(filtered.duration, breaks, right = False, labels = breaks[1:]))
                bout_gb = bout_cut.groupby('duration').agg(
                count = pd.NamedAgg(column = 'duration', aggfunc = 'count')
                )
                bout_gb['prob'] = bout_gb['count'] / bout_gb['count'].sum()
                bout_gb.rename_axis('bins', inplace = True)
                bout_gb.reset_index(level=0, inplace=True)
                old_index = pd.Index([index_name] * len(bout_gb.index), name = 'id')
                bout_gb.set_index(old_index, inplace =True)

                return bout_gb

            else:
                return bout_times

        return Behavpy(self._metadata, self._data.groupby('id', group_keys = False).apply(wrapped_bout_analysis))

    def curate_dead_animals(self, moving_var = 'moving', time_window = 24, prop_immobile = 0.01, resolution = 24):
        """ """
        time_window = 3600 * time_window # converts hours to seconds

        if moving_var not in self._data.columns:
            warnings.warn('variable name entered "{}" is not a column heading!'.format(moving_var))
            exit()

        def wrapped_curate_dead_animals(data, moving_var = 'moving', time_window = time_window, prop_immobile = prop_immobile, resolution = resolution):

            d = self._data[['t', 'moving']]
            target_t = np.array(list(range(d.t.min(), d.t.max(), int(time_window / resolution))))

            local_means = np.array([d[d['t'].between(i, i + time_window)]['moving'].mean() for i in target_t])

            first_death_point = np.where((local_means < prop_immobile) | (local_means == prop_immobile), True, False)

            if any(first_death_point) is False:
                exit()

            last_valid_point = target_t[first_death_point]

            curated_data = data[data['t'].between(data.t.min(), last_valid_point[0])]

            return curated_data 

        self._data = self._data.groupby(self._data.index, group_keys = False).apply(wrapped_curate_dead_animals)

    def bin_data(self, column, bin_column = 't', function = 'mean', bin_mins = 5):
        """ Bin data by time finding mean of input column per bin
            bin is entered as minutes """

        bin_secs = bin_mins * 60

        if column not in self._data.columns:
            warnings.warn('Column heading "{}", is not in the data table'.format(column))
            exit()

        def wrapped_bin_data(data, column = column, bin_column = bin_column, function = function, bin_secs = bin_secs):

            index_name = data.index[0]

            breaks = list(range(data[bin_column].min(), data[bin_column].max() + bin_secs, bin_secs))

            bout_cut = pd.DataFrame(pd.cut(data[bin_column], breaks, right = False, labels = breaks[:-1]))
            data['bin'] = bout_cut

            output_parse_name = '{}_{}'.format(column, function) # create new column name
        
            bout_gb = data.groupby('bin').agg(**{
                output_parse_name : (column, function)    
            })

            bin_parse_name = '{}_bin'.format(bin_column)

            bout_gb.rename_axis(bin_parse_name, inplace = True)
            bout_gb.reset_index(level=0, inplace=True)
            old_index = pd.Index([index_name] * len(bout_gb.index), name = 'id')
            bout_gb.set_index(old_index, inplace =True)

            return bout_gb
            
        return Behavpy(self._metadata, self._data.groupby('id', group_keys = False).apply(wrapped_bin_data))

    def summary(self, detailed = False):
        """ Provides summary statistics of metadata and data counts
            if detailed is True count and range of data points will be broken down per 'id' """

        def print_table(table):
            longest_cols = [
                (max([len(str(row[i])) for row in table]) + 3)
                for i in range(len(table[0]))
            ]
            row_format = "".join(["{:>" + str(longest_col) + "}" for longest_col in longest_cols])
            for row in table:
                print(row_format.format(*row))

        if detailed is False:
            individuals = len(self._metadata.index)
            metavariable = len(self._metadata.columns)
            variables = len(self._data.columns)
            measurements = len(self._data.index)
            table = [
                ['individuals', individuals],
                ['metavariable', metavariable],
                ['variables', variables],
                ['measurements', measurements],
            ]
            print('behavpy table with: ')
            print_table(table)

        if detailed is True:

            def time_range(data):
                return (str(min(data)) + '  ->  ' + str(max(data)))


            group = self._data.groupby('id').agg(
                data_points = pd.NamedAgg(column = 't', aggfunc = 'count'),
                time_range = pd.NamedAgg(column = 't', aggfunc = time_range)
            )

            print(group)

    def add_day_phase(self, reference_hour = None):
        """ adds a column with either light or dark to the data
            adds a column with the sequential day
            column data is catergorical """

        from math import floor

        self._data['day'] = self._data['t'].map(lambda t: floor(t / 86400))
        
        if reference_hour is None:
            self._data['phase'] = np.where(((self._data.t % 86400) > 43200), 'dark', 'light')
            self._data['phase'] = self._data['phase'].astype('category')

    def motion_detector(self, time_window_length = 10, velocity_correction_coef = 3e-3, masking_duration = 0, optional_columns = None):
        """Method version of the motion detector without sleep annotation varaiables"""

        if optional_columns is not None:
            if optional_columns not in self._data.columns:
                warnings.warn('Column heading "{}", is not in the data table'.format(optional_columns))
                exit()

        def wrapped_motion_detector(data, 
                                    time_window_length = time_window_length, 
                                    velocity_correction_coef = velocity_correction_coef, 
                                    masking_duration = masking_duration, 
                                    optional_columns = optional_columns):
            
            from motion_detectors import max_velocity_detector
            
            index_name = data.index[0]
            
            df = max_velocity_detector(data,                                   
                                    time_window_length = time_window_length, 
                                    velocity_correction_coef = velocity_correction_coef, 
                                    masking_duration = masking_duration, 
                                    optional_columns = optional_columns)

            old_index = pd.Index([index_name] * len(df.index), name = 'id')
            df.set_index(old_index, inplace =True)  

            return df                     

        return Behavpy(self._metadata, self._data.groupby('id', group_keys = False).apply(wrapped_motion_detector))
       



              



