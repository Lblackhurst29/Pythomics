import pandas as pd
import warnings 


class behavr:


def check_conform(data, metadata = None):

    if  isinstance(data, pd.DataFrame) is not True:
        warnings.warn('Data input is not a pandas dataframe')
        exit()

    # if metadata != None:
        # check that data is a behavr table already

    if metadata is None:
        warnings.warn('No metadata!')
        exit()

    if  isinstance(data, pd.DataFrame) is not True:
        warnings.warn('Metadata input is not a pandas dataframe')
        exit()