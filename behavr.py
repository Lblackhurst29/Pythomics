import pandas as pd
import numpy as np 
from check_conform import check_conform
from behavp_class import Behavpy

#metadata = pd.DataFrame({'id' : ['A', 'B'],
#                         'treatment' : ['w', 'z'],
#                         'lifespan' : [19, 32],
#                         'ref_x' : [1, 0]})


#data = pd.DataFrame({'id': np.repeat(['A', 'B'], [10, 26], axis = 0), 
#                     't' : list(range(1, 11)) + list(range(5,31)),
#                     'x' : np.random.normal(0, 1, 36)})

data = pd.read_pickle('cached_data_2020-03-05_17-53-47.pkl')
metadata = pd.read_pickle('cached_metadata.pkl')


def set_behavpy(metadata, data):
    """ Takes two data frames, one metadata and the other the recorded values.
        Both added as attributes of an empty class 'behavpy' for easier sorting filtering
        must both contain an 'id' column with matching ids """

    check_conform(data, metadata)

    metadata.set_index('id', inplace = True)

    if 'path' in metadata.columns:
        metadata.drop('path', axis = 1, inplace = True)
    if 'file_name' in metadata.columns:
        metadata.drop('file_name', axis = 1, inplace = True)
    
    data.set_index('id', inplace = True)

    df = Behavpy(metadata, data)

    return df

if __name__ == '__main__':
    df = set_behavpy(metadata, data)
    df = df.xmv('camera', 'old')
    print(df)


    