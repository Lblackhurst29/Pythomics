import pandas as pd
import numpy as np 

from Download_from_remote_dir import download_from_remote_dir
from link_meta_index import link_meta_index
from load_ethoscope import load_ethoscope
from behavr import set_behavpy

#meta = r"C:\Users\Loz\github\Test_DB\20190623_intensity.csv"
#index = r"C:\Users\Loz\github\Test_DB\index2.txt"
#local_direc = r"C:\Users\Loz\github\test_results"
#remote_direc = 'ftp://nas.lab.gilest.ro/auto_generated_data/ethoscope_results/'

#meta_df = link_meta_index(meta, index, local_direc)

#data = load_ethoscope(meta_df, reference_hour=9.0, cache = True)

data = pd.read_pickle('cached_data_2020-03-05_17-53-47_no_sleep.pkl') 
data.reset_index(level=0, inplace=True)

metadata = pd.read_pickle('cached_metadata.pkl')
df = set_behavpy(metadata, data)
df = df.motion_detector(time_window_length=60)

df.data().to_pickle('data_60sec.pkl')