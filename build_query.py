import pandas as pd 
import numpy as np 
import os

results_dir = r"C:\Users\Loz\github\auto_generated_results"

pd.options.display.max_colwidth = 200

def list_of_files(path):
    """ Find and apppend all .db files in the given directory and sub-directories
        """
    files = []
    # r=root, d=directories, f = files
    for r, d, f in os.walk(path):
        for file in f:
            if '.db' in file:
                files.append(os.path.join(r, file))

    return files
