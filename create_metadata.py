from load_metafile import read_meta
from link_meta_index import link_meta_index

meta = r"C:\Users\Loz\github\Test_DB\2020-03-05_cameratest.csv"

index = r"C:\Users\Loz\github\auto_generated_results\index2.txt"

user_dir = r"C:\Users\Loz\github\auto_generated_results"

print(read_meta(meta))

print(link_meta_index(meta, index, user_dir))