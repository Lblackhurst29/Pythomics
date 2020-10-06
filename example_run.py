from Download_from_remote_dir import download_from_remote_dir

metadata = r"C:\Users\lab\Documents\metadata\2019_02_12_AA_300s.csv"
remote_directory = "ftp://etho-node.lab.gilest.ro/auto_generated_data/ethoscope_results/"
local_director = r"C:\Users\lab\Documents\ethoscope_databases"

download_from_remote_dir(metadata, remote_directory, local)