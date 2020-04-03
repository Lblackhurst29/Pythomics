# Copys file to working directory, but only as a 1kb file which requires an access key
import shutil
from urllib.request import urlopen
from contextlib import closing

ftp = 'ftp://nas.lab.gilest.ro/auto_generated_data/ethoscope_results/1298b7565b6948efb9a39b5d5d21e789/ETHOSCOPE_129/2020-02-04_12-54-50/'
source = '2020-02-04_12-54-50_1298b7565b6948efb9a39b5d5d21e789.db'

with closing(urlopen(ftp)) as r:
    with open(source, 'wb') as f:
        shutil.copyfileobj(r, f)


# Won't connect to the ftp server **Error: socket.gaierror: [Errno 11001] getaddrinfo failed** 
import ftplib

remote_dir = 'ftp://nas.lab.gilest.ro'
file_name = '2020-02-04_12-54-50_1298b7565b6948efb9a39b5d5d21e789.db'

ftp = FTP(remote_dir)
ftp.cwd('ftp://nas.lab.gilest.ro/auto_generated_data/ethoscope_results/1298b7565b6948efb9a39b5d5d21e789/ETHOSCOPE_129/2020-02-04_12-54-50/')

def grabFile(file_name2):

    localfile = open(file_name2, 'wb')
    ftp.retrbinary('RETR ' + file_name2, localfile.write)
        
    ftp.quit()
    localfile.close()

grabFile(file_name)