import ftplib
import os

remote_dir = 'nas.lab.gilest.ro'
file_name = '2020-02-04_12-54-50_1298b7565b6948efb9a39b5d5d21e789.db'


ftp = ftplib.FTP(remote_dir)
ftp.login()
ftp.cwd('/auto_generated_data/ethoscope_results/1298b7565b6948efb9a39b5d5d21e789/ETHOSCOPE_129/2020-02-04_12-54-50/')

def grabFile(file_name2):

    path = os.path.join(r'C:\Users\Loz\github\Test_DB', file_name2)
    localfile = open(path, 'wb')
    ftp.retrbinary('RETR ' + file_name2, localfile.write)
        
    ftp.quit()
    localfile.close()

grabFile(file_name)