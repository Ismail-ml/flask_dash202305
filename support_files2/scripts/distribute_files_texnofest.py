import os
import re
import datetime
import pandas as pd

l_path='/home/ismayil/flask_dash/data'
#print(path)
try:
    #print('entered to try')
    if os.path.isfile(os.path.join(l_path, 'files.csv')):
        existing_files = pd.read_csv(os.path.join(l_path, 'files.csv'), header=None)
    for vendor in ['nokia','huawei']:
        filenames = os.listdir(os.path.join(l_path,vendor))
        #print('dddd')
        if len(filenames)<=1:
            #print('entered here')
            continue
        for file in filenames:
            if ('csv' not in file) and ('zip' not in file): continue
            #if os.path.isfile(os.path.join(l_path, 'files.txt')):
            if file in existing_files.values: 
                os.remove(os.path.join(l_path,vendor,file))
                continue
except Exception as e:
    print(file,e)
    input('dsds')
