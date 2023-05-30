import os
import re
import datetime
import pandas as pd
filenames = os.listdir('/home/qarftp/Nokia')
#print(filenames)
today=datetime.date.today()
os.chdir('/home/qarftp/Nokia')
for file in filenames:
    #print(file,'and directory is',os.getcwd())
    if 'xlsx' in file:
        os.remove(file)
        continue
    #print('file name',file)
    #extension=re.search(r"counter_\w",file)[0][-1]
    tarix=re.search(r"\d{4}_\d{2}_\d{2}",file)[0]
    d=datetime.datetime.strptime(tarix,'%Y_%m_%d')
    #print(d)
    #print(today-datetime.timedelta(2))
    if d.date()<(today-datetime.timedelta(2)):
        os.remove(file)

