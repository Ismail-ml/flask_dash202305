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
        filenames = os.listdir(os.path.join(l_path,vendor,'pool'))
        #print('dddd')
        if len(filenames)<=1:
            #print('entered here')
            continue
        for file in filenames:
            if ('csv' not in file) and ('zip' not in file): continue
            #if os.path.isfile(os.path.join(l_path, 'files.txt')):
            if file in existing_files.values: 
                os.remove(os.path.join(l_path,vendor,'pool',file))
                continue
            if 'zip' in file: continue
            #print('file name',file)
            tech=re.search(r"\wG",file)[0].lower()
            if "interf_ta" in file:
                extension='ta_inter'
            else:
                extension=re.search(r"counter_\w",file)[0][-1]
            if vendor == 'huawei':
                tarix=re.search(r"60_\d{8}",file)[0].replace('60_','')
                d=datetime.datetime.strptime(tarix,'%Y%m%d')
                day=datetime.datetime.strftime(d+datetime.timedelta(1),'%Y-%m-%d')
            if vendor == 'nokia':
                tarix=re.search(r"\d{4}_\d{2}_\d{2}",file)[0]
                d=datetime.datetime.strptime(tarix,'%Y_%m_%d')
                #day=datetime.datetime.strftime(d-datetime.timedelta(1),'%Y-%m-%d')
                day=datetime.datetime.strftime(d,'%Y-%m-%d')
            #print('vendor is ',vendor)
            #print('day is',day)
            if day >= datetime.datetime.strftime(datetime.date.today(),'%Y-%m-%d'):
		#if day == datetime.datetime.strftime(datetime.date.today() - datetime.timedelta(1),'%Y-%m-%d'):

                #print('day is true')
                if 'zip' not in file:
                    os.replace(src=os.path.join(l_path,vendor,'pool',file),dst=os.path.join(l_path,vendor,tech,extension,file))
    for vendor in ['nokia','huawei']:
        filenames = os.listdir(os.path.join(l_path,vendor,'pool','missing'))
        if len(filenames)<=0:
            continue
        for file in filenames:
            tech=file[:2].lower()
            extension=re.search(r"counter_\w",file)[0][-1]
            os.replace(src=os.path.join(l_path,vendor,'pool','missing',file),dst=os.path.join(l_path,vendor,tech,extension,'missing',file))
except Exception as e:
    print(file,e)
    input('dsds')
if os.path.isfile(os.path.join(l_path, 'files.csv')):
    os.chdir(os.path.join(l_path,'core'))
    for file in os.listdir(os.path.join(l_path,'core')):
        if file in existing_files.values:
            os.remove(file)
    os.chdir(os.path.join(l_path, 'utilization'))
    for file in os.listdir():
        if file in existing_files.values:
            #print(file,'existing')
            os.remove(file)
            continue
        if (('Power' in file) or ('LCG_Baseband' in file) or ('CE_Util' in file) or ('RSRAN131' in file) or ('PRB_Utilization-' in file) or ('3G_FRM' in file) or ('LTE_Frame' in file)):
            if ('-07_' not in file):
                os.remove(file)
        else :
            tarix = re.search(r"60_\d{8}", file)[0].replace('60_', '')
            d = datetime.datetime.strptime(tarix, '%Y%m%d')
            day = datetime.datetime.strftime(d, '%Y-%m-%d')
            #if day >= datetime.datetime.strftime(datetime.date.today(),'%Y-%m-%d'):
            #    os.remove(file)
                

