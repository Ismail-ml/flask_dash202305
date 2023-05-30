import pandas as pd
import zipfile
import os, shutil
#import pyodbc
import subprocess

#### Get latest dump and tracker from server
directory='/home/ismayil/Downloads'
for p in ['HUAWEI DB','NSN DUMP']:
    print(p)
    path='/mnt/netplan2/'+p
    for i in range(3):
        h={}
        os.chdir(path)
        files_list=os.listdir()
        for file in files_list:
            i=os.stat(file)
            h[file]=i.st_ctime
        y=max(h, key=h.get)
        path=os.path.join(path,y)
    shutil.copy(path,directory+'/dump_'+p.split(' ')[0]+'.zip')
    print(p,' copy finished')
shutil.copy('/mnt/rnp_tracker/Corporate Folder/CTO/Technology trackers/RNP/Azerconnect_RNP_tracker.xlsx',directory)

print('Tracker is loaded from server')
#############################

tracker=pd.read_excel(r'/home/ismayil/Downloads/Azerconnect_RNP_tracker.xlsx',skiprows=[0])
d={}
for j in tracker.columns:
	d[j]=0

d['Economical Region']='Baku'
d['SITE_ID']='BBK0408'
d['Lat']=40.39746
d['Long']=49.8604
tracker=tracker.append(d,ignore_index=True)
d['Economical Region']='Baku'
d['SITE_ID']='BBK0741'
d['Lat']=40.37576
d['Long']=49.86234
tracker=tracker.append(d,ignore_index=True)
d['Economical Region']='Qarabag'
d['SITE_ID']='GNJ1263'
d['Lat']=40.01711
d['Long']=47.07448
tracker=tracker.append(d,ignore_index=True)
d['Economical Region'] = 'Absheron'
d['SITE_ID'] = 'ABS1253'
d['Lat'] = 40.49212
d['Long'] = 49.87089
tracker = tracker.append(d, ignore_index=True)

tracker.to_csv(r'/home/ismayil/flask_dash/support_files/tracker.csv')
print('Tracker loaded successfully')

zf = zipfile.ZipFile('/home/ismayil/Downloads/dump_HUAWEI.zip') 
df = pd.read_excel(zf.open(zipfile.ZipFile.namelist(zf)[14]),sheet_name='CELL')
df.drop(index=0,inplace=True)
df[['BSCName','NODEBNAME','CELLID','CELLNAME','LAC','SAC','RAC','LOCELL','MAXTXPOWER','PCPICHPOWER','UARFCNDOWNLINK']].to_csv('/home/ismayil/flask_dash/support_files/Local_Cell_ID.csv')
df = pd.read_excel(zf.open(zipfile.ZipFile.namelist(zf)[16]),sheet_name='CELL')
df.drop(index=0,inplace=True)
df[['BSCName','NODEBNAME','CELLID','CELLNAME','LAC','SAC','RAC','LOCELL','MAXTXPOWER','PCPICHPOWER','UARFCNDOWNLINK']].to_csv('/home/ismayil/flask_dash/support_files/Local_Cell_ID.csv',
															mode='a',header=False)
shutil.unpack_archive('/home/ismayil/Downloads/dump_NSN.zip',extract_dir='/home/ismayil/Downloads')
subprocess.run('mdb-export /home/ismayil/Downloads/AZC_DUMP.mdb A_WBTS > /home/ismayil/Downloads/outputs.csv',shell=True)
subprocess.run('mdb-export /home/ismayil/Downloads/AZC_DUMP.mdb A_BCF > /home/ismayil/Downloads/outputs2.csv',shell=True)

cd=pd.read_csv('/home/ismayil/Downloads/outputs.csv')
cd=cd[['RncId','WBTSId','name']]
cd['a']='RNC-'+cd['RncId'].astype(str)+'/WBTS-'+cd['WBTSId'].astype(str)
cd2=pd.read_csv('/home/ismayil/Downloads/outputs2.csv')
cd2=cd2[['BSCId','BCFId','name']]
cd2.rename(columns={'BSCId':'RncId','BCFId':'WBTSId'},inplace=True)
cd2['a']='BSC-'+cd2['RncId'].astype(str)+'/BCF-'+cd2['WBTSId'].astype(str)
pd.concat([cd,cd2]).to_csv('/home/ismayil/flask_dash/support_files/site_name_lookup.csv',index=False)

subprocess.run('cd /home/ismayil/Downloads',shell=True)
subprocess.run('rm /home/ismayil/Downloads/*',shell=True)
