import pandas as pd
import zipfile
import os, shutil

#### Get latest dump and tracker from server
path='/mnt/netplan2/HUAWEI DB'
for i in range(3):
    h={}
    os.chdir(path)
    files_list=os.listdir()
    for file in files_list:
        i=os.stat(file)
        h[file]=i.st_ctime
    y=max(h, key=h.get)
    path=os.path.join(path,y)
directory='/home/ismayil/Downloads'
shutil.copy(path,directory+'/dump.zip')
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

zf = zipfile.ZipFile('/home/ismayil/Downloads/dump.zip') 
df = pd.read_excel(zf.open(zipfile.ZipFile.namelist(zf)[14]),sheet_name='CELL')
df.drop(index=0,inplace=True)
df[['BSCName','NODEBNAME','CELLID','CELLNAME','LAC','SAC','RAC','LOCELL','MAXTXPOWER','PCPICHPOWER','UARFCNDOWNLINK']].to_csv('/home/ismayil/flask_dash/support_files/Local_Cell_ID.csv')
df = pd.read_excel(zf.open(zipfile.ZipFile.namelist(zf)[16]),sheet_name='CELL')
df.drop(index=0,inplace=True)
df[['BSCName','NODEBNAME','CELLID','CELLNAME','LAC','SAC','RAC','LOCELL','MAXTXPOWER','PCPICHPOWER','UARFCNDOWNLINK']].to_csv('/home/ismayil/flask_dash/support_files/Local_Cell_ID.csv',
															mode='a',header=False)
