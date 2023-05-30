import pandas as pd
import glob
import os,datetime,sys
import subprocess as sp
#from send_notification import send_mail
os.chdir('/disk2/support_files/archive/core')
yesterday = datetime.date.today() - datetime.timedelta(3)
files = pd.date_range(start=yesterday,periods=3, freq='24H').strftime('core_new_%Y-%m-%d.h5').tolist()
folder = pd.date_range(start=yesterday,periods=3, freq='24H').strftime('%Y/%B/%-d.%m.%Y').tolist()
files2 = pd.date_range(start=yesterday, periods=3, freq='24H').strftime('fix_%Y-%m-%d.h5').tolist()
main ='/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/RAN QA/Daily/Raw_counters/Core/'

city,ultel,isp3=[],[],[]
for num,i in enumerate(files2):
    try:
        df=pd.read_hdf(i,'fix_cacti_uni_citynet')
        df.drop_duplicates(inplace=True)
        df['max_thrp']=df.iloc[:,-2:].max(axis=1)/1000
        city.append(df.groupby('Site').max()['max_thrp'].reset_index())
        df=pd.read_hdf(i,'fix_cacti_ultel')           
        df.drop_duplicates(inplace=True)
        df['max_thrp']=df.iloc[:,-2:].max(axis=1)/1000
        ultel.append(df.groupby('Site').max()['max_thrp'].reset_index())
        df=pd.read_hdf(i,'fix_cacti_3isp')            
        df.drop_duplicates(inplace=True)
        df['max_thrp']=df.iloc[:,-2:].max(axis=1)/1000
        isp3.append(df.groupby('Site').max()['max_thrp'].reset_index())
        print(i,' done')
    except Exception as e:
        print('error on file read')
        print(e)
        continue

map_city=pd.read_excel('/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/Core QA/Mapping for schedule/fix_mapping.xlsx',
                  sheet_name='Citynet-Uninet')
map_ultel=pd.read_excel('/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/Core QA/Mapping for schedule/fix_mapping.xlsx',
                  sheet_name='Ultel')
map_isp=pd.read_excel('/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/Core QA/Mapping for schedule/fix_mapping.xlsx',
                  sheet_name='3 ISP')
try:
    city=pd.concat(city).groupby('Site').max().reset_index().merge(map_city,left_on='Site',right_on='Link',how='left')[['Link','Citynet-Uninet ports','Capacity, Gbps','max_thrp']].\
        to_csv('/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/Core QA/scheduled_reports/fix_util/Citynet_'+files2[-1][4:-3]+'.csv',
            index=False)
    ultel=pd.concat(ultel).groupby('Site').max().reset_index().merge(map_ultel,left_on='Site',right_on='Link',how='left')[['Link','Ultel ports','Capacity, Gbps','max_thrp']].\
        to_csv('/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/Core QA/scheduled_reports/fix_util/Ultel_'+files2[-1][4:-3]+'.csv',
            index=False)
    isp3=pd.concat(isp3).groupby('Site').max().reset_index().merge(map_isp,left_on='Site',right_on='Link',how='left')[['Link','ISP','Capacity, Gbps','max_thrp']].\
        to_csv('/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/Core QA/scheduled_reports/fix_util/3ISP_'+files2[-1][4:-3]+'.csv',
            index=False)
except Exception as e:
    print('error at saving')
    print(e)
    1
print('save finished')
