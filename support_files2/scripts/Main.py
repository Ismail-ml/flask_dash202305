import os
import pandas as pd
import glob,time
# import dask.dataframe as dd
import numpy as np
import datetime
t1=time.time()

yesterday = datetime.datetime.now() - datetime.timedelta(1)
print(yesterday)
file_name=datetime.datetime.strftime(yesterday,"%B_%Y")
os.chdir('/home/ismayil/flask_dash/data')
pool='/home/ismayil/flask_dash/data/nokia/pool/'
# import and get ready tracker file
tracker=pd.read_csv('/home/ismayil/flask_dash/support_files/tracker.csv')

# Check data integrity for all folders
print('Checking data integrity in all folders')
folder_main="/home/ismayil/flask_dash/data"
sub_folder=['nokia/2g/1','nokia/2g/2','nokia/3g/1','nokia/3g/2','nokia/4g/1',r'huawei/2g/1',
            r'huawei/2g/2',r'huawei/2g/3',r'huawei/3g/1',r'huawei/3g/2',r'huawei/3g/3',r'huawei/4g/1',r'huawei/4g/2',r'huawei/2g/ta_inter']
folder_info={}
for i in sub_folder:
    if os.path.isdir(os.path.join(folder_main,i)):
        folder_info[i]=len(os.listdir(os.path.join(folder_main,i)))
    else:
        with open(r"/home/ismayil/flask_dash/import_result.txt","a") as f:
            f.write(str('There is missing folder for yesterday in:'+ i+'\n'))
        folder_info[i]=0
for value in folder_info.keys():
    if folder_info[value]<=1:
        with open(r"/home/ismayil/flask_dash/import_result.txt","a") as f:
            f.write(str('There is missing data in yesterdays folder:'+ value+'\n'))
        if 'nokia' in value:
            with open(r"/home/ismayil/flask_dash/import_result.txt","a") as f:
                f.write(str('That missing data in nokia folder. STOP PROCESSING /n'))
if (folder_info[r'huawei/2g/1']+folder_info[r'huawei/2g/2']+folder_info[r'huawei/2g/3'])/folder_info[r'huawei/2g/1']!=3:
    with open(r"/home/ismayil/flask_dash/import_result.txt","a") as f:
        f.write('There is missing data for yesterday in one of the 2G huawei folders:'+'\n')
        f.write(str(r'huawei/2g/1 =' + str(folder_info[r'huawei/2g/1'])+'\n'))
        f.write(str(r'huawei/2g/2 =' + str(folder_info[r'huawei/2g/2'])+'\n'))
        f.write(str(r'huawei/2g/3 =' + str(folder_info[r'huawei/2g/3'])+'\n'))
if (folder_info[r'huawei/3g/1']+folder_info[r'huawei/3g/2'])/folder_info[r'huawei/3g/1']!=2:
    with open(r"/home/ismayil/flask_dash/import_result.txt","a") as f:
        f.write('There is missing data for yesterday in one of the 3G huawei folders:'+'\n')
        f.write(str(r'huawei/3g/1 =' + str(folder_info[r'huawei/3g/1'])+'\n'))
        f.write(str(r'huawei/3g/2 =' + str(folder_info[r'huawei/3g/2'])+'\n'))
        f.write(str(r'huawei/3g/3 =' + str(folder_info[r'huawei/3g/3'])+'\n'))
if (folder_info[r'huawei/4g/1']+folder_info[r'huawei/4g/2'])/folder_info[r'huawei/4g/1']!=2:
    with open(r"/home/ismayil/flask_dash/import_result.txt","a") as f:
        f.write('There is missing data for yesterday in one of the 4G huawei folders:'+'\n')
        f.write(str(r'huawei/4g/1 =' + str(folder_info[r'huawei/4g/1'])+'\n'))
        f.write(str(r'huawei/4g/2 =' + str(folder_info[r'huawei/4g/2'])+'\n'))
if not os.path.isfile(r'/home/ismayil/flask_dash/import_result.txt'):
    file_status='o-la-la'
    print(file_status)
print('Integrity checked successfully')


print(os.getcwd())
counter=0
os.chdir('/home/ismayil/flask_dash/support_files/scripts')
# Aggregate data
if 1:#file_status=='o-la-la':
    import nokia_2g, nokia_3g, nokia_4g#, huawei_2g, huawei_3g, huawei_4g#,core_traf_cmg#,utilization_hourly
    print('imported sss')
    
    #try:
    #    huawei_2g.run(*[os.path.join(folder_main,sub_folder[i]) for i in (5,6,7,13)],tracker)
    #    for j in [5,6,7,13]:
    #        pd.DataFrame(os.listdir(os.path.join(folder_main, sub_folder[j]))).to_csv(os.path.join(folder_main, 'files.csv'), mode='a', header=None, index=False)
    #except: 1
    #try:
    #    huawei_3g.run(*[os.path.join(folder_main,sub_folder[i]) for i in (8,9,10)],tracker)
    #    for j in [8, 9, 10]:
    #        pd.DataFrame(os.listdir(os.path.join(folder_main, sub_folder[j]))).to_csv(os.path.join(folder_main, 'files.csv'), mode='a', header=None, index=False)
    #except: 1
    #try:
    #    huawei_4g.run(*[os.path.join(folder_main,sub_folder[i]) for i in (11,12)],tracker)
    #    for j in [11, 12]:
    #        pd.DataFrame(os.listdir(os.path.join(folder_main, sub_folder[j]))).to_csv(os.path.join(folder_main, 'files.csv'), mode='a', header=None, index=False)
    #except: 1
    try:
        a=nokia_2g.run(*[os.path.join(folder_main, sub_folder[i]) for i in (0, 1)], tracker)
        #pd.DataFrame(glob.glob(pool + '*.zip')).replace({pool: '2G_'}, regex=True).to_csv(os.path.join(folder_main, 'files.csv'), mode='a', header=None, index=False)
        counter += a
    except: 1
    try:
        a=nokia_3g.run(*[os.path.join(folder_main, sub_folder[i]) for i in (2, 3)], tracker)
        #pd.DataFrame(glob.glob(pool + '*.zip')).replace({pool: '3G_'}, regex=True).to_csv(os.path.join(folder_main, 'files.csv'), mode='a', header=None, index=False)
        counter += a
    except: 1
    try:
        a=nokia_4g.run(os.path.join(folder_main, sub_folder[4]), tracker)
        #pd.DataFrame(glob.glob(pool + '*.zip')).replace({pool: '4G_'}, regex=True).to_csv(os.path.join(folder_main, 'files.csv'), mode='a', header=None, index=False)
        counter += a
    except : 1
    #try:
    #    a=core_traf_cmg.run(os.path.join(folder_main, sub_folder[4]))
    #    #pd.DataFrame(glob.glob(pool + '*.zip')).replace({pool: '4G_'}, regex=True).to_csv(os.path.join(folder_main, 'files.csv'), mode='a', header=None, index=False)
    #    counter += a
    #except : 1
    
    print(counter,'counter')
    if counter==3:
        pd.DataFrame(glob.glob(pool + '*.zip')).replace({pool: ''}, regex=True).to_csv(os.path.join(folder_main, 'files.csv'), mode='a', header=None, index=False)
else: print('Problem with file integrity. That is why data is not aggregated.')
#sub_folder.append('core')
#for j in range(len(sub_folder)):
#    pd.DataFrame(os.listdir(os.path.join(folder_main,sub_folder[j]))).to_csv(os.path.join(folder_main,'files.csv'),mode='a',header=None,index=False) 
#utilization_hourly.run()
required=datetime.datetime.strftime(datetime.datetime.now()-datetime.timedelta(hours=1),'%Y-%m-%d %H:00')
file_name2=datetime.datetime.strftime(datetime.datetime.now()-datetime.timedelta(hours=1),"%Y-%m-%d")
def add_huawei(required,file_name2):
    try:
        pd.read_hdf(r'/disk2/support_files/archive/ran/' + file_name2 + '.h5', '/twoG/bsc',where='Date=required and Vendor="Huawei"').\
            drop(columns=['band_1','band_2','band_3','band_4','band_5']).\
            to_hdf(r'/disk2/support_files/archive/combined_bsc.h5', '/twoG',append=True,
                    format='table', data_columns=['Date', 'BSC_name', 'Vendor', 'Region'], complevel=5,
                    min_itemsize={'BSC_name': 10, 'Vendor': 10, 'Region': 15})
        pd.read_hdf(r'/disk2/support_files/archive/ran/' + file_name2 + '.h5', '/threeG/bsc',where='Date=required and Vendor="Huawei"').\
            drop(columns=['rtwp_num','rtwp_den']).to_hdf(r'/disk2/support_files/archive/combined_bsc.h5', '/threeG',
                append=True,format='table', data_columns=['Date', 'RNC_name', 'Vendor', 'Region'], complevel=5,
                min_itemsize={'RNC_name': 10, 'Vendor': 10, 'Region': 15})
        pd.read_hdf(r'/disk2/support_files/archive/ran/' + file_name2 + '.h5', '/fourG/bsc',where='Date=required and Vendor="Huawei"')\
            [['Date','Vendor','Region','volte_sr_num', 'volte_sr_den', 'volte_dr_num', 'volte_dr_den', 'volte_dl_ps_traf',
                'volte_ul_ps_traf', 'volte_cs_traf','volte_srvcc_e2w_num','volte_srvcc_e2w_den','Voice_VQI_DL_Accept_Times','Voice_VQI_DL_Bad_Times','Voice_VQI_DL_Excellent_Times',
                'Voice_VQI_DL_Good_Times','Voice_VQI_DL_Poor_Times','Voice_VQI_DL_TotalValue','Voice_VQI_UL_Accept_Times',
                'Voice_VQI_UL_Bad_Times','Voice_VQI_UL_Excellent_Times','Voice_VQI_UL_Good_Times','Voice_VQI_UL_Poor_Times',
                'Voice_VQI_UL_TotalValue','Voice_DL_Silent_Num','Voice_UL_Silent_Num']].to_hdf(r'/disk2/support_files/archive/combined_bsc.h5', '/fourGn',
                append=True,format='table', data_columns=['Date', 'Vendor', 'Region'],complevel=5,
                min_itemsize={'Vendor': 10, 'Region': 15})
        pd.read_hdf(r'/disk2/support_files/archive/ran/' + file_name2 + '.h5', '/fourG/bsc',where='Date=required and Vendor="Huawei"').\
            drop(columns=['volte_sr_num','volte_sr_den','volte_dr_num','volte_dr_den','volte_dl_ps_traf',
                'volte_ul_ps_traf','volte_cs_traf','rtwp','volte_srvcc_e2w_num','volte_srvcc_e2w_den','Voice_VQI_DL_Accept_Times','Voice_VQI_DL_Bad_Times','Voice_VQI_DL_Excellent_Times',
                'Voice_VQI_DL_Good_Times','Voice_VQI_DL_Poor_Times','Voice_VQI_DL_TotalValue','Voice_VQI_UL_Accept_Times',
                'Voice_VQI_UL_Bad_Times','Voice_VQI_UL_Excellent_Times','Voice_VQI_UL_Good_Times','Voice_VQI_UL_Poor_Times',
                'Voice_VQI_UL_TotalValue','Voice_DL_Silent_Num','Voice_UL_Silent_Num']).to_hdf(r'/disk2/support_files/archive/combined_bsc.h5', '/fourG',append=True,
                    format='table', data_columns=['Date', 'Vendor', 'Region'], complevel=5,
                    min_itemsize={'Vendor': 10, 'Region': 15})
    except Exception as p:
        print(p)
        print('exception raised during huawei saving')
        1
add_huawei(required,file_name2)
print('add_huawei done for ',required)
if (datetime.datetime.now()-datetime.timedelta(hours=1)).hour==2:
    u=datetime.datetime.strftime(datetime.datetime.now()-datetime.timedelta(hours=1),'%Y-%m-%d 00:00')
    if len(pd.read_hdf(r'/disk2/support_files/archive/combined_bsc.h5', '/twoG',where='Date=u and Vendor="Huawei"'))<1:
        add_huawei(u,file_name2)
print('Total proccess took ', time.time()-t1)
