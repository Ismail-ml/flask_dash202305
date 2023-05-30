import os
import pandas as pd
import glob,time
# import dask.dataframe as dd
import numpy as np
import datetime
t1=time.time()

time.sleep(10)
yesterday = datetime.datetime.now() - datetime.timedelta(1)
print(yesterday)
file_name=datetime.datetime.strftime(yesterday,"%B_%Y")
os.chdir('/home/ismayil/flask_dash/data')
pool='/home/ismayil/flask_dash/data/nokia/'
try:
    
    # import and get ready tracker file
    tracker=pd.read_csv('/home/ismayil/flask_dash/support_files/tracker.csv')
    
    # Check data integrity for all folders
    print('Checking data integrity in all folders')
    folder_main="/home/ismayil/flask_dash/data"
    sub_folder=['nokia/2g/1','nokia/2g/2','nokia/3g/1','nokia/3g/2','nokia/4g/1',r'huawei/2g/1',
               r'huawei/2g/2',r'huawei/2g/3',r'huawei/3g/1',r'huawei/3g/2',r'huawei/3g/3',r'huawei/4g/1',r'huawei/4g/2',r'huawei/2g/ta_inter']
  
    print(os.getcwd())
    counter=0
    # Aggregate data
    import texnofest_2G, texnofest_3G, texnofest_4G, texnofest_4G_nokia, texnofest_5G
    print('imported sss')
    
    try:
        texnofest_4G.run('home/ismayil/flask_dash/data/huawei',tracker)
        pd.DataFrame(os.listdir('/home/ismayil/flask_dash/data/huawei')).to_csv(os.path.join(folder_main, 'files.csv'), mode='a', header=None, index=False)
        [os.remove(i) for i in glob.glob('/home/ismayil/flask_dash/data/huawei/*4G*.csv')]
    except: 1
    try:
        a=texnofest_2G.run(*[os.path.join(folder_main, sub_folder[i]) for i in (0, 1)], tracker)
        #pd.DataFrame(glob.glob(pool + '*.zip')).replace({pool: '2G_'}, regex=True).to_csv(os.path.join(folder_main, 'files.csv'), mode='a', header=None, index=False)
        counter += a
    except: 1
    try:
        a=texnofest_3G.run(*[os.path.join(folder_main, sub_folder[i]) for i in (2, 3)], tracker)
        #pd.DataFrame(glob.glob(pool + '*.zip')).replace({pool: '3G_'}, regex=True).to_csv(os.path.join(folder_main, 'files.csv'), mode='a', header=None, index=False)
        counter += a
    except: 1
    try:
        a=texnofest_4G_nokia.run(sub_folder[4], tracker)
        #pd.DataFrame(glob.glob(pool + '*.zip')).replace({pool: '3G_'}, regex=True).to_csv(os.path.join(folder_main, 'files.csv'), mode='a', header=None, index=False)
        counter += a
    except: 1
    try:
        a=texnofest_5G.run(sub_folder[4], tracker)
        #pd.DataFrame(glob.glob(pool + '*.zip')).replace({pool: '3G_'}, regex=True).to_csv(os.path.join(folder_main, 'files.csv'), mode='a', header=None, index=False)
        counter += a
    except: 1
    
    print(counter,'counter')
    if counter==4:
        pd.DataFrame(glob.glob(pool + '*.zip')).replace({pool: ''}, regex=True).to_csv(os.path.join(folder_main, 'files.csv'), mode='a', header=None, index=False)
    pd.read_csv('/home/ismayil/flask_dash/data/files.csv').drop_duplicates().to_csv('/home/ismayil/flask_dash/data/files.csv',index=False)
    [os.remove(i) for i in glob.glob(pool + '*.zip')]    
    print('Total proccess took ', time.time()-t1)
except Exception as e:
    print(e)

