import pandas as pd
import os
import zipfile
import datetime
import time
import glob

#apn_mapping=pd.read_csv(r'/home/ismayil/flask_dash/support_files/apn_mapping.csv')
apn_mapping=pd.read_excel('/mnt/raw_counters/Corporate Folder/CTO/Technology Governance and Central Support/Core QA/Mapping for schedule/mapping.xlsx',sheet_name='APN')
def run(path1):
    c1=time.time()
    an,pdp,s1=[],[],[]
    nsn=pd.DataFrame()
    path1 = '/home/ismayil/flask_dash/data/nokia/pool'
    print('CMG started')
    for dirpath,dirname,filenames in os.walk(path1):
    
        #existing_files=pd.read_csv(os.path.join(path1,'files.txt'), sep=" ", header=None)
        #all_files = glob.glob(dirpath + "/*.csv") working one ############
        all_files = glob.glob(dirpath + "/*Core_KPIs*.zip")
        #print('For dirpath:',dirpath,'len is:',len(all_files))
        if len(all_files)>0:
            li = []
            for filename in all_files:
                try:
                    if os.path.isfile(os.path.join('/home/ismayil/flask_dash/data', 'files.csv')):
                        existing_files = pd.read_csv(os.path.join('/home/ismayil/flask_dash/data', 'files.csv'), header=None)
                    else: existing_files=pd.DataFrame()
                    if (filename.replace('/home/ismayil/flask_dash/data/nokia/pool/','CMG_') in existing_files.values): continue
                    #if ('_05:00' in filename) and ('Main' in filename): continue
                    files2 = zipfile.ZipFile(filename, 'r')
                    if ('01_14_00' in filename) or ('00_14_00' in filename) or ('02_14_00' in filename): continue
                    #if 'CMM' not in files2.namelist()[-1]:
                    #    df = pd.read_csv(files2.open(files2.namelist()[-5]), sep=';')
                    #else: 
                    df = pd.read_csv(files2.open(files2.namelist()[0]), sep=';')    
                    df['Date']=df['PERIOD_START_TIME']
                    if ('04:00' in filename):
                            df = df[df['Date'].str.contains('00:00:00|01:|23:')]
                    df.rename(columns={'PGW Downlink Data Volume, APN':'4G DL',
                    'PGW Uplink Data Volume, APN':'4G UL',
                        'apn_id':'APN'},inplace=True)
                    df['2G/3G DL']=0
                    df['2G/3G UL']=0
                    df['4G DL']=df['4G DL']*1024
                    df['4G UL']=df['4G UL']*1024
                    df=df[['Date','APN','CMG name','4G DL','4G UL','2G/3G DL','2G/3G UL']]
                    df=df[~df['CMG name'].str.contains('CMG-CP')] 
                    an.append(df)
                    print('traf append finished')
                    # PDP and Bearer addition
                    try:
                        if (filename.replace('/home/ismayil/flask_dash/data/nokia/pool/','CMM_') in existing_files.values): continue
                    #    if ('_05:00' in filename) and ('Main' in filename): continue
                        #files2 = zipfile.ZipFile(filename, 'r')
                        if ('01_14_00' in filename) or ('00_14_00' in filename) or ('02_14_00' in filename): continue
                    #    if 'CMM' not in files2.namelist()[-4]:
                    #        continue
                        df = pd.read_csv(files2.open(files2.namelist()[2]), sep=';')    
                        df['Date']=df['PERIOD_START_TIME']
                        if ('04:00' in filename):
                                df = df[df['Date'].str.contains('00:00:00|01:|23:')]
                        df.rename(columns={'SUCC_MO_PDP_CONTEXT_ACT (M02C000)':'TwoG_pdp_num', 'IU_SUCC_MO_PDP_CON_ACT (M17C000)':'ThreeG_pdp_num',
                                    'ACT_DFLT_EPS_BRR_ATT (M103C193)':'bearer_setup_den', 'ACT_DFLT_EPS_BRR_SUCC (M103C194)':'bearer_setup_num',
                                    'SUCC_GPRS_ATTACH (M01C000)':'TwoG_attach_num', 'IU_SUCC_GPRS_ATTACH (M16C000)':'ThreeG_attach_num',
                                    'Gb MO PDP context activation attempts':'TwoG_pdp_den', 'Gb GPRS attach attempts, e2e':'TwoG_attach_den',
                                    'Iu GPRS attach attempts, e2e':'ThreeG_attach_den', 'Iu MO PDP activation attempts, e2e':'ThreeG_pdp_den','CMM name':'Site'},inplace=True)
                        #print(df.columns,' middle of the pdp aggregation')
                        df=df.groupby(['Date','Site']).sum().reset_index()
                        df[['TwoG_attach_num','TwoG_attach_den','ThreeG_attach_num','ThreeG_attach_den',
                                'TwoG_pdp_num','TwoG_pdp_den','ThreeG_pdp_num','ThreeG_pdp_den','bearer_setup_num','bearer_setup_den']]=df[['TwoG_attach_num','TwoG_attach_den','ThreeG_attach_num','ThreeG_attach_den',
                                'TwoG_pdp_num','TwoG_pdp_den','ThreeG_pdp_num','ThreeG_pdp_den','bearer_setup_num','bearer_setup_den']].astype(float)
                        pdp.append(df[['Date','Site','TwoG_attach_num','TwoG_attach_den','ThreeG_attach_num','ThreeG_attach_den',
                                'TwoG_pdp_num','TwoG_pdp_den','ThreeG_pdp_num','ThreeG_pdp_den']])
                        s1.append(df[['Date','Site','bearer_setup_num','bearer_setup_den']])
                        print('pdp append finished')
                    except Exception as e:
                        print(e,' exception was raised')
                        continue
                except Exception as e:
                    print(e, filename,' outer exception was raised')
                    1
    print(time.time()-c1,'loop finished')
    nsn = pd.concat(an, axis=0, ignore_index=True, sort=False)
    nsn=nsn.merge(apn_mapping,how='left', left_on='APN', right_on='APN ID')
    nsn['Date']=pd.to_datetime(nsn['Date'],format='%m.%d.%Y %H:%M:%S')
    nsn.rename(columns={'CMG name':'CMG_name'},inplace=True)
    nsn=nsn[['Date','MNO','APN','CMG_name','2G/3G DL','2G/3G UL','4G DL','4G UL']]
    nsn[['2G/3G DL','2G/3G UL','4G DL','4G UL']]=nsn[['2G/3G DL','2G/3G UL','4G DL','4G UL']].astype('float')
    print(time.time()-c1,'time to save traf')
    nsn.drop_duplicates(inplace=True)

    for j in nsn['Date'].unique():
            try:
                #file_name = datetime.datetime.strftime(datetime.datetime.utcfromtimestamp(j.astype(datetime.datetime) / 1e9),"%B_%Y")
                file_name2 = datetime.datetime.strftime(datetime.datetime.utcfromtimestamp(j.astype(datetime.datetime) / 1e9),"%Y-%m-%d")
                a=pd.read_hdf('/disk2/support_files/archive/core/core_'+file_name2 +'.h5', 'traf', where='Date=j and CMG_name="10.34.170.1@CSN3_CMG-UP"')
                if len(a)>0:
                    print(j,'not appended')
                    continue
                nsn.loc[nsn['Date']==j].to_hdf('/disk2/support_files/archive/core/core_'+file_name2+'.h5','traf',append=True,
                        format='table', data_columns=['Date', 'APN', 'MNO','CMG_name'], complevel=5,
                        min_itemsize={'APN': 100, 'MNO': 20, 'CMG_name': 100})
            except Exception as e:
                print(e)
                print(j,' error')
                1

    print('Nokia pdp and bearer begin ')
    for u in ['pdp','s1']:
        if len(eval(u))>0:
            nsn = pd.concat(eval(u), axis=0, ignore_index=True, sort=False)
            nsn['Date']=pd.to_datetime(nsn['Date'],format='%m.%d.%Y %H:%M:%S')
            print(time.time()-c1,'time to save pdp')
            nsn.drop_duplicates(inplace=True)
            for j in nsn['Date'].unique():
                    try:
                        #file_name = datetime.datetime.strftime(datetime.datetime.utcfromtimestamp(j.astype(datetime.datetime) / 1e9),"%B_%Y")
                        file_name2 = datetime.datetime.strftime(datetime.datetime.utcfromtimestamp(j.astype(datetime.datetime) / 1e9),"%Y-%m-%d")
                        nsn.loc[nsn['Date']==j].to_hdf('/disk2/support_files/archive/core/core_'+file_name2+'.h5',u,append=True,
                                format='table', data_columns=['Date', 'Site'], complevel=5,
                                min_itemsize={'Site': 100})
                    except Exception as e:
                        print(e)
                        print(j)
                        print(u, 'error')
                        1
    
    pd.DataFrame(glob.glob(path1 + '/*Core_KPIs*.zip')).replace({'/home/ismayil/flask_dash/data/nokia/pool/': 'CMG_'}, regex=True).to_csv(os.path.join('/home/ismayil/flask_dash/data', 'files.csv'), mode='a', header=None, index=False)
    pd.DataFrame(glob.glob(path1 + '/*Core_KPIs*.zip')).replace({'/home/ismayil/flask_dash/data/nokia/pool/': 'CMM_'}, regex=True).to_csv(os.path.join('/home/ismayil/flask_dash/data', 'files.csv'), mode='a', header=None, index=False)
    
    print(time.time()-c1,'all finished')

    return 1
