import pandas as pd
import glob
import os

######################## Board Availability ########################
for mno in ['BKC','AZF']:
    d=[]
    if mno == 'BKC':
        for i in glob.glob('*.h5'):
            if 'azf' in i:continue
            if '2022-05-19' in i: continue
            d.append(pd.read_hdf(i,'OSRuntime'))
    else:
        for i in glob.glob('*azf_*.h5'):
            if '2022-05-19' in i: continue
            d.append(pd.read_hdf(i,'OSRuntime'))
    br = pd.concat(d)
    br.drop_duplicates(inplace=True)
    br.sort_values(by=['UserLabel','Date'],inplace=True)
    br.reset_index(inplace=True,drop=True)
    br['check']=br['UserLabel']==br['UserLabel'].shift(1)
    br['interval']=br['Date']-br['Date'].shift(1)
    br['nese']=br['interval']*br['check']
    br['interval']=br['nese'].apply(lambda x: x.total_seconds()/60/15)
    br.loc[br['check']==False,'interval']=1
    br['hwHostSysUptime']=br['hwHostSysUptime'].astype(int)
    #br[br['hwHostSysUptime']!=2147483647]
    br['delta']=(br['hwHostSysUptime']-br['hwHostSysUptime'].shift(1))*br['check']
    br['delta2']=br['delta']
    br.loc[br['hwHostSysUptime']!=2147483647,'delta2']=(br.loc[br['hwHostSysUptime']!=2147483647,'delta']-100*60*15*br.loc[br['hwHostSysUptime']!=2147483647,'interval'])/100
    br.loc[abs(br['delta2'])<10,'delta2']=0
    br.loc[br['check']==False,'delta2']=0
    br['availability']=(15*60*br['interval']+br['delta2'])/(15*60*br['interval'])*100
    br['avail_num']=15*60*br['interval']+br['delta2']
    br['avail_den']=15*60*br['interval']
    a=br.groupby('Date').sum()[['avail_num','avail_den']]
    b=a.resample('1H').sum()
    b['Availability']=b['avail_num']/b['avail_den']*100
    b.insert(0,'MNO',mno)
    if os.path.exists('board_availability.csv'):
        b.reset_index().to_csv('board_availability.csv',index=False,mode='a',header=False)
    else: b.reset_index().to_csv('board_availability.csv',index=False,mode='a')
    

    #################### DB Availability ############################
    d=[]
    if mno == 'BKC':
        for i in glob.glob('*.h5'):
            if 'azf' in i:continue
            if '2022-05-19' in i: continue
            d.append(pd.read_hdf(i,'persistRunningTime'))
    else:
        for i in glob.glob('*azf_*.h5'):
            if '2022-05-19' in i: continue
            d.append(pd.read_hdf(i,'persistRunningTime'))
    dbr=pd.concat(d)
    dbr.drop_duplicates(inplace=True)
    dbr.sort_values(by=['UserLabel','Date'],inplace=True)
    dbr.reset_index(inplace=True,drop=True)
    dbr['check']=dbr['UserLabel']==dbr['UserLabel'].shift(1)
    dbr['interval']=dbr['Date']-dbr['Date'].shift(1)
    dbr['nese']=dbr['interval']*dbr['check']
    dbr['interval']=dbr['nese'].apply(lambda x: x.total_seconds()/60/5)
    dbr.loc[dbr['check']==False,'interval']=1
    dbr['hwDbSysUptime']=dbr['hwDbSysUptime'].astype(int)
    #br[br['hwHostSysUptime']!=2147483647]
    dbr['delta']=(dbr['hwDbSysUptime']-dbr['hwDbSysUptime'].shift(1))*dbr['check']
    dbr.loc[:,'delta2']=(dbr.loc[:,'delta']-60*5*dbr.loc[:,'interval'])
    dbr.loc[abs(dbr['delta2'])<10,'delta2']=0
    dbr.loc[dbr['delta']==0,'delta2']=0
    dbr.loc[dbr['check']==False,'delta2']=0
    dbr['availability']=(5*60*dbr['interval']+dbr['delta2'])/(5*60*dbr['interval'])*100
    dbr['avail_num']=5*60*dbr['interval']+dbr['delta2']
    dbr['avail_den']=5*60*dbr['interval']
    dbr['avail_num_corrected']=dbr['avail_num']
    filt=abs(dbr['avail_den']-dbr['avail_num'])<50
    dbr.loc[filt,'avail_num_corrected']=dbr.loc[filt,'avail_den']
    a=dbr.groupby('Date').sum()[['avail_num_corrected','avail_num','avail_den']]
    b=a.resample('1H').sum()
    b['Availability']=b['avail_num_corrected']/b['avail_den']*100
    b.loc[b['Availability']>100,'Availability']=100
    b.insert(0,'MNO',mno)
    if os.path.exists('db_availability.csv'):
        b.reset_index().to_csv('db_availability.csv',index=False,mode='a',header=False)
    else: b.reset_index().to_csv('db_availability.csv',index=False,mode='a')
    

    d=[]
    if mno == 'BKC':
        for i in glob.glob('*.h5'):
            if 'azf' in i:continue
            if '2022-05-19' in i: continue
            d.append(pd.read_hdf(i,'Storage'))
    else:
        for i in glob.glob('*azf_*.h5'):
            if '2022-05-19' in i: continue
            d.append(pd.read_hdf(i,'Storage'))

    s=pd.concat(d)
    s.iloc[:,8:]=s.iloc[:,8:].astype(int)
    s['util']=(s['TotalSize']-s['FreeSize'])/s['TotalSize']*100
    s['day']=s['Date'].dt.date
    #b=round(s.pivot_table(index=['LocalDn','UserLabel','StorageName','hrStorageIndex','StorageDescr'],
    #            columns='day',values=['TotalSize','FreeSize','util'],aggfunc=['mean','median','max']),2)
    b=round(s.groupby(['day','LocalDn','UserLabel','StorageName','hrStorageIndex','StorageDescr']).agg(['mean','median','max'])['util'],2)
    b.insert(0,'MNO',mno)
    if os.path.exists('db_storage.csv'):
        b.reset_index().to_csv('db_storage.csv',index=False,mode='a',header=False)
    else: b.reset_index().to_csv('db_storage.csv',index=False,mode='a')
