import pandas as pd
import os, datetime
import time
import glob
from datetime import datetime as dt
time.sleep(10)


needed_files=['IG41022','IG27','IG30014','IG30024','IGMSTP','IG30029','IGRTN']
#'',''
os.chdir('/home/ismayil/flask_dash/data/tx')

existing_files = pd.read_csv('/home/ismayil/flask_dash/data/files_tx.csv', header=None)
k=0
for j in needed_files:
    try:
       print(j, 'begined')
       df = pd.concat([pd.read_csv(i, skiprows=[0]) for i in os.listdir() if (j in i)])
       df['CollectionTime'] = pd.to_datetime(df['CollectionTime'])
       df.iloc[:,5:]=df.iloc[:,5:].astype('float')
       for i in df['CollectionTime'].dt.date.unique():
           file_name = datetime.datetime.strftime(i,"%d.%m.%Y")
           print(i)
           df.loc[df['CollectionTime'].dt.date==i].to_hdf('/disk2/support_files/archive/tx/tx_'+file_name+'.h5', j, append=True, format='table', data_columns=df.columns[1:4], complevel=5,
              min_itemsize={'DeviceName': 100, 'ResourceName': 100})
       print('append finish')
    except:
       print('file failed ',j)
       continue
try:
    n=glob.glob('*.csv')[-1].find('_15_')+4
    with open('last_file.txt', 'w') as f:
        print(glob.glob('*.csv')[-1][n:n+12])
        print(glob.glob('*.csv')[-1])
        f.write(glob.glob('*.csv')[-1][n:n+12])
    pd.DataFrame(glob.glob('*.csv')).to_csv(os.path.join('/home/ismayil/flask_dash/data', 'files_tx.csv'), mode='a', header=None, index=False)
    os.system('rm -f /home/ismayil/flask_dash/data/tx/*.csv')
except:
    print('find in glob error')
    1

# Nokia tx part

print('Nokia part begin')
dictionary={'rsl_hop':[],
'tsl_hop':[],
'rx_stats':[],
'tx_stats':[],
'modulation':[]}

for i in glob.glob('*.xml'):
    if (i in existing_files): continue
    try:
        a=pd.read_xml(i)
        a['Date']=a['timeCaptured'].apply(lambda d: dt.fromtimestamp(round(int(d)/1000/60,0)*60))
        if 'RSLHopCurrentDataStats15Min' in i:
            a.columns=a.columns.str.replace('rslHopCD','RSL_',regex=True)
            a[['Date','periodicTime',
            'monitoredObjectClass'
            ,'monitoredObjectPointer','displayedName','monitoredObjectSiteId','monitoredObjectSiteName',
            'RSL_AverageLeveldBm','RSL_MaximumLeveldBm','RSL_MinimumLeveldBm']]
            dictionary['rsl_hop'].append(a)
        elif 'TSLHopCurrentDataStats15Min' in i:
            a.columns=a.columns.str.replace('tslHopCD','TSL_',regex=True)
            a[['Date','periodicTime',
            'monitoredObjectClass'
            ,'monitoredObjectPointer','displayedName','monitoredObjectSiteId','monitoredObjectSiteName',
            'TSL_AverageLeveldBm','TSL_MaximumLeveldBm','TSL_MinimumLeveldBm']]
            dictionary['tsl_hop'].append(a)
        elif 'ethernetequipment.AggrMaintRxStats' in i:
            a.columns=a.columns.str.replace('aggr','',regex=True)
            a[['Date','periodicTime',
            'monitoredObjectClass'
            ,'monitoredObjectPointer','displayedName','monitoredObjectSiteId','monitoredObjectSiteName',
            'RxThroughput','RxThroughputPeriodic','RxUtilization','RxUtilizationPeriodic',
            'DiscardedFrameRatio','DiscardedFrameRatioPeriodic']]
            dictionary['rx_stats'].append(a)
        elif 'AggrMaintTxStats' in i:
            a.columns=a.columns.str.replace('aggr','',regex=True)
            a[['Date','periodicTime',
            'monitoredObjectClass'
            ,'monitoredObjectPointer','displayedName','monitoredObjectSiteId','monitoredObjectSiteName',
            'TxThroughput','TxThroughputPeriodic','TxUtilization','TxUtilizationPeriodic',
            'DiscardedFrameRatio','DiscardedFrameRatioPeriodic']]
            dictionary['tx_stats'].append(a)
        elif 'AdaptiveModulationCurrentDataStats15Min' in i:
            a.columns=a.columns.str.replace('adaptiveModulationCDUsageTime','',regex=True)
            a[[*['Date','periodicTime',
            'monitoredObjectClass'
            ,'monitoredObjectPointer','displayedName','monitoredObjectSiteId','monitoredObjectSiteName'],*list(a.columns[5:-10].values)]]
            dictionary['modulation'].append(a)
        else: continue
    except Exception as e:
        print(e)
        1
print('Nokia part end. time to save')
for j in dictionary.keys():
    if len(dictionary[j])>0:
        df=pd.concat(dictionary[j])
        temp_dict={'monitoredObjectClass': 200, 'monitoredObjectPointer': 200,
                        'displayedName': 200, 'monitoredObjectSiteId': 200,'monitoredObjectSiteName':200}
        for n in temp_dict.keys():
            #print(df[n])
            df[n].apply(lambda x: x[:temp_dict[n]] if len(x)>temp_dict[n] else x)
        for i in df['Date'].dt.date.unique():
                try:
                    file_name = datetime.datetime.strftime(i,"%d.%m.%Y")
                    print(i)
                    df.loc[df['Date'].dt.date==i].to_hdf('/disk2/support_files/archive/tx/tx_'+file_name+'.h5', j, append=True, format='table', 
                    data_columns=df.columns[:7], complevel=5,
                        min_itemsize={'monitoredObjectClass': 200, 'monitoredObjectPointer': 200,
                        'displayedName': 200, 'monitoredObjectSiteId': 200,'monitoredObjectSiteName':200})
                except Exception as e:
                    print(e)
                    1
pd.DataFrame(os.listdir()).to_csv(os.path.join('/home/ismayil/flask_dash/data', 'files_tx.csv'), mode='a', header=None, index=False)
os.system('rm -f /home/ismayil/flask_dash/data/tx/*.xml')
print('completely finished')
