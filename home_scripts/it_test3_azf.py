f={}
import numpy as np
import xmltodict
import pandas as pd
import glob
import datetime
import os, re
import concurrent.futures as cf
#import shutil
#shutil.unpack_archive('logs.zip',extract_dir='test')
dict = {'CPU':{'Date':20,'LocalDn':200,'UserLabel':200},
'hwBillingDCCMsgNumber':{'Date':20,'LocalDn':200,'UserLabel':200,'hwNetDevice':200,'hwEventType':50},
'hwBillingDCCMsgNumber_NT':{'Date':20,'LocalDn':200,'UserLabel':200,'hwNetDevice':200,'hwEventType':50},
'hwBillingDCCSessionNumber':{'Date':20,'LocalDn':200,'UserLabel':200,'hwNetDevice':200,'hwEventType':50},
'hwBillingDCCSessionNumber_NT':{'Date':20,'LocalDn':200,'UserLabel':200,'hwNetDevice':200},
'hwBillingDCCSeviceCallNumber':{'Date':20,'LocalDn':200,'UserLabel':200,'hwServiceName':100},
'hwBillingDCCSeviceCallNumber_NT':{'Date':20,'LocalDn':200,'UserLabel':200,'hwServiceName':100},
'hwForwardDiamMsgStat':{'Date':20,'LocalDn':200,'UserLabel':200,'hwEventType':50},
'hwOfflineStat':{'Date':20,'LocalDn':200,'UserLabel':200,'hwNetDevice':200,'hwEventType':50},
'hwOfflineStat_NT':{'Date':20,'LocalDn':200,'UserLabel':200,'hwNetDevice':200,'hwEventType':50},
'LoadAvrg':{'Date':20,'LocalDn':200,'UserLabel':200},
'LogicalCPU':{'Date':20,'LocalDn':200,'UserLabel':200,'LogicalCPUName':100},
'Memory':{'Date':20,'LocalDn':200,'UserLabel':200},
'OSRuntime':{'Date':20,'LocalDn':200,'UserLabel':200},
'persistRunningTime':{'Date':20,'LocalDn':200,'UserLabel':200},
'serviceDatabaseInf':{'Date':20,'LocalDn':200,'UserLabel':200,'databaseName':200},
'Storage':{'Date':20,'LocalDn':200,'UserLabel':200,'StorageName':200,'hrStorageIndex':200,'StorageDescr':200},
'VirtualMemory':{'Date':20,'LocalDn':200,'UserLabel':200}
}

os.chdir('/home/qarftp/IT/AZF')
d=[]
def proccess_excel(diddd):
    try:
    	xml_data = open(diddd, 'r').read()  # Read data
    	if re.search('&WIFI',xml_data)!=None:
        	xml_data = re.sub(re.search('&WIFI',xml_data)[0], '_WIFI', xml_data)
    	xmlDict = xmltodict.parse(xml_data)  # Parse XML
    except Exception as e:
    	print(e)
    print(diddd)
    if len(xmlDict['measCollecFile']['measData'])<1:
        os.remove(diddd)
    else:
        for j in range(len(xmlDict['measCollecFile']['measData'])):
            try:
                k1=pd.DataFrame(xmlDict['measCollecFile']['measData'][j]['measInfo']).T
                #print(k1)
                if k1.columns[0]==0:
                    for i in k1.columns:
                        if k1.loc['@measInfoId'][i] not in dict.keys(): continue
                        h=pd.DataFrame(np.array(k1.loc['measValue',i]['measResults'].split(' ')).reshape(1,-1),columns=k1.loc['measTypes',i].split(' '))
                        h.insert(0,'Date',k1.loc['granPeriod',i]['@beginTime'].replace('T',' ').replace('+04:00',''))
                        h.insert(1,'Period',k1.loc['granPeriod',i]['@duration'].replace('PT',''))
                        h.insert(2,'LocalDn',xmlDict['measCollecFile']['measData'][j]['managedElement']['@localDn'])
                        h.insert(3,'UserLabel',xmlDict['measCollecFile']['measData'][j]['managedElement']['@userLabel'])
                        h['Date']=pd.to_datetime(h['Date'])
                        if k1.loc['@measInfoId'][i] in f.keys():
                            f[k1.loc['@measInfoId'][i]]=pd.concat([f[k1.loc['@measInfoId'][i]],h],ignore_index=True)
                        else:
                            f[k1.loc['@measInfoId'][i]]=h
                else:
                    if k1.loc['@measInfoId','measResults'] not in dict.keys(): continue
                    h=pd.DataFrame(np.array(k1.loc['measValue','measResults'].split(' ')).reshape(1,-1),
                                                            columns=k1.loc['measTypes','measResults'].split(' '))
                    h.insert(0,'Date',k1.loc['granPeriod','@beginTime'].replace('T',' ').replace('+04:00',''))
                    h.insert(1,'Period',k1.loc['granPeriod','@duration'].replace('PT',''))
                    h.insert(2,'LocalDn',xmlDict['measCollecFile']['measData'][j]['managedElement']['@localDn'])
                    h.insert(3,'UserLabel',xmlDict['measCollecFile']['measData'][j]['managedElement']['@userLabel'])
                    h['Date']=pd.to_datetime(h['Date'])
                    if k1.loc['@measInfoId','measResults'] in f.keys():
                        f[k1.loc['@measInfoId','measResults']]=pd.concat([f[k1.loc['@measInfoId','measResults']],h],ignore_index=True)
                    else:
                        f[k1.loc['@measInfoId','measResults']]=h
                
            except Exception as e:
                print(e)
                print(j, i)
                continue
        d.append(diddd)

print(os.listdir())
with cf.ThreadPoolExecutor() as executor:
        executor.map(proccess_excel,os.listdir())
    
print('loop finished. time to save')

for j in f.keys():
    a=f[j]
    dict2=dict[j].copy()
    dict2.pop('Date')
    for m in a.columns:
        if m not in dict2.keys():
            dict2[m]=20
    for i in a['Date'].dt.date.unique():
        try:
        #file_name = datetime.datetime.strftime(datetime.datetime.utcfromtimestamp(i.astype(datetime.datetime)/1e9), "%B_%Y")
            file_name2 = datetime.datetime.strftime(i, "%Y-%m-%d")
            a.loc[a['Date'].dt.date==i].to_hdf(r'/home/qarftp/azf_' + file_name2 + '.h5', j , append=True,
                                format='table', data_columns=dict[j].keys(),
                                complevel=5,
                                min_itemsize=dict2)
        except Exception as e:
            print(e, j, i)
            1
for i in set(d): 
    try:
        os.remove(i)
    except Exception as e:
        print(e, i)
        1

