import numpy as np
import xmltodict
import pandas as pd
import glob
import datetime
import os
import concurrent.futures as cf
from datetime import datetime as dt
import datetime
import time


time.sleep(15)
#import shutil
#shutil.unpack_archive('logs.zip',extract_dir='test')
dict = {'CPU':{'Date':20,'LocalDn':200,'UserLabel':200},
'hwBillingDCCMsgNumber':{'Date':20,'LocalDn':200,'UserLabel':200,'hwNetDevice':200,'hwEventType':50},
'hwBillingDCCAccessMsgNumberV4':{'Date':20,'LocalDn':200,'UserLabel':200,'hwNetDevice':200,'hwEventType':50},
'hwBillingDCCMsgNumberV4':{'Date':20,'LocalDn':200,'UserLabel':200,'hwNetDevice':200,'hwEventType':50},
'hwBillingDCCMsgNumber_NT':{'Date':20,'LocalDn':200,'UserLabel':200,'hwNetDevice':200,'hwEventType':50},
'hwBillingDCCSessionNumber':{'Date':20,'LocalDn':200,'UserLabel':200,'hwNetDevice':200,'hwEventType':50},
'hwBillingDCCAccessSessionNum':{'Date':20,'LocalDn':200,'UserLabel':200,'hwNetDevice':200,'hwEventType':50},
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

f={}
os.chdir('/home/ismayil/flask_dash/data/it/BKC')
for i in os.listdir():
    if (os.path.getsize(i)<2500) and ('xml' in i): os.remove(i)

d=[]
def proccess_excel(diddd):
    xml_data = open(diddd, 'r').read()  # Read data
    if xml_data.rfind('/measCollecFile')==-1:
        l=xml_data.rfind('/measInfo')
        xml_data=xml_data[:l+10]
        xml_data=xml_data+'\n</measData>\n</measCollecFile>'
    xmlDict = xmltodict.parse(xml_data)  # Parse XML
    #print(diddd)
    if len(xmlDict['measCollecFile']['measData'])>=3:
        for j in range(len(xmlDict['measCollecFile']['measData'])):
            #print(j)
            #print(xmlDict['measCollecFile']['measData'][j])
            try:
                k1=pd.DataFrame(xmlDict['measCollecFile']['measData'][j]['measInfo']).T
                if k1.columns[0]==0:
                    for i in k1.columns:
                        try:
                            if k1.loc['@measInfoId'][i] not in dict.keys(): continue
                            if (k1.loc['@measInfoId'][i]=='NeStat'): continue
                            if k1.loc['measValue',i]['measResults'].count(',')>2: sp=','
                            else: sp=' '
                            h=pd.DataFrame(np.array(k1.loc['measValue',i]['measResults'].split(sp)).reshape(1,-1),
                                                                columns=k1.loc['measTypes',i].split(sp))  
                            if pd.Series(h.columns[0]).str.count(' ').values>3:
                                h=pd.DataFrame(np.array(k1.loc['measValue',i]['measResults'].split(' ')).reshape(1,-1),
                                                                columns=k1.loc['measTypes',i].split(' '))
                            h.insert(0,'Date',k1.loc['granPeriod',i]['@beginTime'].replace('T',' ').replace('+04:00',''))
                            h.insert(1,'Period',k1.loc['granPeriod',i]['@duration'].replace('PT',''))
                            h.insert(2,'LocalDn',xmlDict['measCollecFile']['measData'][j]['managedElement']['@localDn'])
                            h.insert(3,'UserLabel',xmlDict['measCollecFile']['measData'][j]['managedElement']['@userLabel'])
                            h['Date']=pd.to_datetime(h['Date'])
                            if k1.loc['@measInfoId'][i] in f.keys():
                                f[k1.loc['@measInfoId'][i]]=pd.concat([f[k1.loc['@measInfoId'][i]],h],ignore_index=True)
                            else:
                                f[k1.loc['@measInfoId'][i]]=h
                        except Exception as u:
                            print(u)
                else:
                    try:
                        if k1.loc['@measInfoId'][0] not in dict.keys(): continue
                        #if (k1.loc['@measInfoId']=='NeStat'): continue
                        #if k1.loc['measValue']['measResults'].count(',')>2: sp=','
                        #else: sp=' '
                        sp=','
                        h=pd.DataFrame(np.array(k1.loc['measValue','measResults'].split(sp)).reshape(1,-1),
                                                        columns=k1.loc['measTypes','measResults'].split(sp))
                        h.insert(0,'Date',k1.loc['granPeriod','@beginTime'].replace('T',' ').replace('+04:00',''))
                        h.insert(1,'Period',k1.loc['granPeriod','@duration'].replace('PT',''))
                        h.insert(2,'LocalDn',xmlDict['measCollecFile']['measData'][j]['managedElement']['@localDn'])
                        h.insert(3,'UserLabel',xmlDict['measCollecFile']['measData'][j]['managedElement']['@userLabel'])
                        h['Date']=pd.to_datetime(h['Date'])
                        if k1.loc['@measInfoId','measResults'] in f.keys():
                            f[k1.loc['@measInfoId','measResults']]=pd.concat([f[k1.loc['@measInfoId','measResults']],h],ignore_index=True)
                        else:
                            f[k1.loc['@measInfoId','measResults']]=h
                    except Exception as u:
                        print(u)
                
            except Exception as e:
                print(e,j,' error')
                #print(j, i)
                continue
            
            d.append(diddd)
    else:
        k1=pd.DataFrame(xmlDict['measCollecFile']['measData']['measInfo']).T
        for i in k1.columns:
            if k1.loc['@measInfoId'][i] not in dict.keys(): continue
            if (k1.loc['@measInfoId'][i]=='NeStat'): continue
            if k1.loc['measValue',i]['measResults'].count(',')>2: sp=','
            else: sp=' '
            h=pd.DataFrame(np.array(k1.loc['measValue',i]['measResults'].split(sp)).reshape(1,-1),columns=k1.loc['measTypes',i].split(sp))
            h.insert(0,'Date',k1.loc['granPeriod',i]['@beginTime'].replace('T',' ').replace('+04:00',''))
            h.insert(1,'Period',k1.loc['granPeriod',i]['@duration'].replace('PT',''))
            h.insert(2,'LocalDn',xmlDict['measCollecFile']['measData']['managedElement']['@localDn'])
            h.insert(3,'UserLabel',xmlDict['measCollecFile']['measData']['managedElement']['@userLabel'])
            h['Date']=pd.to_datetime(h['Date'])
            if k1.loc['@measInfoId'][i] in f.keys():
                f[k1.loc['@measInfoId'][i]]=pd.concat([f[k1.loc['@measInfoId'][i]],h],ignore_index=True)
            else:
                f[k1.loc['@measInfoId'][i]]=h
    d.append(diddd)
with cf.ThreadPoolExecutor() as executor:
        executor.map(proccess_excel,glob.glob('*.xml'))
    
print('loop finished. time to save')


for j in f.keys():
    a=f[j]
    dict2=dict[j].copy()
    dict2.pop('Date')
    for m in a.columns:
        if m not in dict2.keys():
            dict2[m]=20
    #if ('DCC' not in j) & ('Offline' not in j) & ('Forward' not in j): 
    #    if os.path.exists('/disk2/support_files/archive/it_trend_files/'+j+'_bkc.csv'):
    #        a.to_csv('/disk2/support_files/archive/it_trend_files/'+j+'_bkc.csv',index=False,header=False,mode='a')
    #    else:
    #        a.to_csv('/disk2/support_files/archive/it_trend_files/'+j+'_bkc.csv',index=False,mode='a')
    a['Date']=pd.to_datetime(a['Date'])
    to_change=list(set(a.columns) - set(dict[j].keys()))
    for i in to_change:
        try:
            a[i]=a[i].astype(float)
        except:
            continue
    for i in a['Date'].dt.date.unique():
        try:
        #file_name = datetime.datetime.strftime(datetime.datetime.utcfromtimestamp(i.astype(datetime.datetime)/1e9), "%B_%Y")
            file_name2 = datetime.datetime.strftime(i, "%Y-%m-%d")
            a.loc[a['Date'].dt.date==i].to_hdf(r'/disk2/support_files/archive/it/billing_' + file_name2 + '.h5', j , append=True,
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
print(len(set(d)),' length of files')
print('processing part finished, anomaly begins')
####################### Anomaly check ########################
final_all = []
not_evaluated = []
for k in ['hwBillingDCCMsgNumber','hwBillingDCCSessionNumber','hwBillingDCCAccessSessionNum','hwBillingDCCAccessMsgNumberV4','OSRuntime', \
'persistRunningTime','CPU','LogicalCPU','Memory','VirtualMemory','hwBillingDCCSeviceCallNumber','hwForwardDiamMsgStat','hwOfflineStat',\
    'serviceDatabaseInf','Storage']: 
    try:
        aa=f[k]
        #dates=a.sort_values(by='Date')['Date'][-3:].values
        dates=list(aa.sort_values(by='Date')['Date'].unique())
        #print('dates are equal to: ',dates)
        for a in dates:
            #print(a,' date format')
            needed = dt.strptime(dt.strftime(dt.utcfromtimestamp(a.astype(datetime.datetime)/1e9),'%Y-%m-%d %H:%M'), '%Y-%m-%d %H:%M')
            if (k=='hwBillingDCCMsgNumber') | (k=='hwBillingDCCSessionNumber') | (k=='hwBillingDCCSeviceCallNumber') | (k=='hwForwardDiamMsgStat') | (k=='hwOfflineStat') | (k=='hwBillingDCCAccessMsgNumberV4') | (k=='hwBillingDCCAccessSessionNum'):
                hh = pd.date_range(end=needed, periods=15, freq='24H')
                files = pd.date_range(end=needed, periods=15, freq='24H').strftime("%Y-%m-%d").tolist()
            elif k=='OSRuntime':
                hh = pd.date_range(end=needed, periods=3, freq='15min')
                files=pd.date_range(end=needed, periods=1, freq='24H').strftime("%Y-%m-%d").tolist()
            elif k=='persistRunningTime':
                hh = pd.date_range(end=needed, periods=3, freq='5min')
                files=pd.date_range(end=needed, periods=1, freq='24H').strftime("%Y-%m-%d").tolist()
            #elif k in ['CPU','LogicalCPU','Memory','VirtualMemory']:
            #    hh = pd.date_range(end=needed, periods=3, freq='15min')
            #    files=pd.date_range(end=needed, periods=1, freq='3H').strftime("%Y-%m-%d").tolist()
            d=[]
            os.chdir('/disk2/support_files/archive/it')
            if k not in ['CPU','LogicalCPU','Memory','VirtualMemory','serviceDatabaseInf','Storage']:
                if k in ['OSRuntime','persistRunningTime']:
                    for i in set(files):
                        try:
                            d.append(pd.read_hdf('billing_'+i+'.h5',k,where='Date in hh'))
                        except Exception as e:
                            print(e)
                            continue
                else:
                    for i in set(files[:-1]):
                        try:
                            d.append(pd.read_hdf('billing_'+i+'.h5',k,where='Date in hh'))
                        except Exception as e:
                            print(e)
                            continue
            else:
                d.append(aa[aa['Date']==a])
            if (k=='hwBillingDCCMsgNumber') | (k=='hwBillingDCCAccessMsgNumberV4'):
                dcc2=aa[aa['Date']==a]
                dcc2.loc[:,'hour']=dcc2['Date'].dt.hour
                dcc2.loc[:,'minute']=dcc2['Date'].dt.minute
                if k=='hwBillingDCCMsgNumber':
                    dcc2.iloc[:,6:]=dcc2.iloc[:,6:].astype(float)
                else:
                    dcc2.iloc[:,9:]=dcc2.iloc[:,9:].astype(float)
                dcc=pd.concat(d)
                dcc.drop_duplicates(inplace=True)
                if k=='hwBillingDCCMsgNumber':
                    dcc.iloc[:,6:]=dcc.iloc[:,6:].astype(float)
                else:
                    dcc.iloc[:,9:]=dcc.iloc[:,9:].astype(float)
                dcc.loc[:,'hour']=dcc['Date'].dt.hour
                dcc.loc[:,'minute']=dcc['Date'].dt.minute
                mean=dcc.groupby(['hour','minute','LocalDn','UserLabel','hwNetDevice','hwEventType']).mean()
                std=dcc.groupby(['hour','minute','LocalDn','UserLabel','hwNetDevice','hwEventType']).std()
                dl_filt=mean-3*std
                ul_filt=mean+3*std
                l=dcc.columns.get_loc('hwEventType') + 1
                #dcc2=dcc.loc[dcc['Date']==dcc.sort_values(by='Date')['Date'].unique()[-1]]
                up=dcc2.melt(id_vars=dcc.columns[2:l], value_vars=dcc.columns[l:-2]).merge(
                    ul_filt.reset_index().melt(id_vars=dcc.columns[2:l], value_name='up_threshold'), on=list(dcc.columns[2:l]).append('variable'))
                up_down=up.merge(dl_filt.reset_index().melt(id_vars=dcc.columns[2:l], value_name='down_threshold'), on=list(dcc.columns[2:l]).append('variable'))
                up_down['status']=0
                up_down.loc[up_down['value']>up_down['up_threshold'],'status']=1
                up_down.loc[up_down['value']<up_down['down_threshold'],'status']=2
                up_down['Date']=a
                final=up_down[up_down['status']>0]
                final['period']=dcc['Period'].values[0]
                final['file']=k
                final=final[~((final['status']==1) & (final['variable']=='hwDCCSuccNumber'))]
                #final['LocalDn']=np.NaN
                final=final[['UserLabel','hwNetDevice','hwEventType','LocalDn','variable','value','up_threshold','down_threshold','status','Date','period','file']]
            elif (k=='hwBillingDCCSessionNumber') | (k=='hwBillingDCCAccessSessionNum'):
                dcc_ses2=aa[aa['Date']==a]
                dcc_ses2.drop(columns='hwBEID',inplace=True)
                #dcc_ses2['day']=dcc_ses2['Date'].dt.date
                dcc_ses2.iloc[:,6:]=dcc_ses2.iloc[:,6:].astype(float)
                dcc_ses2.loc[:,'hour']=dcc_ses2['Date'].dt.hour
                dcc_ses2.loc[:,'minute']=dcc_ses2['Date'].dt.minute
                dcc_ses2=dcc_ses2.groupby(['hour','minute','LocalDn','UserLabel','hwNetDevice','hwEventType']).sum().reset_index()

                dcc_ses=pd.concat(d)
                dcc_ses.drop(columns='hwBEID',inplace=True)
                #dcc_ses['day']=dcc_ses['Date'].dt.date
                dcc_ses.iloc[:,6:]=dcc_ses.iloc[:,6:].astype(float)
                dcc_ses.loc[:,'hour']=dcc_ses['Date'].dt.hour
                dcc_ses.loc[:,'minute']=dcc_ses['Date'].dt.minute
                mean=dcc_ses.groupby(['hour','minute','LocalDn','UserLabel','hwNetDevice','hwEventType']).mean()
                std=dcc_ses.groupby(['hour','minute','LocalDn','UserLabel','hwNetDevice','hwEventType']).std()
                dl_filt=mean-3*std
                ul_filt=mean+3*std
                l=dcc_ses.columns.get_loc('hwEventType') + 1
                #dcc_ses2=dcc_ses.loc[dcc_ses['Date']==dcc_ses.sort_values(by='Date')['Date'].unique()[-1]]
                up=dcc_ses2.melt(id_vars=dcc_ses.columns[2:l], value_vars=dcc_ses.columns[l:-2]).merge(
                    ul_filt.reset_index().melt(id_vars=dcc_ses.columns[2:l], value_name='up_threshold'), on=list(dcc_ses.columns[2:l]).append('variable'))
                up_down=up.merge(dl_filt.reset_index().melt(id_vars=dcc_ses.columns[2:l], value_name='down_threshold'), on=list(dcc_ses.columns[2:l]).append('variable'))
                up_down['status']=0
                up_down.loc[up_down['value']>up_down['up_threshold'],'status']=1
                up_down.loc[up_down['value']<up_down['down_threshold'],'status']=2
                up_down['Date']=a
                final=up_down[up_down['status']>0]
                final['period']=dcc_ses['Period'].values[0]
                final['file']=k
                final=final[['UserLabel','hwNetDevice','hwEventType','LocalDn','variable','value','up_threshold','down_threshold','status','Date','period','file']]
            elif k=='hwBillingDCCSeviceCallNumber':
                dcc_sc2=aa[aa['Date']==a]
                #dcc_sc2['day']=dcc_sc2['Date'].dt.date
                dcc_sc2.iloc[:,5:]=dcc_sc2.iloc[:,5:].astype(float)
                dcc_sc2.loc[:,'hour']=dcc_sc2['Date'].dt.hour
                dcc_sc2.loc[:,'minute']=dcc_sc2['Date'].dt.minute
                dcc_sc2.rename(columns={'hwServiceName':'hwEventType'},inplace=True)
                dcc_sc2.drop(columns=['hwServiceCallMax','hwServiceCallMinDelay','hwServiceCallMaxDelay','hwServiceCallTotalTime'],inplace=True)

                dcc_sc=pd.concat(d)
                #dcc_sc['day']=dcc_sc['Date'].dt.date
                dcc_sc.iloc[:,5:]=dcc_sc.iloc[:,5:].astype(float)
                dcc_sc.loc[:,'hour']=dcc_sc['Date'].dt.hour
                dcc_sc.loc[:,'minute']=dcc_sc['Date'].dt.minute
                dcc_sc.rename(columns={'hwServiceName':'hwEventType'},inplace=True)
                dcc_sc.drop(columns=['hwServiceCallMax','hwServiceCallMinDelay','hwServiceCallMaxDelay','hwServiceCallTotalTime'],inplace=True)
                mean=dcc_sc.groupby(['hour','minute','LocalDn','UserLabel','hwEventType']).mean()
                std=dcc_sc.groupby(['hour','minute','LocalDn','UserLabel','hwEventType']).std()
                dl_filt=mean-3*std
                ul_filt=mean+3*std
                l=dcc_sc.columns.get_loc('hwEventType') + 1
                #dcc_sc2=dcc_sc.loc[dcc_sc['Date']==dcc_sc.sort_values(by='Date')['Date'].unique()[-1]]
                up=dcc_sc2.melt(id_vars=dcc_sc.columns[2:l], value_vars=dcc_sc.columns[l:-2]).merge(
                    ul_filt.reset_index().melt(id_vars=dcc_sc.columns[2:l], value_name='up_threshold'), on=list(dcc_sc.columns[2:l]).append('variable'))
                up_down=up.merge(dl_filt.reset_index().melt(id_vars=dcc_sc.columns[2:l], value_name='down_threshold'), on=list(dcc_sc.columns[2:l]).append('variable'))
                up_down['status']=0
                up_down.loc[up_down['value']>up_down['up_threshold'],'status']=1
                up_down.loc[up_down['value']<up_down['down_threshold'],'status']=2
                up_down['Date']=a
                final=up_down[up_down['status']>0]
                final['hwNetDevice']=None
                final['period']=dcc_sc['Period'].values[0]
                final['file']=k
                final=final[['UserLabel','hwNetDevice','hwEventType','LocalDn','variable','value','up_threshold','down_threshold','status','Date','period','file']]
            elif k=='hwForwardDiamMsgStat':
                dcc_df2=aa[aa['Date']==a]
                #dcc_df2['day']=dcc_df2['Date'].dt.date
                dcc_df2.iloc[:,5:]=dcc_df2.iloc[:,5:].astype(float)
                dcc_df2.loc[:,'hour']=dcc_df2['Date'].dt.hour
                dcc_df2.loc[:,'minute']=dcc_df2['Date'].dt.minute

                dcc_df=pd.concat(d)
                #dcc_df['day']=dcc_df['Date'].dt.date
                dcc_df.iloc[:,5:]=dcc_df.iloc[:,5:].astype(float)
                dcc_df.loc[:,'hour']=dcc_df['Date'].dt.hour
                dcc_df.loc[:,'minute']=dcc_df['Date'].dt.minute
                mean=dcc_df.groupby(['hour','minute','LocalDn','UserLabel','hwEventType']).mean()
                std=dcc_df.groupby(['hour','minute','LocalDn','UserLabel','hwEventType']).std()
                dl_filt=mean-3*std
                ul_filt=mean+3*std
                l=dcc_df.columns.get_loc('hwEventType') + 1
                #dcc_df2=dcc_df.loc[dcc_df['Date']==dcc_df.sort_values(by='Date')['Date'].unique()[-1]]
                up=dcc_df2.melt(id_vars=dcc_df.columns[2:l], value_vars=dcc_df.columns[l:-2]).merge(
                    ul_filt.reset_index().melt(id_vars=dcc_df.columns[2:l], value_name='up_threshold'), on=list(dcc_df.columns[2:l]).append('variable'))
                up_down=up.merge(dl_filt.reset_index().melt(id_vars=dcc_df.columns[2:l], value_name='down_threshold'), on=list(dcc_df.columns[2:l]).append('variable'))
                up_down['status']=0
                up_down.loc[up_down['value']>up_down['up_threshold'],'status']=1
                up_down.loc[up_down['value']<up_down['down_threshold'],'status']=2
                up_down['Date']=a
                final=up_down[up_down['status']>0]
                final['hwNetDevice']=None
                final['period']=dcc_df['Period'].values[0]
                final['file']=k
                final=final[['UserLabel','hwNetDevice','hwEventType','LocalDn','variable','value','up_threshold','down_threshold','status','Date','period','file']]
            elif k=='hwOfflineStat':
                dcc_off2=aa[aa['Date']==a]
                #dcc_off2['day']=dcc_off2['Date'].dt.date
                dcc_off2.iloc[:,6:]=dcc_off2.iloc[:,6:].astype(float)
                dcc_off2.loc[:,'hour']=dcc_off2['Date'].dt.hour
                dcc_off2.loc[:,'minute']=dcc_off2['Date'].dt.minute

                dcc_off=pd.concat(d)
                #dcc_off['day']=dcc_off['Date'].dt.date
                dcc_off.iloc[:,6:]=dcc_off.iloc[:,6:].astype(float)
                dcc_off.loc[:,'hour']=dcc_off['Date'].dt.hour
                dcc_off.loc[:,'minute']=dcc_off['Date'].dt.minute
                mean=dcc_off.groupby(['hour','minute','LocalDn','UserLabel','hwEventType','hwNetDevice']).mean()
                std=dcc_off.groupby(['hour','minute','LocalDn','UserLabel','hwEventType','hwNetDevice']).std()
                dl_filt=mean-3*std
                ul_filt=mean+3*std
                l=dcc_off.columns.get_loc('hwEventType') + 1
                #dcc_off2=dcc_off.loc[dcc_off['Date']==dcc_off.sort_values(by='Date')['Date'].unique()[-1]]
                up=dcc_off2.melt(id_vars=dcc_off.columns[2:l], value_vars=dcc_off.columns[l:-2]).merge(
                    ul_filt.reset_index().melt(id_vars=dcc_off.columns[2:l], value_name='up_threshold'), on=list(dcc_off.columns[2:l]).append('variable'))
                up_down=up.merge(dl_filt.reset_index().melt(id_vars=dcc_off.columns[2:l], value_name='down_threshold'), on=list(dcc_off.columns[2:l]).append('variable'))
                up_down['status']=0
                up_down.loc[up_down['value']>up_down['up_threshold'],'status']=1
                up_down.loc[up_down['value']<up_down['down_threshold'],'status']=2
                up_down['Date']=a
                final=up_down[up_down['status']>0]
                final['period']=dcc_off['Period'].values[0]
                final['file']=k
                final=final[['UserLabel','hwNetDevice','hwEventType','LocalDn','variable','value','up_threshold','down_threshold','status','Date','period','file']]        
            elif k=='OSRuntime':
                br = pd.concat(d)
                br=br[br['Period']=='15M']
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
                br.loc[br['delta']<0,'delta']=90000-br['hwHostSysUptime']
                br['delta2']=br['delta']
                br.loc[br['hwHostSysUptime']!=2147483647,'delta2']=(br.loc[br['hwHostSysUptime']!=2147483647,'delta']-100*60*15*br.loc[br['hwHostSysUptime']!=2147483647,'interval'])/100
                br.loc[abs(br['delta2'])<10,'delta2']=0
                br.loc[br['check']==False,'delta2']=0
                br['availability']=(15*60*br['interval']+br['delta2'])/(15*60*br['interval'])*100
                br['avail_num']=15*60*br['interval']+br['delta2']
                br['avail_den']=15*60*br['interval']
                br['Availability']=br['avail_num']/br['avail_den']*100
                br['status']=0
                br['Date']=a
                br_f=br.groupby('Date').sum().reset_index()
                br_f=br_f[['Date','avail_num','avail_den']]
                br_f['availability']=br_f['avail_num']/br_f['avail_den']*100
                br_f.to_csv('/disk2/support_files/archive/OSRuntime_bkc.csv',index=False,mode='a',header=False)
                br['up_threshold']=0
                br['down_threshold']=100
                br['variable']='Board Availability'
                br.rename(columns={'Availability':'value'},inplace=True)
                br.loc[br['value']<br['down_threshold'],'status']=2
                final=br[br['status']>0]
                final['hwNetDevice']=np.NaN
                final['hwEventType']=np.NaN
                final['period']=br['Period'].values[0]
                final['file']=k
                final=final[['UserLabel','hwNetDevice','hwEventType','LocalDn','variable','value','up_threshold','down_threshold','status','Date','period','file']]
            elif k=='persistRunningTime':
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
                dbr.loc[dbr['delta']<0,'delta']=300-dbr['hwDbSysUptime']
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
                dbr['Availability']=dbr['avail_num_corrected']/dbr['avail_den']*100
                dbr.loc[dbr['Availability']>100,'Availability']=100
                dbr['status']=0
                dbr['Date']=a
                dbr_f=dbr.groupby('Date').sum().reset_index()
                dbr_f=dbr_f[['Date','avail_num_corrected','avail_den']]
                dbr_f['availability']=dbr_f['avail_num_corrected']/dbr_f['avail_den']*100
                dbr_f.to_csv('/disk2/support_files/archive/persistRunningTime_bkc.csv',index=False,mode='a',header=False)
                dbr['up_threshold']=0
                dbr['down_threshold']=100
                dbr['variable']='Database Availability'
                dbr.rename(columns={'Availability':'value'},inplace=True)
                dbr.loc[dbr['value']<dbr['down_threshold'],'status']=2
                final=dbr[dbr['status']>0]
                final['hwNetDevice']=np.NaN
                final['hwEventType']=np.NaN
                final['period']=dbr['Period'].values[0]
                final['file']=k
                final=final[['UserLabel','hwNetDevice','hwEventType','LocalDn','variable','value','up_threshold','down_threshold','status','Date','period','file']]
            elif k=='CPU':
                cpu=pd.concat(d)
                cpu['CpuUsage']=cpu['CpuUsage'].astype(float)
                cpu['status']=0
                #cpu['Date']=a
                cpu['up_threshold']=95
                cpu['down_threshold']=0
                cpu['variable']='CPU Usage'
                cpu.rename(columns={'CpuUsage':'value'},inplace=True)
                cpu.loc[cpu['value']>cpu['up_threshold'],'status']=1
                final=cpu[cpu['status']>0]
                final['hwNetDevice']=np.NaN
                final['hwEventType']=np.NaN
                final['period']=cpu['Period'][0]
                final['file']=k
                final=final[['UserLabel','hwNetDevice','hwEventType','LocalDn','variable','value','up_threshold','down_threshold','status','Date','period','file']]
            elif k=='LogicalCPU':
                lcpu=pd.concat(d)
                lcpu['LogicalCPUUsage']=lcpu['LogicalCPUUsage'].astype(float)
                lcpu['status']=0
                #lcpu['Date']=a
                lcpu['up_threshold']=95
                lcpu['down_threshold']=0
                lcpu['variable']='Logical CPU Usage'
                lcpu.rename(columns={'LogicalCPUUsage':'value'},inplace=True)
                lcpu.loc[lcpu['value']>lcpu['up_threshold'],'status']=1
                final=lcpu[lcpu['status']>0]
                final.rename(columns={'LogicalCPUName':'hwNetDevice'},inplace=True)
                final['hwEventType']=np.NaN
                final['period']=lcpu['Period'][0]
                final['file']=k
                final=final[['UserLabel','hwNetDevice','hwEventType','LocalDn','variable','value','up_threshold','down_threshold','status','Date','period','file']]
            elif k=='Memory':
                load=pd.concat(d)
                load['MemUsage']=load['MemUsage'].astype(float)
                load['status']=0
                #load['Date']=a
                load['up_threshold']=90
                load['down_threshold']=0
                load['variable']='Memory Usage'
                load.rename(columns={'MemUsage':'value'},inplace=True)
                load.loc[load['value']>load['up_threshold'],'status']=1
                final=load[load['status']>0]
                final['hwNetDevice']=np.NaN
                final['hwEventType']=np.NaN
                final['period']=load['Period'][0]
                final['file']=k
                final=final[['UserLabel','hwNetDevice','hwEventType','LocalDn','variable','value','up_threshold','down_threshold','status','Date','period','file']]
            elif k=='VirtualMemory':
                vm=pd.concat(d)
                vm[['TotalSize','FreeSize']]=vm[['TotalSize','FreeSize']].astype(float)
                vm.iloc[:,4:]=vm.iloc[:,4:].astype(float)
                vm.insert(5,'usage',(vm['TotalSize']-vm['FreeSize'])/vm['TotalSize']*100)
                vm['status']=0
                #vm['Date']=a
                vm['up_threshold']=90
                vm['down_threshold']=0
                vm['variable']='Virtual Memory Usage'
                vm.rename(columns={'usage':'value'},inplace=True)
                vm.loc[vm['value']>vm['up_threshold'],'status']=1
                final=vm[vm['status']>0]
                final['hwNetDevice']=np.NaN
                final['hwEventType']=np.NaN
                final['period']=vm['Period'][0]
                final['file']=k
                final=final[['UserLabel','hwNetDevice','hwEventType','LocalDn','variable','value','up_threshold','down_threshold','status','Date','period','file']]
            elif k=='serviceDatabaseInf':
                sdb=pd.concat(d)
                sdb.iloc[:,-5:]=sdb.iloc[:,-5:].astype(float)
                sdb['value']=sdb['dbUsedSize']/(sdb['dbUsedSize']+sdb['dbFreeSize'])*100
                sdb['variable']='Database Size usage'
                sdb.rename(columns={'databaseName':'hwNetDevice'},inplace=True)
                sdb['status']=0
                #sdb['Date']=a
                sdb['up_threshold']=90
                sdb['down_threshold']=0
                sdb.loc[sdb['value']>sdb['up_threshold'],'status']=1
                final=sdb[sdb['status']>0]
                final['hwEventType']=np.NaN
                final['period']=sdb['Period'][0]
                final['file']=k
                final=final[['UserLabel','hwNetDevice','hwEventType','LocalDn','variable','value','up_threshold','down_threshold','status','Date','period','file']]
            elif k=='Storage':
                s=pd.concat(d)
                s.iloc[:,8:]=s.iloc[:,8:].astype(int)
                s['value']=(s['TotalSize']-s['FreeSize'])/s['TotalSize']*100
                s['variable']='Board storage usage'
                s.rename(columns={'StorageName':'hwNetDevice'},inplace=True)
                s['status']=0
                #sdb['Date']=a
                s['up_threshold']=90
                s['down_threshold']=0
                s.loc[s['value']>s['up_threshold'],'status']=1
                final=s[s['status']>0]
                final.loc[:,'hwEventType']=np.NaN
                final.loc[:,'period']=s['Period'][0]
                final.loc[:,'file']=k
                final=final[['UserLabel','hwNetDevice','hwEventType','LocalDn','variable','value','up_threshold','down_threshold','status','Date','period','file']]
    except Exception as e:
        not_evaluated.append(k)
        print(e,' from bkc billing proccessing of file', k)
        continue

    final_all.append(final)

final=pd.concat(final_all)    
if len(final)>0:
    ##final.to_csv('anomaly.csv',mode='a',index=False)
    #if os.path.exists('anomaly.csv'):
    #    final.to_csv('anomaly.csv',index=False,mode='a',header=False)
    #else: final.to_csv('anomaly.csv',index=False,mode='a')


########################
    final.to_csv('/home/ismayil/flask_dash/support_files/anomality_detection/it_anomalies.csv', index=False)
    final[['UserLabel','hwNetDevice','hwEventType','LocalDn','variable']]=final[['UserLabel','hwNetDevice','hwEventType','LocalDn','variable']].astype(str)
    if os.path.exists('/home/ismayil/flask_dash/support_files/anomality_detection/it_ongoing_anomalies.csv'):
        final['hwEventType']=final['hwEventType'].astype(str)
        anomalies = pd.read_csv('/home/ismayil/flask_dash/support_files/anomality_detection/it_ongoing_anomalies.csv')
        temp=anomalies.loc[anomalies['file'].isin(not_evaluated)] 
        temp.rename(columns={'status':'status_x'},inplace=True)
        anomalies[['UserLabel','hwNetDevice','hwEventType','LocalDn','variable']]=anomalies[['UserLabel','hwNetDevice','hwEventType','LocalDn','variable']].astype(str)
        anomalies['hwEventType']=anomalies['hwEventType'].astype(str)
        anomalies = final.merge(anomalies[['UserLabel','hwNetDevice','hwEventType','LocalDn','variable','status','c_days','period','file']],
                            on=['UserLabel','hwNetDevice','hwEventType','LocalDn','variable','period','file'], how='left')
        anomalies = pd.concat([anomalies,temp],ignore_index=True)
    else:
        anomalies=final
        anomalies['c_days']=0
        anomalies.rename(columns={'status':'status_x'},inplace=True)
        anomalies['status_y']=0
        #anomalies['value_y']=0
        anomalies['hwEventType']=anomalies['hwEventType'].astype(str)
    anomalies.loc[anomalies['c_days'].notnull(), 'c_days'] += 1
    anomalies.loc[anomalies['c_days'].isnull(), 'c_days'] = 1
    anomalies.rename(columns={'status_x': 'status','value_x':'value'}, inplace=True)
    anomalies.drop(columns=['status_y'], inplace=True) #,'value_y'
    anomalies=anomalies.drop_duplicates()
    anomalies['Date']=pd.to_datetime(anomalies['Date'])
    anomalies['c_days']=anomalies['c_days'].astype(float)
    anomalies.to_csv('/home/ismayil/flask_dash/support_files/anomality_detection/it_ongoing_anomalies.csv', index=False)
    anomalies[['Date','file','value']].groupby(['Date','file'],as_index=False).count().to_csv('/disk2/support_files/archive/bkc_it_anom_trend.csv',mode='a',
                    index=False,header=None)
    temp_dict={'UserLabel': 40, 'hwNetDevice': 40, 'LocalDn':40,'hwEventType':40}
    try:
        for n in temp_dict.keys():
            anomalies[n]=anomalies[n].apply(lambda x: x[:temp_dict[n]] if len(x)>temp_dict[n] else x)
    except Exception as dsd:
        print(dsd)
        1
    #anomalies=anomalies.convert_dtypes()
    anomalies['value']=anomalies['value'].astype(float)
    anomalies['hwEventType']=anomalies['hwEventType'].apply(lambda x: str(x).replace(' ',''))
    #print(anomalies.info())
    for d in anomalies['Date'].dt.date.unique():
        file_name2 = datetime.datetime.strftime(d, "%Y-%m-%d")
        #f2=datetime.datetime.strftime(datetime.datetime.now(), "%H_%M")
        anomalies.loc[anomalies['Date'].dt.date==d].to_hdf(
            '/disk2/support_files/archive/anomality/' + file_name2 + '_anomalies.h5', '/it',
            append=True, format='table', data_columns=anomalies.columns, complevel=5,
            min_itemsize={'UserLabel': 40, 'hwEventType':40,'hwNetDevice': 40, 'LocalDn':40,
            'variable': 30, 'value': 30, 'up_threshold': 30, 'down_threshold': 30, 'period':10, 'file':30})
    print('saved successfully')
#else: 
#    if os.path.exists('/home/ismayil/flask_dash/support_files/anomality_detection/it_ongoing_anomalies.csv'):
#        os.remove('/home/ismayil/flask_dash/support_files/anomality_detection/it_ongoing_anomalies.csv')
