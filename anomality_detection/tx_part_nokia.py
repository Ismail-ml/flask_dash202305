import pandas as pd
import os
import datetime
import numpy as np
import time

#time.sleep(40)
c1 = time.time()

os.chdir('/home/ismayil/flask_dash/data/tx')
with open('last_file.txt') as f:
    needed = f.readline()
needed_hour = datetime.datetime.strftime(datetime.datetime.strptime(needed, '%Y%m%d%H%M'), '%Y-%m-%d %H:%M')
needed_day = datetime.datetime.strftime(datetime.datetime.strptime(needed, '%Y%m%d%H%M'), '%d.%m.%Y')

all_cols = {'rsl_hop': ['RSL_AverageLeveldBm'],

            'tsl_hop': ['TSL_AverageLeveldBm'],

            'rx_stats': ['RxUtilization','DiscardedFrameRatio'], #'RxThroughput'

            'tx_stats': ['TxUtilization','DiscardedFrameRatio'], #'TxThroughput'

            'modulation': ['4QAM','4QAMStrong','16QAM','64QAM','8QAM','32QAM','128QAM','256QAM','512QAM',
                            '1024QAM','2048QAM','BPSK','4096QAM','BPskOneFourth','BPskOneHalf']

            }

comb = []
# for_plot=[]
for f in ['rsl_hop', 'tsl_hop', 'rx_stats', 'tx_stats', 'modulation']:
    try:
       df = pd.read_hdf('/disk2/support_files/archive/tx/tx_' + needed_day + '.h5', f, where='Date==needed_hour')
       files = pd.date_range(end=df['Date'].iloc[-1] - datetime.timedelta(1), periods=15, freq='24H').strftime(
        "%d.%m.%Y").tolist()
       hour = df['Date'].iloc[-1].strftime("%H:%M")
       cols = all_cols[f]
       h = []
       for i in files[:-1]:
           try:
                if os.path.isfile(os.path.join('/disk2/support_files/archive/tx','tx_'+i+'.h5')):
                    t = datetime.datetime.strptime(i + " " + hour, "%d.%m.%Y %H:%M")
                    h.append(pd.read_hdf('/disk2/support_files/archive/tx/tx_' + i + '.h5', f, where='Date==t'))
                else: continue
           except Exception as d:
                print(d)
       ready = pd.concat(h)
       Q1 = ready.groupby(['displayedName','monitoredObjectSiteId','monitoredObjectSiteName']).mean()[cols]
       Q3 = ready.groupby(['displayedName','monitoredObjectSiteId','monitoredObjectSiteName']).std()[cols]
       filt = [i for i in Q1.columns if
            (('TSL' in i) | ('RSL' in i) | ('LCUR' in i) | ('IF_SNR' in i) | ('IF_MSE' in i))]
       df_filt = df[['displayedName','monitoredObjectSiteId','monitoredObjectSiteName', 'Date'] + cols]
       merged = df_filt.melt(id_vars=df_filt.columns[:4], value_vars=df_filt.columns[4:]).merge(
        Q1.reset_index().melt(id_vars=['displayedName','monitoredObjectSiteId','monitoredObjectSiteName'], value_name='Q1'), how='left', \
            on=['displayedName','monitoredObjectSiteId','monitoredObjectSiteName','variable'])
       merged = merged.merge(Q3.reset_index().melt(id_vars=['displayedName','monitoredObjectSiteId','monitoredObjectSiteName'], value_name='Q3'),
                          on=['displayedName','monitoredObjectSiteId','monitoredObjectSiteName','variable'], how='left')
       merged['status'] = 0
       merged.loc[((merged['value'] > (merged['Q1'] + 3 * merged['Q3'])) & (
        ~merged['variable'].isin(filt)) & (merged['Q3']!=0)), 'status'] = -1
       merged.loc[((merged['value'] > (merged['Q1'] + 3 *merged['Q3'] )) & (
        merged['variable'].isin(filt)) & (merged['Q3']!=0)), 'status'] = -1
       merged.loc[((merged['value'] < (merged['Q1'] - 3 * merged['Q3'] )) & (
        merged['variable'].isin(filt)) & (merged['Q3']!=0)), 'status'] = 1
       merged.loc[(merged['value'] < 60) & (merged['variable'].str.contains('UTILIZATION')), 'status'] = 0
       merged.loc[(merged['value'] < 60) & (merged['variable'].str.contains('Utilization')), 'status'] = 0
       merged.loc[
        ((abs(abs(merged['value']) - abs(merged['Q1'])) < 2) & (merged['variable'].str.contains('RSL'))) |
        ((merged['value']>-45) & (merged['value']>=merged['Q1']) & (merged['variable'].str.contains('RSL'))), 'status'] = 0
       merged.loc[
        ((abs(abs(merged['value']) - abs(merged['Q1'])) < 2) & (merged['variable'].str.contains('TSL'))) |
        ((merged['value']>=merged['Q1']) & (merged['variable'].str.contains('TSL'))), 'status'] = 0
       merged.loc[
        (abs(abs(merged['value']) - abs(merged['Q1'])) < 2) & (merged['variable'].str.contains('RPL') | merged['variable'].str.contains('TLB') |
        merged['variable'].str.contains('TPL')), 'status'] = 0
       merged.loc[
        (abs(abs(merged['value']) - abs(merged['Q1'])) < 2) & (merged['variable'].str.contains('IF_MSE')), 'status'] = 0
       merged.loc[
        (abs(abs(merged['value']) - abs(merged['Q1'])) < 2) & (merged['variable'].str.contains('IF_SNR')), 'status'] = 0
       merged.loc[:, 'delta'] = round(
        (merged['value'] - merged['Q1']) / merged['value'] * 100, 2)
       merged['delta'].replace(np.inf, -2, inplace=True)
       merged.insert(0, 'file', f)
    # ready_plot=ready[['DeviceName','ResourceName','CollectionTime']+cols].melt(id_vars=df_filt.columns[:3], value_vars=df_filt.columns[3:])
    # ready_plot.insert(0,'file',f)
    # for_plot.append(merged.append(ready_plot))
       comb.append(merged[merged['status'] != 0])
    except Exception as e:
       print(e,f)
       continue
print('loop finished in ', time.time() - c1)
combed = pd.concat(comb)
combed=combed.drop_duplicates()
# pd.concat(for_plot).to_hdf('/disk2/support_files/archive/anomalies.h5', '/tx_trend',
#                         format='table', complevel=5, data_columns=merged.columns)
if len(combed)>0:
    combed.to_csv('/home/ismayil/flask_dash/support_files/anomality_detection/tx_anomalies_nokia.csv', index=False)
    # combed.to_hdf('/disk2/support_files/archive/anomalies.h5', '/tx_level',
    #                     format='table', data_columns=combed.columns, complevel=5)
    print('append finished')
    if os.path.exists('/home/ismayil/flask_dash/support_files/anomality_detection/tx_ongoing_anomalies_nokia.csv'):
        anomalies = pd.read_csv('/home/ismayil/flask_dash/support_files/anomality_detection/tx_ongoing_anomalies_nokia.csv')
        anomalies = combed.merge(anomalies[['displayedName','monitoredObjectSiteId','monitoredObjectSiteName', 'variable', 'status', 'c_days']],
                            on=['displayedName','monitoredObjectSiteId','monitoredObjectSiteName', 'variable'], how='left')
    else: 
        anomalies=combed.copy()
        anomalies['c_days']=0
        anomalies['status_y']=0
    anomalies.loc[anomalies['c_days'].notnull(), 'c_days'] += 1
    anomalies.loc[anomalies['c_days'].isnull(), 'c_days'] = 1
    anomalies.rename(columns={'status_x': 'status'}, inplace=True)
    anomalies.drop(columns='status_y', inplace=True)
    anomalies=anomalies.drop_duplicates()
    anomalies.to_csv('/home/ismayil/flask_dash/support_files/anomality_detection/tx_ongoing_anomalies_nokia.csv', index=False)
    anomalies['Date']=pd.to_datetime(anomalies['Date'])
    anomalies.to_hdf(
        '/disk2/support_files/archive/anomality/' + anomalies['Date'][0].strftime(
            "%Y-%m-%d") + '_anomalies.h5', '/tx_nokia',
        append=True, format='table', data_columns=anomalies.columns, complevel=5,
        min_itemsize={'displayedName':40,'monitoredObjectSiteId':40,'monitoredObjectSiteName': 40, 'variable': 25, 'value': 15, 'Q1': 15, 'Q3': 15, 'delta': 15})
else:
    anomalies=combed
    anomalies['c_days']=None
    anomalies.to_csv('/home/ismayil/flask_dash/support_files/anomality_detection/tx_ongoing_anomalies_nokia.csv', index=False)
