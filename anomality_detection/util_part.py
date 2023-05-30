import pandas as pd
import os
import datetime
import numpy as np
import time
from datetime import datetime as dt

#time.sleep(40)
c1 = time.time()

needed = dt.strptime(dt.strftime(dt.now() - datetime.timedelta(hours=1), '%d.%m.%y %H:00'), '%d.%m.%y %H:00')
hh = pd.date_range(end=needed, periods=15, freq='24H')

files = pd.date_range(end=needed, periods=15, freq='24H').strftime("%Y-%m-%d").tolist()

util_cols=['Ovl_TxCrPwr_time_share_DL','TCP_Utilization','BB_subunit_util', 'Max_HSUPA_user_util_ratio',
'Max_HSDPA_user_util_ratio','Max_HSUPA_thr_util_ratio', 'Max_HSDPA_thr_util_ratio','DL_PRB_Utilzation','UL_PRB_Utilzation']
all_cols = {'cell_3g': [['RNC_name', 'WBTS_name', 'WCEL_name'],
            ['IUB_LOSS_CC_FRAME_LOSS_IND','Ovl_TxCrPwr_time_share_DL','TCP_Utilization']],
            'lcg_3g': [['RNC_name', 'WBTS_name', 'lcg_id'],
            ['DCH_FP_REC_FRMS_W_CRC_ERR','DCH_FP_REC_FRMS_W_DELAY', 'DCH_FP_REC_FRMS_W_OTH_ERR','HS_DSCH_FP_FRMS_W_CRC',
                        'HS_DSCH_FP_FRMS_W_OTH_ERR','BB_subunit_util', 'Max_HSUPA_user_util_ratio']],
            'site_3g': [['RNC_name', 'WBTS_name'],['HS_DSCH_CREDIT_RDCT_FRM_LOSS (M5000C178)','Max_HSDPA_user_util_ratio','Max_HSUPA_thr_util_ratio', 'Max_HSDPA_thr_util_ratio','Frame_error_huawei']],
            'site_4g': [['Site_name'],['IPPM_lost']],
            'cell_4g': [['Vendor', 'Site_name', 'Cell_name'],['DL_PRB_Utilzation','UL_PRB_Utilzation']]
            }

comb = []
# for_plot=[]
for f in all_cols.keys():
    try:
       cols = all_cols[f][1]
       h = []
       for i in files:
           if os.path.isfile(os.path.join('/disk2/support_files/archive/util/'+i+'_util_hourly.h5')):
               #t = datetime.datetime.strptime(i + " " + hour, "%d.%m.%Y %H:%M")
               h.append(pd.read_hdf('/disk2/support_files/archive/util/'+i+'_util_hourly.h5', f, where='Date in hh'))
           else: continue
       ready = pd.concat(h)
       ready.fillna(0,inplace=True)
       if f=='cell_3g':
            ready['NE']=ready['WCEL_name']
       elif f=='lcg_3g':
            ready['NE']=ready['WBTS_name']+'_'+ready['lcg_id'].astype('str')
       elif f=='site_3g':
            ready['NE']=ready['WBTS_name']
       elif f=='site_4g':
            ready['RNC_name']='none'
            ready['NE']=ready['Site_name']
       elif f=='cell_4g':
            ready['RNC_name']='none'
            ready['NE']=ready['Cell_name']
       Q1 = ready.groupby(['RNC_name','NE']).mean()[cols]
       Q3 = ready.groupby(['RNC_name','NE']).std()[cols]
       df_last=ready.loc[ready['Date']==ready['Date'].unique()[-1]]
       #filt = [i for i in Q1.columns if
       #     (('TSL' in i) | ('RSL' in i) | ('LCUR' in i) | ('IF_SNR' in i) | ('IF_MSE' in i))]
       #df_filt = df[['displayedName','monitoredObjectSiteId','monitoredObjectSiteName', 'Date'] + cols]
       merged = df_last.melt(id_vars=['Date','RNC_name','NE'], value_vars=cols).merge(
        Q1.reset_index().melt(id_vars=['RNC_name','NE'], value_name='Q1'), how='left', \
            on=['RNC_name','NE','variable'])
       merged = merged.merge(Q3.reset_index().melt(id_vars=['RNC_name','NE'], value_name='Q3'),
                          on=['RNC_name','NE','variable'], how='left')
       merged['status'] = 0
       merged.loc[((merged['variable'].isin(util_cols) & (merged['value']>90))), 'status'] = 2
       #merged.loc[((~merged['variable'].isin(util_cols)) & (merged['value']>500)), 'status'] = 2
       merged.loc[((merged['value'] > (merged['Q1'] + 3 * merged['Q3'])) & (merged['Q3']!=0) & \
        (merged['variable'].isin(util_cols)) & (merged['value']>90)), 'status'] = 1
       merged.loc[((merged['value'] > (merged['Q1'] + 3 *merged['Q3'] )) & (merged['Q3']!=0) & \
        (~merged['variable'].isin(util_cols)) & (merged['value']>500)), 'status'] = 1
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
    combed.to_csv('/home/ismayil/flask_dash/support_files/anomality_detection/util_anomalies.csv', index=False)
    # combed.to_hdf('/disk2/support_files/archive/anomalies.h5', '/tx_level',
    #                     format='table', data_columns=combed.columns, complevel=5)
    print('append finished')
    if os.path.exists('/home/ismayil/flask_dash/support_files/anomality_detection/util_ongoing_anomalies.csv'):
        anomalies = pd.read_csv('/home/ismayil/flask_dash/support_files/anomality_detection/util_ongoing_anomalies.csv')
        anomalies = combed.merge(anomalies[['RNC_name','NE', 'variable', 'status', 'c_days']],
                            on=['RNC_name','NE', 'variable'], how='left')
    else: 
        anomalies=combed.copy()
        anomalies['c_days']=0
        anomalies['status_y']=0
    anomalies.loc[anomalies['c_days'].notnull(), 'c_days'] += 1
    anomalies.loc[anomalies['c_days'].isnull(), 'c_days'] = 1
    anomalies.rename(columns={'status_x': 'status'}, inplace=True)
    anomalies.drop(columns='status_y', inplace=True)
    anomalies=anomalies.drop_duplicates()
    anomalies.to_csv('/home/ismayil/flask_dash/support_files/anomality_detection/util_ongoing_anomalies.csv', index=False)
    anomalies['RNC_name']=anomalies['RNC_name'].astype(str)
    for d in anomalies['Date'].dt.date.unique():
        file_name2 = datetime.datetime.strftime(d, "%Y-%m-%d")
        anomalies.loc[anomalies['Date'].dt.date==d].to_hdf(
            '/disk2/support_files/archive/anomality/' + file_name2 + '_anomalies.h5', '/util',
            append=True, format='table', data_columns=anomalies.columns, complevel=5,
            min_itemsize={'RNC_name':40,'NE':40, 'variable': 25, 'value': 15, 'Q1': 15, 'Q3': 15, 'delta': 15})
else:
    anomalies=combed
    anomalies['c_days']=None
    anomalies.to_csv('/home/ismayil/flask_dash/support_files/anomality_detection/util_ongoing_anomalies.csv', index=False)

