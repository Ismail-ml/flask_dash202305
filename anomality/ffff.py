import pandas as pd
import os
import datetime
import numpy as np
import time

time.sleep(40)
c1 = time.time()

os.chdir('/home/ismayil/flask_dash/data/tx')
with open('last_file.txt') as f:
    needed = f.readline()
needed_hour = datetime.datetime.strftime(datetime.datetime.strptime(needed, '%Y%m%d%H%M'), '%Y-%m-%d %H:%M')
needed_day = datetime.datetime.strftime(datetime.datetime.strptime(needed, '%Y%m%d%H%M'), '%d.%m.%Y')

all_cols = {'IGRTN': ['PORT_TX_BW_UTILIZATION', 'PORT_RX_BW_UTILIZATION_AVG', 'PORT_RX_BW_UTILIZATION_MAX',
                      'PORT_TX_BW_UTILIZATION_MAX', 'IF_BBE', 'IF_ES', 'RSL_AVG', 'RSL_CUR', 'TSL_AVG', 'TSL_CUR',
                      'IF_SES', 'IF_UAS',
                      'QPSKWS', 'QAMWS16', 'QAMWS32', 'QAMWS64', 'QAMWS128', 'QAMWS256'],

            'IG27': ['Inbound Bandwidth Utilization', 'Outbound Bandwidth Utilization',
                     'Outbound Discarded Packet Rate',
                     'Inbound Discarded Packet Ratio', 'Outbound Discarded Packet Ratio', 'Inbound Error Packet Rate',
                     'Outbound Error Packet Rate', 'Inbound Error Packet Ratio', 'Outbound Error Packet Ratio',
                     'Inbound Discarded Packet', 'Inbound Error Packet', 'Outbound Discarded Packet',
                     'Outbound Error Packet',
                     'IPv4 Error Packet Header Rate', 'IPv4 Outbound Packet Rate', 'Inbound CRC Error Packets',
                     'Inbound Fragment Error Packets', 'Inbound Over Flow  Packets', 'Outbound Over Flow  Packets',
                     'Max Bandwidth Utilization'],

            'IG30014': ['TX_DROP_PKTS', 'RX_DROP_PKTS', 'RXBBAD', 'TXBBAD', 'RX_DROP_RATIO', 'TX_DROP_RATIO'],

            'IG30024': ['TLBCUR', 'TPLCUR', 'RPLCUR'],

            'IG30029': ['IF_SNR_MAX', 'IF_SNR_AVG', 'QAMWS512', 'QAMWS1024', 'IF_BER', 'IF_MSE_CUR', 'QAMWS2048'],

            'IG41022': ['RXBBAD', 'TXBBAD', 'RX_DROP_RATIO', 'TX_DROP_RATIO', 'RXPAUSE', 'TXPAUSE'],

            'IGMSTP': ['PORT_RX_BW_UTILIZATION', 'PORT_TX_BW_UTILIZATION', 'ETHDROP']

            }

comb = []
# for_plot=[]
for f in ['IG27', 'IG30014', 'IG30024', 'IG30029', 'IG41022', 'IGMSTP', 'IGRTN']:

    df = pd.read_hdf('/disk2/support_files/archive/tx/tx_' + needed_day + '.h5', f, where='CollectionTime==needed_hour')
    files = pd.date_range(end=df['CollectionTime'].iloc[-1] - datetime.timedelta(1), periods=15, freq='24H').strftime(
        "%d.%m.%Y").tolist()
    hour = df['CollectionTime'].iloc[-1].strftime("%H:%M")
    cols = all_cols[f]
    h = []
    for i in files:
        if os.path.isfile(i):
            t = datetime.datetime.strptime(i + " " + hour, "%d.%m.%Y %H:%M")
            h.append(pd.read_hdf('/disk2/support_files/archive/tx/tx_' + i + '.h5', f, where='CollectionTime==t'))
        else: continue
    ready = pd.concat(h)
    Q1 = ready.groupby('ResourceName').quantile(0.25)[cols]
    Q3 = ready.groupby('ResourceName').quantile(0.75)[cols]
    filt = [i for i in Q1.columns if
            (('TSL' in i) | ('RSL' in i) | ('LCUR' in i) | ('IF_SNR' in i) | ('IF_MSE' in i))]
    df_filt = df[['DeviceName', 'ResourceName', 'CollectionTime'] + cols]
    merged = df_filt.melt(id_vars=df_filt.columns[:3], value_vars=df_filt.columns[3:]).merge(
        Q1.reset_index().melt(id_vars='ResourceName', value_name='Q1'), how='left', on=['ResourceName', 'variable'])
    merged = merged.merge(Q3.reset_index().melt(id_vars='ResourceName', value_name='Q3'),
                          on=['ResourceName', 'variable'], how='left')
    merged['status'] = 0
    merged.loc[((merged['value'] > (merged['Q3'] + 1.5 * (merged['Q3'] - merged['Q1']))) & (
        ~merged['variable'].isin(filt))), 'status'] = -1
    merged.loc[((merged['value'] > (merged['Q3'] + 1.5 * (merged['Q3'] - merged['Q1']))) & (
        merged['variable'].isin(filt))), 'status'] = -1
    merged.loc[((merged['value'] < (merged['Q1'] - 1.5 * (merged['Q3'] - merged['Q1']))) & (
        merged['variable'].isin(filt))), 'status'] = 1
    merged.loc[(merged['value'] < 60) & (merged['variable'].str.contains('UTILIZATION')), 'status'] = 0
    merged.loc[(merged['value'] < 60) & (merged['variable'].str.contains('Utilization')), 'status'] = 0
    merged.loc[
        (abs(abs(merged['value']) - abs(merged['Q1'])) < 2) & (merged['variable'].str.contains('RSL')), 'status'] = 0
    merged.loc[
        (abs(abs(merged['value']) - abs(merged['Q1'])) < 2) & (merged['variable'].str.contains('TSL')), 'status'] = 0
    merged.loc[
        (abs(abs(merged['value']) - abs(merged['Q1'])) < 2) & (merged['variable'].str.contains('CUR')), 'status'] = 0
    merged.loc[
        (abs(abs(merged['value']) - abs(merged['Q1'])) < 2) & (merged['variable'].str.contains('IF_MSE')), 'status'] = 0
    merged.loc[
        (abs(abs(merged['value']) - abs(merged['Q1'])) < 2) & (merged['variable'].str.contains('IF_SNR')), 'status'] = 0
    merged.loc[:, 'delta'] = round(
        (merged['value'] - (merged['Q3'] + 1.5 * (merged['Q3'] - merged['Q1']))) / merged['value'] * 100, 2)
    merged['delta'].replace(np.inf, -2, inplace=True)
    merged.insert(0, 'file', f)
    # ready_plot=ready[['DeviceName','ResourceName','CollectionTime']+cols].melt(id_vars=df_filt.columns[:3], value_vars=df_filt.columns[3:])
    # ready_plot.insert(0,'file',f)
    # for_plot.append(merged.append(ready_plot))
    comb.append(merged[merged['status'] != 0])
print('loop finished in ', time.time() - c1)
combed = pd.concat(comb)
# pd.concat(for_plot).to_hdf('/disk2/support_files/archive/anomalies.h5', '/tx_trend',
#                         format='table', complevel=5, data_columns=merged.columns)
combed.to_csv('/home/ismayil/flask_dash/support_files/anomality_detection/tx_anomalies.csv', index=False)
# combed.to_hdf('/disk2/support_files/archive/anomalies.h5', '/tx_level',
#                     format='table', data_columns=combed.columns, complevel=5)
print('append finished')
anomalies = pd.read_csv('/home/ismayil/flask_dash/support_files/anomality_detection/tx_ongoing_anomalies.csv')
anomalies = combed.merge(anomalies[['ResourceName', 'variable', 'status', 'c_days']],
                         on=['ResourceName', 'variable'], how='left')

anomalies.loc[anomalies['c_days'].notnull(), 'c_days'] += 1
anomalies.loc[anomalies['c_days'].isnull(), 'c_days'] = 1
anomalies.rename(columns={'status_x': 'status'}, inplace=True)
anomalies.drop(columns='status_y', inplace=True)
anomalies.to_hdf(
    '/disk2/support_files/archive/' + anomalies['CollectionTime'][0].strftime(
        "%B_%Y") + '_anomalies.h5', '/tx',
    append=True, format='table', data_columns=anomalies.columns, complevel=5,
    min_itemsize={'DeviceName': 40, 'ResourceName': 40, 'variable': 25, 'value': 15, 'Q1': 15, 'Q3': 15, 'delta': 15})
anomalies.to_csv('/home/ismayil/flask_dash/support_files/anomality_detection/tx_ongoing_anomalies.csv', index=False)