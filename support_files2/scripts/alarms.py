# uncompyle6 version 3.9.0
# Python bytecode version base 3.8.0 (3413)
# Decompiled from: Python 3.9.9 (tags/v3.9.9:ccb0e6a, Nov 15 2021, 18:08:50) [MSC v.1929 64 bit (AMD64)]
# Embedded file name: /home/ismayil/flask_dash/support_files/scripts/alarms.py
# Compiled at: 2023-02-14 15:53:45
# Size of source mod 2**32: 8828 bytes
import pandas as pd, glob, os, datetime, numpy as np

def check_alarm(b):

    def haversine(lon1, lat1, lon2, lat2):
        """
        Calculate the great circle distance between two points
        on the earth (specified in decimal degrees)
        """
        lon1 = np.radians(lon1.values)
        lat1 = np.radians(lat1.values)
        lon2 = np.radians(lon2.values)
        lat2 = np.radians(lat2.values)
        dlon = np.subtract(lon2, lon1)
        dlat = np.subtract(lat2, lat1)
        a = np.add(np.power(np.sin(np.divide(dlat, 2)), 2), np.multiply(np.cos(lat1), np.multiply(np.cos(lat2), np.power(np.sin(np.divide(dlon, 2)), 2))))
        c = np.multiply(2, np.arcsin(np.sqrt(a)))
        r = 6371
        return c * r

    required = datetime.datetime.utcfromtimestamp(b['Date'].values[0].item() / 1000000000).strftime('%Y%m%d%H')
    dfs = []
    lil = glob.glob('/mnt/raw_counters/Corporate Folder/CTO/Alarm_LOG/NETACT*' + required + '*.csv')
    for i in lil:
        df = pd.read_csv(i, index_col=False)
        df = df[df['TEXT'].notnull()]
        dfs.append(df[df['TEXT'].str.lower().str.contains('oml fault|nodeb unavailable|s1ap link down|bts o&m link failure|                failure in wcdma wbts o&m connection|s1ap link down')])
    else:
        nokia = pd.concat(dfs)
        nokia['Site'] = nokia['NE_NAME'].apply(lambda x: str(x)[1:8])
        nokia['temp'] = ''
        nokia.loc[(nokia['Site'] == 'an', 'temp')] = '1'
        nokia.loc[(nokia['Site'] == 'an', 'Site')] = nokia.loc[(nokia['Site'] == 'an', 'DN')]
        for i in nokia.loc[(nokia['temp'] == '1', 'Site')].unique():
            try:
                nokia.loc[(nokia['Site'] == i, 'Site')] = nokia[(nokia['DN'] == i) & nokia['NE_NAME'].notnull()]['NE_NAME'].iloc[0]
            except:
                continue

        else:
            nokia.loc[(nokia['temp'] == '1', 'Site')] = nokia.loc[(nokia['temp'] == '1', 'Site')].apply(lambda x: str(x)[1:8])
            nokia.rename(columns={'TEXT':'AlarmName',  'ALARM_TIME':'AlarmTime'}, inplace=True)
            nokia = nokia[['Site', 'AlarmName', 'AlarmTime']]
            dfs = []
            for i in glob.glob('/mnt/raw_counters/Corporate Folder/CTO/ALARM_LOG/*' + required + '*.zip'):
                df = pd.read_csv(i, compression='zip')
                dfs.append(df[df['AlarmName'].str.lower().str.contains('oml fault|nodeb unavailable|s1ap link down|bts o&m link failure|                failure in wcdma wbts o&m connection|s1ap link down|s1ap link down')])
            else:
                huawei = pd.concat(dfs)
                huawei['Site'] = huawei['Object Identity Name'].replace({'NodeB Name=':'',  'eNodeB Function Name=':'',  'Label=':''}, regex=True)
                huawei.loc[(huawei['Site'].str.contains('USN9810BHQ'), 'Site')] = huawei.loc[huawei['Site'].str.contains('USN9810BHQ')]['LocationInformation'].apply(lambda x: x[x.find('EnodeBName=') + 11:x.find(', EnodeBIPAddress')])
                huawei['Site'] = huawei['Site'].apply(lambda x: x[1:8])
                huawei['AlarmTime'] = pd.to_datetime((huawei['OccurrenceTime']), format='%Y/%m/%d %H:%M:%S GMT+04:00')
                huawei['ff'] = huawei['Site'].apply(lambda x: x[:3])
                huawei[['Site', 'ff']] = huawei[['Site', 'ff']].astype(str)
                huawei = huawei[huawei['ff'].isin(["'ABS'", "'BBK'", "'MBS'", "'COW'", "'SVN'", "'SHK'", "'LNK'", 
                 "'NCV'", "'QUB'", "'GNJ'", "'QRB'"])]
                huawei = huawei[['Site', 'AlarmName', 'AlarmTime']]
                tracker = pd.read_csv('/home/ismayil/flask_dash/support_files/tracker.csv')
                ftt = pd.concat([nokia, huawei])
                h = []
                for i in b['Site_name'].unique():
                    new = tracker[['SITE_ID', 'Long', 'Lat']]
                    new['dist'] = haversine(new.loc[(new['SITE_ID'] == i[1:8], 'Long')], new.loc[(tracker['SITE_ID'] == i[1:8], 'Lat')], new['Long'], new['Lat'])
                    new = new.sort_values(by='dist').iloc[1:16]
                    f = ftt.merge(new, left_on='Site', right_on='SITE_ID')
                    f['origin_site'] = i
                    h.append(f[["'origin_site'", "'Site'", "'dist'", "'AlarmName'", "'AlarmTime'"]])
                else:
                    b['lookup'] = b['Site_name'].apply(lambda x: x[1:8])
                    df8 = ftt[ftt['Site'].isin(b['lookup'].unique())]
                    df = pd.concat(h).drop_duplicates()
                    df.loc[(df['AlarmName'] == 'OML Fault', '2G down')] = df.loc[df['AlarmName'] == 'OML Fault']['AlarmTime']
                    df.loc[(df['AlarmName'] == 'BTS O&M LINK FAILURE', '2G down')] = df.loc[df['AlarmName'] == 'BTS O&M LINK FAILURE']['AlarmTime']
                    df.loc[(df['AlarmName'] == 'FAILURE IN WCDMA WBTS O&M CONNECTION', '3G down')] = df.loc[df['AlarmName'] == 'FAILURE IN WCDMA WBTS O&M CONNECTION']['AlarmTime']
                    df.loc[(df['AlarmName'] == 'NodeB Unavailable', '3G down')] = df.loc[df['AlarmName'] == 'NodeB Unavailable']['AlarmTime']
                    df.loc[(df['AlarmName'] == 'S1ap Link Down', '4G down')] = df.loc[df['AlarmName'] == 'S1ap Link Down']['AlarmTime']
                    df7 = df.groupby(['origin_site', 'Site']).count()['AlarmTime'].reset_index()
                    for m in ('2G down', '3G down', '4G down'):
                        if m not in df.columns:
                            df.loc[m, :] = np.nan
                        tmp_df = df[[m, 'Site']]
                        pivot_table = tmp_df.pivot_table(index='Site',
                          columns=m,
                          values=m,
                          aggfunc={m: 'count'})
                        pivot_table = pivot_table.reset_index()
                        pivot_table = pivot_table.melt(id_vars='Site', value_vars=(pivot_table.columns[1:]))
                        pivot_table = pivot_table[pivot_table['value'].notnull()]
                        pivot_table.rename(columns={'variable': m}, inplace=True)
                        df7 = df7.merge(pivot_table, left_on='Site', right_on='Site', how='left')
                    else:
                        df7 = df7.merge((df[['Site', 'dist']].groupby('Site').mean().reset_index()), on='Site', how='left')
                        df7['dist'] = round(df7['dist'], 2)
                        df7.rename(columns={'dist':'dist_km',  'Site':'Neighbour_site'}, inplace=True)
                        df7 = df7[["'origin_site'", "'Neighbour_site'", "'dist_km'", "'2G down'", 
                         "'3G down'", "'4G down'"]]
                        df7['Unique down'] = np.nan
                        df7.loc[(df7['2G down'].notnull() | df7['3G down'].notnull() | df7['4G down'].notnull(), 'Unique down')] = 1
                        if len(df8) > 0:
                            df8.loc[(df8['AlarmName'] == 'OML Fault', '2G down')] = df8.loc[df8['AlarmName'] == 'OML Fault']['AlarmTime']
                            df8.loc[(df8['AlarmName'] == 'BTS O&M LINK FAILURE', '2G down')] = df8.loc[df8['AlarmName'] == 'BTS O&M LINK FAILURE']['AlarmTime']
                            df8.loc[(df8['AlarmName'] == 'FAILURE IN WCDMA WBTS O&M CONNECTION', '3G down')] = df8.loc[df8['AlarmName'] == 'FAILURE IN WCDMA WBTS O&M CONNECTION']['AlarmTime']
                            df8.loc[(df8['AlarmName'] == 'NodeB Unavailable', '3G down')] = df8.loc[df8['AlarmName'] == 'NodeB Unavailable']['AlarmTime']
                            df8.loc[(df8['AlarmName'] == 'S1ap Link Down', '4G down')] = df8.loc[df8['AlarmName'] == 'S1ap Link Down']['AlarmTime']
                            df9 = df8.groupby(['Site']).count()['AlarmTime'].reset_index()
                            for m in ('2G down', '3G down', '4G down'):
                                if m not in df8.columns:
                                    df8.loc[m, :] = np.nan
                                tmp_df = df8[[m, 'Site']]
                                pivot_table = tmp_df.pivot_table(index='Site',
                                  columns=m,
                                  values=m,
                                  aggfunc={m: 'count'})
                                pivot_table = pivot_table.reset_index()
                                pivot_table = pivot_table.melt(id_vars='Site', value_vars=(pivot_table.columns[1:]))
                                pivot_table = pivot_table[pivot_table['value'].notnull()]
                                pivot_table.rename(columns={'variable': m}, inplace=True)
                                df9 = df9.merge(pivot_table, left_on='Site', right_on='Site', how='left')
                            else:
                                df9['Down_tech'] = ''
                                df9.loc[(df9['2G down'].notnull(), 'Down_tech')] += '2G'
                                df9.loc[(df9['3G down'].notnull(), 'Down_tech')] += '/3G'
                                df9.loc[(df9['4G down'].notnull(), 'Down_tech')] += '/4G'
                                df9 = df9[['Site', 'Down_tech']]

                        else:
                            df9 = df8
                        return (df7.groupby('origin_site').sum()['Unique down'].reset_index(), df7[["'origin_site'", "'Neighbour_site'", "'dist_km'", "'2G down'", 
                          "'3G down'", "'4G down'"]],
                         df9, df[['origin_site', 'Site']].drop_duplicates().groupby('origin_site').count().reset_index())
# okay decompiling alarms.cpython-38.pyc
