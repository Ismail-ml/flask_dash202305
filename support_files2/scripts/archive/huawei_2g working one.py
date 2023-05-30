import pandas as pd
import glob, time, os, re
import dask.dataframe as dd
import concurrent.futures as cf
import numpy as np
import datetime
try:
    def run(path1,path2,path3,tracker):
        print('Huawei 2G aggregation begin...')
        c1 = time.time()

        hw = pd.DataFrame()

        def proccess_excel(diddd):
            #existing_files = pd.read_csv(os.path.join(path1, 'files.txt'), sep=" ", header=None)
            all_files = glob.glob(diddd + "/*.csv")
            for filename in all_files:
                #if os.path.basename(filename) in existing_files: continue
                df = pd.read_csv(filename, skiprows=[1])
                df['cssr_num1'] = df['1278072520']
                df['cssr_den1'] = df['1278087421']
                df['cssr_num2'] = df['1278087432']
                df['cssr_den2'] = df['1278087430']
                df['cssr_num3'] = df['1278087421']
                df['cssr_den3'] = df['1278087419']
                df['sdcch_block_rate_num'] = df['1278087420']
                df['sdcch_block_rate_den'] = df['1278087419']
                df['sdcch_drop_rate_num'] = df['1278072520']
                df['sdcch_drop_rate_den'] = df['1278087421']
                df['tch_avail_num'] = df['1278087439']
                df['tch_avail_den'] = df['1278087440']
                df['cs_traffic_erl'] = df['1278087438']
                df['drop_rate_num'] = df['1278072498']
                df['tbf_est_sr_num'] = df['1279174418'] + df['1279176418'] + df['1279173418'] + df['1279175418']
                df['tbf_est_sr_den'] = df['1279174417'] + df['1279176417'] + df['1279173417'] + df['1279175417']
                df['tbf_drop_rate_num'] = df['1279173422'] + df['1279173423'] + df['1279174434'] + df['1279175422'] + df[
                    '1279175423'] + df['1279176434']
                df['tbf_drop_rate_den'] = df['1279173418'] + df['1279174418'] + df['1279175418'] + df['1279176418']
                df['dcr_den_1'] = df['1278087432'] + df['1278080467'] - df['1278081557']
                df = df[['Object Name', 'Result Time', 'drop_rate_num',
                         'sdcch_block_rate_num', 'sdcch_block_rate_den', 'tch_avail_num', 'tch_avail_den',
                         'sdcch_drop_rate_num',
                         'sdcch_drop_rate_den', 'cs_traffic_erl', 'tbf_est_sr_num',
                         'tbf_est_sr_den', 'tbf_drop_rate_num', 'tbf_drop_rate_den', 'cssr_num1', 'cssr_den1', 'cssr_num2',
                         'cssr_den2',
                         'cssr_num3', 'cssr_den3', 'dcr_den_1', '1278087426', '1278087430',
                         '1279180454', '1278081557', '1278081558']]
                df['Date'] = pd.to_datetime(df['Result Time'], format='%Y-%m-%d %H:%M')
                df['BSC_name'] = df['Object Name'].apply(lambda x: x[:6])
                df['Cell_name'] = df['Object Name'].apply(lambda x: re.search(r'\w{4}\d{4}\w', x).group())
                df['Site_name'] = df['Cell_name'].apply(lambda x: x[:8])
                li.append(df)

        h = []
        [h.append(r) for r, d, folder in os.walk(path1)]
        li = []

        with cf.ThreadPoolExecutor() as executor:
            executor.map(proccess_excel, h)

        hw = pd.concat(li, axis=0, ignore_index=True, sort=False)
        print(time.time() - c1, 'part1 finish')

        hw2 = pd.DataFrame()

        def proccess_excel2(diddd):
            #existing_files = pd.read_csv(os.path.join(path2, 'files.txt'), sep=" ", header=None)
            all_files = glob.glob(diddd + "/*.csv")
            for filename in all_files:
                #if os.path.basename(filename) in existing_files: continue
                df = pd.read_csv(filename, skiprows=[1])
                df['sdcch_avail_num'] = df['1278087423']
                df['sdcch_avail_den'] = df['1278087424']
                df['comb_thrp_num'] = np.nan_to_num(
                    df['1279184442'] / (df['1279184441'] * 8 / 1024) * df['1279184441']) + np.nan_to_num(
                    df['1279184444'] / (df['1279184443'] * 8 / 1024) * df['1279184443']) + np.nan_to_num(
                    df['1279183438'] / (df['1279183437'] * 8 / 1024) * df['1279183437']) + np.nan_to_num(
                    df['1279183440'] / (df['1279183439'] * 8 / 1024) * df['1279183439'])
                df['comb_thrp_den'] = df['1279184441'] + df['1279184443'] + df['1279183437'] + df['1279183439']
                df['dcr_den_2'] = df['1278078459'] + df['1278082436'] - df['1278079528']
                df['ps_traf_2'] = df['1279177439'] + df['1279178439'] + df['1279179453']
                df = df[['Object Name', 'Result Time', 'sdcch_avail_num',
                         'sdcch_avail_den', 'comb_thrp_num', 'comb_thrp_den', 'dcr_den_2', 'ps_traf_2',
                         '1278087431', '1278087425', '1278079531', '1278079528']]
                df['Date'] = pd.to_datetime(df['Result Time'], format='%Y-%m-%d %H:%M')
                df['BSC_name'] = df['Object Name'].apply(lambda x: x[:6])
                df['Cell_name'] = df['Object Name'].apply(lambda x: re.search(r'\w{4}\d{4}\w', x).group())
                # df['Site_name']=df['Cell_name'].apply(lambda x: x[:8])
                li.append(df)

        h = []
        [h.append(r) for r, d, folder in os.walk(path2)]
        li = []

        with cf.ThreadPoolExecutor() as executor:
            executor.map(proccess_excel2, h)

        hw2 = pd.concat(li, axis=0, ignore_index=True, sort=False)
        print(time.time() - c1, 'part2 finish')

        hw3 = pd.DataFrame()

        def proccess_excel3(diddd):
            #existing_files = pd.read_csv(os.path.join(path3, 'files.txt'), sep=" ", header=None)
            all_files = glob.glob(diddd + "/*.csv")
            for filename in all_files:
                #if os.path.basename(filename) in existing_files: continue
                df = pd.read_csv(filename, skiprows=[1])
                df['Date'] = pd.to_datetime(df['Result Time'], format='%Y-%m-%d %H:%M')
                df['BSC_name'] = df['Object Name'].apply(lambda x: x[:6])
                df['Cell_name'] = df['Object Name'].apply(lambda x: re.search(r'\w{4}\d{4}\w', x).group())
                # df['Site_name']=df['Cell_name'].apply(lambda x: x[:8])
                li.append(df)

        h = []
        [h.append(r) for r, d, folder in os.walk(path3)]
        li = []

        with cf.ThreadPoolExecutor() as executor:
            executor.map(proccess_excel3, h)

        hw3 = pd.concat(li, axis=0, ignore_index=True, sort=False)
        print(time.time() - c1, 'part3 finish')

        hw_agr1 = pd.merge(hw, hw2, on=['Cell_name', 'BSC_name', 'Date'], how='left')
        hw_agr = pd.merge(hw_agr1, hw3, on=['Cell_name', 'BSC_name', 'Date'], how='left')
        print(time.time() - c1)

        hw_agr['drop_rate_den'] = hw_agr['dcr_den_1'] + hw_agr['dcr_den_2'] - hw_agr['1278083469']
        hw_agr['call_block_rate_num'] = hw_agr['1278087431'] + hw_agr['1278087426']
        hw_agr['call_block_rate_den'] = hw_agr['1278087430'] + hw_agr['1278087425']
        try:
            hw_agr['cell_avail_blck_num'] = hw_agr['1282438383']
        except:
            hw_agr['cell_avail_blck_num'] = 0
        hw_agr['cell_avail_den'] = 3600
        hw_agr['cell_avail_num'] = hw_agr['1276071425'] - np.nan_to_num(hw_agr['cell_avail_blck_num'])
        hw_agr['cell_avail_blck_den'] = 0
        hw_agr['hosr_num'] = hw_agr['1278079528'] + hw_agr['1278081557'] + hw_agr['1278077482']
        hw_agr['hosr_den'] = hw_agr['1278079531'] + hw_agr['1278081558'] + hw_agr['1278077486']
        hw_agr['ps_traffic_mb'] = (hw_agr['ps_traf_2'] + hw_agr['1279180454']) / 1024

        # hw_agr_finish.drop(labels=['Object Name','Result Time'],axis=1,inplace=True)

        print(time.time() - c1, 'whole finish')

        hw_agr['Vendor'] = 'Huawei'
        hw_agr['lookup'] = hw_agr['Site_name'].apply(lambda x: x[1:])
        # tracker=pd.read_excel(r'\\file-server\AZERCONNECT_LLC_OLD\Corporate Folder\CTO\Technology trackers\RNP\Azerconnect_RNP_tracker.xlsx',skiprows=[0])
        hw_agr = pd.merge(hw_agr, tracker[['SITE_ID', 'Economical Region']], left_on='lookup', right_on='SITE_ID',
                          how='left')
        hw_agr.rename(columns={'Economical Region': 'Region'}, inplace=True)
        print(time.time() - c1, 'end of merging')
        hw_agr_finish = hw_agr[['Date', 'Vendor', 'BSC_name', 'Site_name', 'Cell_name', 'Region', 'call_block_rate_den',
                                'call_block_rate_num', 'cell_avail_den', 'cell_avail_num', 'cell_avail_blck_num',
                                'cell_avail_blck_den',
                                'comb_thrp_den', 'comb_thrp_num', 'cs_traffic_erl', 'cssr_den1',
                                'cssr_den2', 'cssr_den3', 'cssr_num1', 'cssr_num2', 'cssr_num3',
                                'drop_rate_den', 'drop_rate_num', 'hosr_den', 'hosr_num',
                                'ps_traffic_mb', 'sdcch_avail_den', 'sdcch_avail_num',
                                'sdcch_block_rate_den', 'sdcch_block_rate_num', 'sdcch_drop_rate_den',
                                'sdcch_drop_rate_num', 'tbf_drop_rate_den', 'tbf_drop_rate_num',
                                'tbf_est_sr_den', 'tbf_est_sr_num', 'tch_avail_den', 'tch_avail_num']]
        hw_agr_finish.iloc[:, 6:] = hw_agr_finish.iloc[:, 6:].astype(np.float64)
        hw_agr_finish['cell_avail_blck_num'].fillna(0, inplace=True)
        hw_agr_finish.loc[:,'cell_avail_blck_num']=hw_agr_finish.loc[:,'cell_avail_blck_num'].astype(np.int64)
        hw_agr_finish.loc[:,'cell_avail_blck_den']=hw_agr_finish.loc[:,'cell_avail_blck_den'].astype(np.int64)
        hw_agr_finish.drop_duplicates(keep='first',inplace=True)
        print(time.time() - c1, 'new table created')
        hw_bsc = hw_agr_finish.groupby(['Date', 'BSC_name', 'Vendor', 'Region']).sum()
        hw_bsc.reset_index(inplace=True)
        hw_network = hw_agr_finish.groupby(['Date']).sum()
        hw_network.reset_index(inplace=True)
        #file_name = datetime.datetime.strftime(hw_bsc['Date'].iloc[0], "%B_%Y") it was working
        #file_name2 = datetime.datetime.strftime(hw_bsc['Date'].iloc[0], "%Y-%m-%d") it was working
        print(time.time() - c1, 'time to save')
        #print('File name is', file_name, ' and filename2 is ', file_name2)
        for i in hw_bsc['Date'].unique():

            file_name = datetime.datetime.strftime(datetime.datetime.utcfromtimestamp(i.astype(datetime.datetime)/1e9), "%B_%Y")
            file_name2 = datetime.datetime.strftime(datetime.datetime.utcfromtimestamp(i.astype(datetime.datetime)/1e9), "%Y-%m-%d")
            hw_agr_finish.loc[hw_agr_finish['Date']==i].to_hdf(r'/disk2/support_files/archive/ran/' + file_name2 + '.h5', '/twoG', append=True,
                                 format='table',
                                 data_columns=['Date', 'BSC_name', 'Site_name', 'Cell_name', 'Vendor', 'Region'], complevel=5,
                                 min_itemsize={'BSC_name': 10, 'Site_name': 20, 'Cell_name': 20, 'Vendor': 10, 'Region': 15})
            hw_bsc.loc[hw_bsc['Date']==i].to_hdf(r'/disk2/support_files/archive/ran/' + file_name2 + '.h5', '/twoG/bsc', append=True,
                          format='table', data_columns=['Date', 'BSC_name', 'Vendor', 'Region'], complevel=5,
                          min_itemsize={'BSC_name': 10, 'Vendor': 10, 'Region': 15})
        #hw_agr_finish.to_hdf(r'/disk2/support_files/archive/'+file_name+'.h5','/twoG',append=True,
        #           format='table',data_columns=['Date','BSC_name','Site_name','Cell_name','Vendor','Region'],complevel=5,
        #                min_itemsize={'BSC_name':10,'Site_name':20,'Cell_name':20,'Vendor':10,'Region':15})
        #hw_bsc.to_hdf(r'/disk2/support_files/archive/'+file_name+'.h5','/twoG/bsc',append=True,
        #           format='table',data_columns=['Date','BSC_name','Vendor','Region'],complevel=5,
        #                min_itemsize={'BSC_name':10,'Vendor':10,'Region':15})
        hw_bsc.to_hdf(r'/disk2/support_files/archive/combined_bsc.h5', '/twoG',append=True,
                      format='table', data_columns=['Date', 'BSC_name', 'Vendor', 'Region'], complevel=5,
                      min_itemsize={'BSC_name': 10, 'Vendor': 10, 'Region': 15})


        print(time.time()-c1)
except Exception as e:
    print(e)
