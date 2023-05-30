import os
import pandas as pd
import glob,time
import dask.dataframe as dd
import numpy as np
import datetime

def run(path1,path2,tracker):
    try:
        print('Nokia 2G aggregation begin...')
        c1=time.time()
        nokia_2G_1={'BCF name': 'object','AVE_AVAIL_TCH_DEN (c002060)': 'float64',       'AVE_AVAIL_TCH_SUM (c002059)': 'float64',
               'AVE_GPRS_CHANNELS_DEN (c002062)': 'float64',       'AVE_GPRS_CHANNELS_SUM (c002061)': 'float64',
               'AVE_NON_AVAIL_SDCCH (c002038_1)': 'float64',       'AVE_NON_AVAIL_TCH_TIMESLOT (c002069)': 'float64',
               'AVE_SDCCH_SUB (c002004)': 'float64',       'AVE_TCH_BUSY_FULL (c002046_1)': 'float64',
               'AVE_TCH_BUSY_HALF (c002048_1)': 'float64',       'BSC_O_HO_CMD_ASSGN (c001196)': 'float64',
               'BTS_HO_ASSGN (c001197)': 'float64',       'DROP_AFTER_TCH_ASSIGN (c001202)': 'float64',
               'FORCED_RELEASES (c001070)': 'float64',       'MSC_O_HO_CMD (c001195)': 'float64',
               'NON_AVAIL_TCH_DENOM (c002070)': 'float64',       'RES_AV_DENOM1 (c002001)': 'float64',
               'SDCCH_ABIS_FAIL_CALL (c001075)': 'float64',       'SDCCH_ABIS_FAIL_OLD (c001076)': 'float64',
               'SDCCH_ASSIGN (c001007)': 'float64',       'SDCCH_A_IF_FAIL_CALL (c001078)': 'float64',
               'SDCCH_A_IF_FAIL_OLD (c001079)': 'float64',       'SDCCH_BCSU_RESET (c001038)': 'float64',
               'SDCCH_BTS_FAIL (c001036)': 'float64',       'SDCCH_BUSY_ATT (c001001)': 'float64',
               'SDCCH_HO_SEIZ (c001006)': 'float64',       'SDCCH_LAPD_FAIL (c001035)': 'float64',
               'SDCCH_NETW_ACT (c001039)': 'float64',       'SDCCH_RADIO_FAIL (c001003)': 'float64',
               'SDCCH_RF_OLD_HO (c001004)': 'float64',       'SDCCH_SEIZ_ATT (c001000)': 'float64',
               'SDCCH_USER_ACT (c001037)': 'float64',       'SUCC_TCH_SEIZ_CALL_ATTEMPT (c001193)': 'float64',
               'TCH_REQUESTS_CALL_ATTEMPT (c001192)': 'float64',       'TCH_SEIZ_ATT_DUE_SDCCH_CON (c001098)': 'float64',
               'SDCCH_DROP_CALL_AND_HO (c001191)': 'float64','SDCCH_ALLOC_FOR_VOICE_CALL (c002085)':'float64',
                'TCH_NORM_RELEASE (c057035)':'float64','TCH_RE_EST_RELEASE (c057037)':'float64',
                'MSC_O_HO_CMD (c001195)':'float64','BSC_O_HO_CMD_ASSGN (c001196)':'float64',
                'BTS_HO_ASSGN (c001197)':'float64','MSC_GEN_SYS_WCDMA_RAN_HO_COM (c004154)':'float64',
                'T3101_EXPIRED (c057020)':'float64','DL GPRS RLC payload':'float64','UL GPRS RLC payload':'float64',
                'DL EGPRS RLC payload':'float64','UL EGPRS RLC payload':'float64',
           'ALLOC_1_TSL_DL (c072049)': 'float64',       'ALLOC_1_TSL_UL (c072044)': 'float64',
           'ALLOC_2_TSL_DL (c072050)': 'float64',       'ALLOC_2_TSL_UL (c072045)': 'float64',
           'ALLOC_3_TSL_DL (c072051)': 'float64',       'ALLOC_3_TSL_UL (c072046)': 'float64',
           'ALLOC_4_TSL_DL (c072052)': 'float64',       'ALLOC_4_TSL_UL (c072047)': 'float64',
           'ALLOC_5_TSL_DL (c072137)': 'float64',       'ALLOC_5_TSL_UL (c072133)': 'float64',
           'ALLOC_6_TSL_DL (c072138)': 'float64',       'ALLOC_6_TSL_UL (c072134)': 'float64',
           'ALLOC_7_TSL_DL (c072139)': 'float64',       'ALLOC_7_TSL_UL (c072135)': 'float64',
           'ALLOC_8_TSL_DL (c072140)': 'float64',       'ALLOC_8_TSL_UL (c072136)': 'float64',
           'AVE_DUR_DL_TBF_DEN (c072009)': 'float64',       'AVE_DUR_UL_TBF_DEN (c072004)': 'float64',
           'DL_EGPRS_TBF_REL_DUE_NO_RESP (c072095)': 'float64',       'DL_TBF_ESTABLISHMENT_FAILED (c072093)': 'float64',
           'DL_TBF_ESTABL_STARTED (c072122)': 'float64',       'DL_TBF_RELEASES_DUE_DTM (c072202)': 'float64',
           'DL_TBF_REL_DUE_TO_FLUSH (c072059)': 'float64',       'DL_TBF_REL_DUE_TO_SUSPEND (c072061)': 'float64',
           'NBR_OF_DL_TBF (c072005)': 'float64',       'NBR_OF_UL_TBF (c072000)': 'float64',
           'NO_RADIO_RES_AVA_DL_TBF (c072080)': 'float64',       'NO_RADIO_RES_AVA_UL_TBF (c072079)': 'float64',
           'REQ_1_TSL_DL (c072039)': 'float64',       'REQ_1_TSL_UL (c072034)': 'float64',
           'REQ_2_TSL_DL (c072040)': 'float64',       'REQ_2_TSL_UL (c072035)': 'float64',
           'REQ_3_TSL_DL (c072041)': 'float64',       'REQ_3_TSL_UL (c072036)': 'float64',
           'REQ_4_TSL_DL (c072042)': 'float64',       'REQ_4_TSL_UL (c072037)': 'float64',
           'REQ_5_TSL_DL (c072129)': 'float64',       'REQ_5_TSL_UL (c072125)': 'float64',
           'REQ_6_TSL_DL (c072130)': 'float64',       'REQ_6_TSL_UL (c072126)': 'float64',
           'REQ_7_TSL_DL (c072131)': 'float64',       'REQ_7_TSL_UL (c072127)': 'float64',
           'REQ_8_TSL_DL (c072132)': 'float64',       'REQ_8_TSL_UL (c072128)': 'float64',
           'UL_EGPRS_TBF_REL_DUE_NO_RESP (c072094)': 'float64',       'UL_TBF_ESTABLISHMENT_FAILED (c072092)': 'float64',
           'UL_TBF_ESTABL_STARTED (c072121)': 'float64',       'UL_TBF_RELEASES_DUE_DTM (c072201)': 'float64',
           'UL_TBF_REL_DUE_TO_FLUSH (c072058)': 'float64',       'UL_TBF_REL_DUE_TO_SUSPEND (c072060)': 'float64'}
        nokia_2G_2={'BCF name': 'object','LLC_BYTES_DL (c097020)': 'float64',       'LLC_BYTES_DL_EGPRS (c097022)': 'float64',
               'LLC_BYTES_UL (c097019)': 'float64',       'LLC_BYTES_UL_EGPRS (c097021)': 'float64',
               'new_llc_10_den': 'float64',       'new_llc_10_num': 'float64',
               'new_llc_11_den': 'float64',       'new_llc_11_num': 'float64',
               'new_llc_12_den': 'float64',       'new_llc_12_num': 'float64',
               'new_llc_13_den': 'float64',       'new_llc_13_num': 'float64'}
        an=[]
        nsn=pd.DataFrame()
        for dirpath,dirname,filenames in os.walk(path1):
            #existing_files=pd.read_csv(os.path.join(path1,'files.txt'), sep=" ", header=None)
            all_files = glob.glob(dirpath + "/*.csv")
            #print('For dirpath:',dirpath,'len is:',len(all_files))
            if len(all_files)>0:
                li = []
                for filename in all_files:
                    #if os.path.basename(filename) in existing_files: continue
                    df = dd.read_csv(filename,sep=';',dtype=nokia_2G_1)
                    df['drop_rate_num']=df['DROP_AFTER_TCH_ASSIGN (c001202)']+df['FORCED_RELEASES (c001070)']
                    df['drop_rate_den']=df['TCH_NORM_RELEASE (c057035)']+df['TCH_RE_EST_RELEASE (c057037)']+df['DROP_AFTER_TCH_ASSIGN (c001202)']+df['FORCED_RELEASES (c001070)']
                    df['call_block_rate_num']=df['TCH_REQUESTS_CALL_ATTEMPT (c001192)']-df['SUCC_TCH_SEIZ_CALL_ATTEMPT (c001193)']
                    df['call_block_rate_den']=df['TCH_REQUESTS_CALL_ATTEMPT (c001192)']
                    df['cell_avail_num']=df['BCCH_UPTIME (c002093)']
                    df['cell_avail_den']=df['BCCH_DOWNTIME (c002092)']+df['BCCH_UPTIME (c002093)']
                    df['hosr_num']=df['MSC_O_SUCC_HO (c004004)']+df['BSC_O_SUCC_HO (c004014)']+df['CELL_SUCC_HO (c004018)']+df['MSC_TO_WCDMA_RAN_SUCC_TCH_HO (c004158)']
                    df['hosr_den']=df['MSC_O_HO_CMD (c001195)']+df['BSC_O_HO_CMD_ASSGN (c001196)']+df['BTS_HO_ASSGN (c001197)']+df['MSC_GEN_SYS_WCDMA_RAN_HO_COM (c004154)']
                    df['sdcch_avail_num']=df['AVE_SDCCH_SUB (c002004)']
                    df['sdcch_avail_den']=df['AVE_SDCCH_SUB (c002004)']+df['AVE_NON_AVAIL_SDCCH (c002038_1)']*180
                    df['sdcch_block_rate_num']=df['SDCCH_BUSY_ATT (c001001)']-df['TCH_SEIZ_ATT_DUE_SDCCH_CON (c001098)']
                    df['sdcch_block_rate_den']=df['SDCCH_SEIZ_ATT (c001000)']
                    df['tch_avail_num']=df['AVE_AVAIL_TCH_SUM (c002059)']/df['AVE_AVAIL_TCH_DEN (c002060)']+df['AVE_GPRS_CHANNELS_SUM (c002061)']/df['AVE_GPRS_CHANNELS_DEN (c002062)']
                    df['tch_avail_den']=df['AVE_AVAIL_TCH_SUM (c002059)']/df['AVE_AVAIL_TCH_DEN (c002060)']+df['AVE_GPRS_CHANNELS_SUM (c002061)']/df['AVE_GPRS_CHANNELS_DEN (c002062)']+df['AVE_NON_AVAIL_TCH_TIMESLOT (c002069)']/df['NON_AVAIL_TCH_DENOM (c002070)']
                    df['sdcch_drop_rate_num']=df['SDCCH_RADIO_FAIL (c001003)']+df['SDCCH_RF_OLD_HO (c001004)']+df['SDCCH_USER_ACT (c001037)']+df['SDCCH_BCSU_RESET (c001038)']+df['SDCCH_NETW_ACT (c001039)']+df['SDCCH_ABIS_FAIL_CALL (c001075)']+df['SDCCH_ABIS_FAIL_OLD (c001076)']+df['SDCCH_BTS_FAIL (c001036)']+df['SDCCH_LAPD_FAIL (c001035)']+df['SDCCH_A_IF_FAIL_CALL (c001078)']+df['SDCCH_A_IF_FAIL_OLD (c001079)']-df['T3101_EXPIRED (c057020)']
                    df['sdcch_drop_rate_den']=df['SDCCH_ASSIGN (c001007)']+df['SDCCH_HO_SEIZ (c001006)']
                    df['cs_traffic_erl']=df['TCH traffic sum']
                    df['ps_traffic_mb']=(df['DL GPRS RLC payload']+df['UL GPRS RLC payload']+df['DL EGPRS RLC payload']+df['UL EGPRS RLC payload'])/1024
                    df['tbf_est_sr_num']=df['NBR_OF_DL_TBF (c072005)']+df['NBR_OF_UL_TBF (c072000)']-df['DL_TBF_ESTABLISHMENT_FAILED (c072093)']-df['DL_EGPRS_TBF_REL_DUE_NO_RESP (c072095)']-df['UL_TBF_ESTABLISHMENT_FAILED (c072092)']-df['UL_EGPRS_TBF_REL_DUE_NO_RESP (c072094)']
                    df['tbf_est_sr_den']=df['DL_TBF_ESTABL_STARTED (c072122)']+df['UL_TBF_ESTABL_STARTED (c072121)']
                    df['tbf_drop_rate_num']=df['NBR_OF_UL_TBF (c072000)']+df['NBR_OF_DL_TBF (c072005)']-df['UL_TBF_ESTABLISHMENT_FAILED (c072092)']-df['DL_TBF_ESTABLISHMENT_FAILED (c072093)']-df['UL_EGPRS_TBF_REL_DUE_NO_RESP (c072094)']-df['DL_EGPRS_TBF_REL_DUE_NO_RESP (c072095)']-df['AVE_DUR_UL_TBF_DEN (c072004)']-df['AVE_DUR_DL_TBF_DEN (c072009)']-df['UL_TBF_REL_DUE_TO_FLUSH (c072058)']-df['DL_TBF_REL_DUE_TO_FLUSH (c072059)']-df['UL_TBF_REL_DUE_TO_SUSPEND (c072060)']-df['DL_TBF_REL_DUE_TO_SUSPEND (c072061)']-df['UL_TBF_RELEASES_DUE_DTM (c072201)']-df['DL_TBF_RELEASES_DUE_DTM (c072202)']
                    df['tbf_drop_rate_den']=df['NBR_OF_UL_TBF (c072000)']+df['NBR_OF_DL_TBF (c072005)']-df['UL_TBF_ESTABLISHMENT_FAILED (c072092)']-df['DL_TBF_ESTABLISHMENT_FAILED (c072093)']-df['UL_EGPRS_TBF_REL_DUE_NO_RESP (c072094)']-df['DL_EGPRS_TBF_REL_DUE_NO_RESP (c072095)']-df['UL_TBF_REL_DUE_TO_FLUSH (c072058)']-df['DL_TBF_REL_DUE_TO_FLUSH (c072059)']-df['UL_TBF_REL_DUE_TO_SUSPEND (c072060)']-df['DL_TBF_REL_DUE_TO_SUSPEND (c072061)']-df['UL_TBF_RELEASES_DUE_DTM (c072201)']-df['DL_TBF_RELEASES_DUE_DTM (c072202)']
                    df['cssr_num1']=df['SDCCH_DROP_CALL_AND_HO (c001191)']
                    df['cssr_den1']=df['SDCCH_ALLOC_FOR_VOICE_CALL (c002085)']
                    df['cssr_num2']=df['SUCC_TCH_SEIZ_CALL_ATTEMPT (c001193)']
                    df['cssr_den2']=df['TCH_REQUESTS_CALL_ATTEMPT (c001192)']
                    df['cssr_num3']=df['SDCCH_ASSIGN (c001007)']
                    df['cssr_den3']=df['SDCCH_SEIZ_ATT (c001000)']
                    df=df[['BTS name','PERIOD_START_TIME','BSC name','drop_rate_num','drop_rate_den','call_block_rate_num','call_block_rate_den',
                    'cell_avail_num','cell_avail_den','hosr_num','hosr_den','sdcch_avail_num','sdcch_avail_den',
                    'sdcch_block_rate_num','sdcch_block_rate_den','tch_avail_num','tch_avail_den','sdcch_drop_rate_num',
                    'sdcch_drop_rate_den','cs_traffic_erl','ps_traffic_mb','tbf_est_sr_num','tbf_est_sr_den','tbf_drop_rate_num',
                           'tbf_drop_rate_den','cssr_num1','cssr_den1','cssr_num2','cssr_den2','cssr_num3','cssr_den3']]
                    #df=df[df['BSC name']!='ASBSC22']
                    li.append(df)
                nsn = dd.concat(li, axis=0, ignore_index=True)

            #print('len of nsn:',len(nsn))
            an.append(nsn)
        print(time.time()-c1,'append finish')
        nsn = dd.concat(an, axis=0, ignore_index=True,sort=False)
        print(time.time()-c1,'concat1 finish')
        nsn=nsn.compute()
        print(time.time()-c1,'compute1 finish')

        an=[]
        nsn2=pd.DataFrame()
        for dirpath,dirname,filenames in os.walk(path2):
            #existing_files=pd.read_csv(os.path.join(path2,'files.txt'), sep=" ", header=None)
            all_files = glob.glob(dirpath + "/*.csv")
            #print('For dirpath:',dirpath,'len is:',len(all_files))
            if len(all_files)>0:
                li = []
                for filename in all_files:
                    #if os.path.basename(filename) in existing_files: continue
                    df = dd.read_csv(filename,sep=';',dtype=nokia_2G_2)
                    #df = df[df['BSC name'] != 'ASBSC22']
                    li.append(df)
                nsn2 = dd.concat(li, axis=0, ignore_index=True)

            an.append(nsn2)
        nsn2 = dd.concat(an, axis=0, ignore_index=True,sort=False)
        print(time.time()-c1,'concat2 finish')
        nsn2=nsn2.compute()
        nsn2['comb_thrp_num']=(np.nan_to_num(nsn2['new_llc_12_num']*8/nsn2['new_llc_12_den'])/10)*nsn2['LLC_BYTES_UL_EGPRS (c097021)']+(np.nan_to_num(nsn2['new_llc_13_num']*8/nsn2['new_llc_13_den'])/10)*nsn2['LLC_BYTES_DL_EGPRS (c097022)']+(np.nan_to_num(nsn2['new_llc_10_num']*8/nsn2['new_llc_10_den'])/10)*(nsn2['LLC_BYTES_UL (c097019)']-nsn2['LLC_BYTES_UL_EGPRS (c097021)'])+(np.nan_to_num(nsn2['new_llc_11_num']*8/nsn2['new_llc_11_den'])/10)*(nsn2['LLC_BYTES_DL (c097020)']-nsn2['LLC_BYTES_DL_EGPRS (c097022)'])
        nsn2['comb_thrp_den']=nsn2['LLC_BYTES_UL (c097019)']+nsn2['LLC_BYTES_DL (c097020)']
        print(time.time()-c1,'compute2 finish')
        print(time.time()-c1,'end of concatenation')
        nsn_agr=pd.merge(nsn,nsn2,left_on=['BTS name','BSC name','PERIOD_START_TIME'],right_on=['Segment Name','BSC name','PERIOD_START_TIME'],how='left')
        nsn_agr['Date']=pd.to_datetime(nsn_agr['PERIOD_START_TIME'], format='%m.%d.%Y %H:%M:%S')
        nsn_agr['Site_name']=nsn_agr['BTS name'].apply(lambda x: x[:8])
        nsn_agr['cell_avail_blck_den']=0
        nsn_agr['cell_avail_blck_den'][(nsn_agr['cell_avail_num']+nsn_agr['cell_avail_den'])==0]=3600
        nsn_agr['cell_avail_blck_num']=0
        nsn_agr['cell_avail_den']=nsn_agr['cell_avail_den']+nsn_agr['cell_avail_blck_den']
        nsn_agr.rename(columns={'BTS name':'Cell_name','BSC name':'BSC_name'},inplace=True)
        #nsn_agr.drop(labels=['Segment Name','PERIOD_START_TIME'],axis=1,inplace=True)
        nsn_agr['Vendor']='Nokia'
        nsn_agr['lookup']=nsn_agr['Site_name'].apply(lambda x: x[1:])
        #tracker=pd.read_csv(r'tracker.csv')
        nsn_agr=pd.merge(nsn_agr,tracker[['SITE_ID','Economical Region']],left_on='lookup',right_on='SITE_ID',how='left')
        nsn_agr.rename(columns={'Economical Region':'Region'},inplace=True)
        print(time.time()-c1,'end of merging')
        nsn_agr=nsn_agr[['Date','Vendor','BSC_name','Site_name','Cell_name','Region','call_block_rate_den',
               'call_block_rate_num', 'cell_avail_den', 'cell_avail_num','cell_avail_blck_num','cell_avail_blck_den',
               'comb_thrp_den', 'comb_thrp_num', 'cs_traffic_erl', 'cssr_den1',
               'cssr_den2', 'cssr_den3', 'cssr_num1', 'cssr_num2', 'cssr_num3',
               'drop_rate_den', 'drop_rate_num', 'hosr_den', 'hosr_num',
               'ps_traffic_mb', 'sdcch_avail_den', 'sdcch_avail_num',
               'sdcch_block_rate_den', 'sdcch_block_rate_num', 'sdcch_drop_rate_den',
               'sdcch_drop_rate_num', 'tbf_drop_rate_den', 'tbf_drop_rate_num',
               'tbf_est_sr_den', 'tbf_est_sr_num', 'tch_avail_den', 'tch_avail_num']]
        #nsn_agr.iloc[:,6:]=nsn_agr.iloc[:,6:].astype(np.float64)
        nsn_agr.drop_duplicates(keep='first',inplace=True)
        print(time.time()-c1,'new table created')
        nsn_bsc=nsn_agr.groupby(['Date','BSC_name','Vendor','Region']).sum()
        nsn_bsc.reset_index(inplace=True)
        print(time.time()-c1,'time to save')
        #print(file_name)
        #file_name = datetime.datetime.strftime(nsn_bsc['Date'].iloc[0], "%B_%Y") it was working
        #file_name2 = datetime.datetime.strftime(nsn_bsc['Date'].iloc[0], "%Y-%m-%d") it was working

        #print('file name is ', file_name, ' and file name2 is ', file_name2)
        for i in nsn_bsc['Date'].unique():
            file_name = datetime.datetime.strftime(datetime.datetime.utcfromtimestamp(i.astype(datetime.datetime) / 1e9),"%B_%Y")
            file_name2 = datetime.datetime.strftime(datetime.datetime.utcfromtimestamp(i.astype(datetime.datetime) / 1e9),"%Y-%m-%d")
            nsn_agr.loc[nsn_agr['Date']==i].to_hdf(r'/disk2/support_files/archive/ran/' + file_name2 + '.h5', '/twoG',append=True,
                           format='table', data_columns=['Date', 'BSC_name', 'Site_name', 'Cell_name', 'Vendor', 'Region'],
                           complevel=5,min_itemsize={'BSC_name': 10, 'Site_name': 20, 'Cell_name': 20, 'Vendor': 10, 'Region': 15})
            nsn_bsc.loc[nsn_bsc['Date']==i].to_hdf(r'/disk2/support_files/archive/ran/' + file_name2 + '.h5', '/twoG/bsc',
                           append=True,format='table', data_columns=['Date', 'BSC_name', 'Vendor', 'Region'], complevel=5,
                           min_itemsize={'BSC_name': 10, 'Vendor': 10, 'Region': 15})
        #nsn_agr.to_hdf(r'/disk2/support_files/archive/'+file_name+'.h5', '/twoG',
        #               append=True,
        #               format='table', data_columns=['Date', 'BSC_name', 'Site_name', 'Cell_name', 'Vendor', 'Region'],
        #               complevel=5,
        #               min_itemsize={'BSC_name': 10, 'Site_name': 20, 'Cell_name': 20, 'Vendor': 10, 'Region': 15})
        #nsn_bsc.to_hdf(r'/disk2/support_files/archive/'+file_name+'.h5', '/twoG/bsc',
        #               append=True,
        #               format='table', data_columns=['Date', 'BSC_name', 'Vendor', 'Region'], complevel=5,
        #               min_itemsize={'BSC_name': 10, 'Vendor': 10, 'Region': 15})
        nsn_bsc.to_hdf(r'/disk2/support_files/archive/combined_bsc.h5', '/twoG',
                       append=True,
                       format='table', data_columns=['Date', 'BSC_name', 'Vendor', 'Region'], complevel=5,
                       min_itemsize={'BSC_name': 10, 'Vendor': 10, 'Region': 15})

        print(time.time() - c1)
    except Exception as e:
        print(e)
