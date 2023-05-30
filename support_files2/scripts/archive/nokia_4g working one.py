import os
import pandas as pd
import glob,time
import dask.dataframe as dd
import numpy as np
import datetime

def run(path1,tracker):
    print('Nokia 4G aggregation begin...')
    c1=time.time()
    nokia_4G_1={'ACTIVE_TTI_DL (M8012C90)': 'float64',      'ACTIVE_TTI_UL (M8012C89)': 'float64',
       'ATT_INTER_ENB_HO (M8014C6)': 'float64',       'ATT_INTRA_ENB_HO (M8009C6)': 'float64',
       'EPC_EPS_BEARER_REL_REQ_DETACH (M8006C7)': 'float64',       'EPC_EPS_BEARER_REL_REQ_NORM (M8006C6)': 'float64',
       'EPC_EPS_BEARER_REL_REQ_OTH (M8006C9)': 'float64',       'EPC_EPS_BEARER_REL_REQ_RNL (M8006C8)': 'float64',
       'EPS_BEARER_SETUP_ATTEMPTS (M8006C0)': 'float64',       'EPS_BEARER_SETUP_COMPLETIONS (M8006C1)': 'float64',
       'ERAB_REL_ENB (M8006C254)': 'float64',       'ERAB_REL_ENB_ACT_NON_GBR (M8006C180)': 'float64',
       'ERAB_REL_ENB_RNL_INA (M8006C255)': 'float64',       'ERAB_REL_ENB_RNL_RED (M8006C258)': 'float64',
       'ERAB_REL_ENB_RNL_RRNA (M8006C260)': 'float64',       'ERAB_REL_EPC_PATH_SWITCH (M8006C277)': 'float64',
       'ERAB_REL_HO_PART (M8006C261)': 'float64',       'ERAB_REL_HO_PART_QCI1 (M8006C273)': 'float64',
       'ERAB_REL_HO_PART_QCI2 (M8006C291)': 'float64',       'ERAB_REL_TEMP_QCI1 (M8006C301)': 'float64',
       'HO_INTFREQ_ATT (M8021C0)': 'float64',       'HO_INTFREQ_SUCC (M8021C2)': 'float64',
       'INTER_ENB_S1_HO_ATT (M8014C18)': 'float64',       'INTER_ENB_S1_HO_SUCC (M8014C19)': 'float64',
       'ISYS_HO_ATT (M8016C21)': 'float64',       'ISYS_HO_SUCC (M8016C23)': 'float64',
       'PDCP_SDU_VOL_DL (M8012C20)': 'float64',       'PDCP_SDU_VOL_UL (M8012C19)': 'float64',
       'SIGN_CONN_ESTAB_ATT_DEL_TOL (M8013C34)': 'float64',       'SIGN_CONN_ESTAB_ATT_EMG (M8013C21)': 'float64',
       'SIGN_CONN_ESTAB_ATT_HIPRIO (M8013C31)': 'float64',       'SIGN_CONN_ESTAB_ATT_MO_D (M8013C19)': 'float64',
       'SIGN_CONN_ESTAB_ATT_MO_S (M8013C17)': 'float64',       'SIGN_CONN_ESTAB_ATT_MT (M8013C18)': 'float64',
       'SIGN_CONN_ESTAB_COMP (M8013C5)': 'float64',       'SUCC_INTER_ENB_HO (M8014C7)': 'float64',
       'SUCC_INTRA_ENB_HO (M8009C7)': 'float64',       'UE_CTX_MOD_ATT_CSFB (M8013C51)': 'float64',
       'UE_CTX_MOD_SUCC_CSFB (M8013C53)': 'float64',       'UE_CTX_SETUP_ATT_CSFB (M8013C46)': 'float64',
       'UE_CTX_SETUP_SUCC_CSFB (M8013C48)': 'float64','DENOM_CELL_AVAIL (M8020C6)': 'float64',
       'SAMPLES_CELL_AVAIL (M8020C3)': 'float64',  'SAMPLES_CELL_PLAN_UNAVAIL (M8020C4)': 'float64'}

    an=[]
    nsn=pd.DataFrame()
    #tracker=pd.read_excel(r'\\file-server\AZERCONNECT_LLC_OLD\Corporate Folder\CTO\Technology trackers\RNP\Azerconnect_RNP_tracker.xlsx',skiprows=[0])
    for dirpath,dirname,filenames in os.walk(path1):
        #existing_files=pd.read_csv(os.path.join(path1,'files.txt'), sep=" ", header=None)
        all_files = glob.glob(dirpath + "/*.csv")
        #print('For dirpath:',dirpath,'len is:',len(all_files))
        if len(all_files)>0:
            li = []
            for filename in all_files:
                #if os.path.basename(filename) in existing_files: continue
                df = dd.read_csv(filename,sep=';',dtype=nokia_4G_1, assume_missing=True)
                df['cell_avail_num']=df['SAMPLES_CELL_AVAIL (M8020C3)']*10
                df['cell_avail_den']=df['DENOM_CELL_AVAIL (M8020C6)']*10
                df['cell_avail_blck_den']=df['SAMPLES_CELL_PLAN_UNAVAIL (M8020C4)']*10
                df['cell_avail_blck_num']=0
                df['csfb_sr_num']=df['UE_CTX_MOD_SUCC_CSFB (M8013C53)']+df['UE_CTX_SETUP_SUCC_CSFB (M8013C48)']
                df['csfb_sr_den']=df['UE_CTX_MOD_ATT_CSFB (M8013C51)']+df['UE_CTX_SETUP_ATT_CSFB (M8013C46)']
                df['rrc_sr_num']=df['SIGN_CONN_ESTAB_COMP (M8013C5)']
                df['rrc_sr_den']=df['SIGN_CONN_ESTAB_ATT_MO_S (M8013C17)']+df['SIGN_CONN_ESTAB_ATT_MT (M8013C18)']+df['SIGN_CONN_ESTAB_ATT_MO_D (M8013C19)']+df['SIGN_CONN_ESTAB_ATT_DEL_TOL (M8013C34)']+df['SIGN_CONN_ESTAB_ATT_HIPRIO (M8013C31)']+df['SIGN_CONN_ESTAB_ATT_EMG (M8013C21)']
                df['rab_sr_num']=df['EPS_BEARER_SETUP_COMPLETIONS (M8006C1)']
                df['rab_sr_den']=df['EPS_BEARER_SETUP_ATTEMPTS (M8006C0)']
                df['dcr_num']=df['ERAB_REL_HO_PART (M8006C261)']+df['ERAB_REL_ENB (M8006C254)']-df['ERAB_REL_ENB_RNL_INA (M8006C255)']-df['ERAB_REL_ENB_RNL_RED (M8006C258)']-df['ERAB_REL_ENB_RNL_RRNA (M8006C260)']-df['ERAB_REL_TEMP_QCI1 (M8006C301)']
                df['dcr_den']=df['ERAB_REL_ENB (M8006C254)']+df['ERAB_REL_HO_PART (M8006C261)']+df['EPC_EPS_BEARER_REL_REQ_NORM (M8006C6)']+df['EPC_EPS_BEARER_REL_REQ_DETACH (M8006C7)']+df['EPC_EPS_BEARER_REL_REQ_RNL (M8006C8)']+df['EPC_EPS_BEARER_REL_REQ_OTH (M8006C9)']+df['ERAB_REL_EPC_PATH_SWITCH (M8006C277)']+df['ERAB_REL_TEMP_QCI1 (M8006C301)']
                df['dl_ps_traf']=df['PDCP_SDU_VOL_DL (M8012C20)']/(1024*1024)
                df['ul_ps_traf']=df['PDCP_SDU_VOL_UL (M8012C19)']/(1024*1024)
                df['dl_thrp_num']=df['PDCP_SDU_VOL_DL (M8012C20)']*8
                df['dl_thrp_den']=df['ACTIVE_TTI_DL (M8012C90)']
                df['ul_thrp_num']=df['PDCP_SDU_VOL_UL (M8012C19)']*8
                df['ul_thrp_den']=df['ACTIVE_TTI_UL (M8012C89)']
                df['intra_freq_ho_num']=df['SUCC_INTRA_ENB_HO (M8009C7)']+df['SUCC_INTER_ENB_HO (M8014C7)']+df['INTER_ENB_S1_HO_SUCC (M8014C19)']-df['HO_INTFREQ_SUCC (M8021C2)']
                df['intra_freq_ho_den']=df['ATT_INTRA_ENB_HO (M8009C6)']+df['ATT_INTER_ENB_HO (M8014C6)']+df['INTER_ENB_S1_HO_ATT (M8014C18)']-df['HO_INTFREQ_ATT (M8021C0)']
                df['irat_ho_num']=df['ISYS_HO_SUCC (M8016C23)']
                df['irat_ho_den']=df['ISYS_HO_ATT (M8016C21)']
                                  
                df=df[['LNCEL name','PERIOD_START_TIME','cell_avail_num','cell_avail_den',
                       'cell_avail_blck_num','cell_avail_blck_den','csfb_sr_num','csfb_sr_den','rrc_sr_num','rrc_sr_den','rab_sr_num',
                       'rab_sr_den','dcr_num','dcr_den','dl_ps_traf','ul_ps_traf','dl_thrp_num','dl_thrp_den',
                       'ul_thrp_num','ul_thrp_den','intra_freq_ho_num','intra_freq_ho_den','irat_ho_num','irat_ho_den']]
                li.append(df)
            nsn = dd.concat(li, axis=0, ignore_index=True)
        
        #print('len of nsn:',len(nsn))
        an.append(nsn)
    print(time.time()-c1,'append finish')
    nsn = dd.concat(an, axis=0, ignore_index=True,sort=False)
    print(time.time()-c1,'concat finish')
    nsn=nsn.compute()
    print(time.time()-c1,'compute finish')

    nsn['Site_name']=nsn['LNCEL name'].apply(lambda x: x[:8])
    nsn['Date']=pd.to_datetime(nsn['PERIOD_START_TIME'], format='%m.%d.%Y %H:%M:%S')                                 
    nsn['lookup']=nsn['Site_name'].apply(lambda x: x[1:])
    nsn=pd.merge(nsn,tracker[['SITE_ID','Economical Region']],left_on='lookup',right_on='SITE_ID',how='left')
    nsn.rename(columns={'LNCEL name':'Cell_name','Economical Region':'Region'},inplace=True)   
    nsn['Vendor']='Nokia'

    nsn_agr=nsn[['Date','Vendor','Site_name','Cell_name','Region','cell_avail_num','cell_avail_den',
                 'cell_avail_blck_num','cell_avail_blck_den','csfb_sr_num','csfb_sr_den','rrc_sr_num','rrc_sr_den','rab_sr_num',
                 'rab_sr_den','dcr_num','dcr_den','dl_ps_traf','ul_ps_traf','dl_thrp_num','dl_thrp_den',
                 'ul_thrp_num','ul_thrp_den','intra_freq_ho_num','intra_freq_ho_den','irat_ho_num','irat_ho_den']]
    nsn_agr.iloc[:,5:]=nsn_agr.iloc[:,5:].astype(np.float64)
    nsn_agr.drop_duplicates(keep='first',inplace=True)
    print(time.time()-c1,'new table created')
    nsn_rnc=nsn_agr.groupby(['Date','Vendor','Region']).sum()
    nsn_rnc.reset_index(inplace=True)
    print(time.time()-c1,'time to save')
    #file_name = datetime.datetime.strftime(nsn_rnc['Date'].iloc[0], "%B_%Y") it was working
    #file_name2 = datetime.datetime.strftime(nsn_rnc['Date'].iloc[0], "%Y-%m-%d") it was working
    #print('file name is ', file_name, ' and file name2 is ', file_name2)
    for i in nsn_rnc['Date'].unique():
        file_name = datetime.datetime.strftime(datetime.datetime.utcfromtimestamp(i.astype(datetime.datetime)/1e9), "%B_%Y")
        file_name2 = datetime.datetime.strftime(datetime.datetime.utcfromtimestamp(i.astype(datetime.datetime)/1e9), "%Y-%m-%d")
        nsn_agr.loc[nsn_agr['Date']==i].to_hdf(r'/disk2/support_files/archive/ran/' + file_name2 + '.h5', '/fourG', append=True,
                       format='table', data_columns=['Date', 'Site_name', 'Cell_name', 'Vendor', 'Region'], complevel=5,
                       min_itemsize={'Site_name': 20, 'Cell_name': 20, 'Vendor': 10, 'Region': 15})
        nsn_rnc.loc[nsn_rnc['Date']==i].to_hdf(r'/disk2/support_files/archive/ran/' + file_name2 + '.h5', '/fourG/bsc', append=True,
                       format='table', data_columns=['Date', 'Vendor', 'Region'], complevel=5,
                       min_itemsize={'Vendor': 10, 'Region': 15})
    #nsn_agr.to_hdf(r'/disk2/support_files/archive/'+file_name+'.h5','/fourG',append=True,
    #           format='table',data_columns=['Date','Site_name','Cell_name','Vendor','Region'],complevel=5,
    #                min_itemsize={'Site_name':20,'Cell_name':20,'Vendor':10,'Region':15})
    #nsn_rnc.to_hdf(r'/disk2/support_files/archive/'+file_name+'.h5','/fourG/bsc',append=True,
    #           format='table',data_columns=['Date','Vendor','Region'],complevel=5,
    #                min_itemsize={'Vendor':10,'Region':15})
    nsn_rnc.to_hdf(r'/disk2/support_files/archive/combined_bsc.h5', '/fourG',append=True,
                   format='table', data_columns=['Date', 'Vendor', 'Region'], complevel=5,
                   min_itemsize={'Vendor': 10, 'Region': 15})


    print(time.time()-c1)
