import os
import pandas as pd
import glob,time
#import dask.dataframe as dd
import numpy as np
import datetime
import zipfile
try:
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
           'SAMPLES_CELL_AVAIL (M8020C3)': 'float64',  'SAMPLES_CELL_PLAN_UNAVAIL (M8020C4)': 'float64',
           'MAX_RTWP (M8005C314)':'float64','ERAB_ADD_SETUP_ATT_QCI1 (M8006C197)':'float64','ERAB_INI_SETUP_ATT_QCI1 (M8006C188)':'float64',
            'ERAB_INI_SETUP_SUCC_QCI1 (M8006C206)':'float64','ERAB_ADD_SETUP_SUCC_QCI1 (M8006C215)':'float64',
            'EPC_EPS_BEAR_REL_REQ_N_QCI1 (M8006C89)':'float64','EPC_EPS_BEAR_REL_REQ_D_QCI1 (M8006C98)':'float64',
            'EPC_EPS_BEAR_REL_REQ_R_QCI1 (M8006C107)':'float64','EPC_EPS_BEAR_REL_REQ_O_QCI1 (M8006C116)':'float64',
            'ERAB_REL_ENB_QCI1 (M8006C266)':'float64','ERAB_REL_EPC_PATH_SWITCH_QCI1 (M8006C278)':'float64',
            'ERAB_REL_ENB_RNL_INA_QCI1 (M8006C267)':'float64','ERAB_REL_ENB_RNL_RED_QCI1 (M8006C270)':'float64',
            'IP_TPUT_VOL_UL_QCI_1 (M8012C91)':'float64','IP_TPUT_VOL_DL_QCI_1 (M8012C117)':'float64','ERAB_IN_SESSION_TIME_QCI1 (M8006C181)':'float64',
            'ERAB_IN_SESSION_TIME_QCI2 (M8006C182)':'float64', 'ERAB_IN_SESSION_TIME_QCI3 (M8006C183)':'float64', 'ERAB_IN_SESSION_TIME_QCI4 (M8006C184)':'float64',
            'ERAB_IN_SESSION_TIME_NON_GBR (M8006C185)':'float64','ERAB_REL_ENB_RNL_RRNA_QCI1 (M8006C272)':'float64',
            'ERAB_REL_ENB_INI_S1_G_R_QCI1 (M8006C316)':'float64','ERAB_REL_MME_INI_S1_G_R_QCI1 (M8006C317)':'float64',
            'ERAB_REL_MME_INI_S1_P_R_QCI1 (M8006C319)':'float64','ERAB_REL_ENB_INI_S1_P_R_QCI1 (M8006C318)':'float64',
            'ERAB_REL_S1_OUTAGE_QCI1 (M8006C320)':'float64','ERAB_REL_SUCC_HO_UTRAN_QCI1 (M8006C304)':'float64','ERAB_REL_SUCC_HO_GERAN_QCI1 (M8006C307)':'float64',
            'SIGN_CONN_ESTAB_FAIL_MAXRRC (M8013C68)':'float64'
}

        an=[]
        nsn=pd.DataFrame()
        #tracker=pd.read_excel(r'\\file-server\AZERCONNECT_LLC_OLD\Corporate Folder\CTO\Technology trackers\RNP\Azerconnect_RNP_tracker.xlsx',skiprows=[0])
        path1='/home/ismayil/flask_dash/data/nokia/pool'
        for dirpath,dirname,filenames in os.walk(path1):
            #existing_files=pd.read_csv(os.path.join(path1,'files.txt'), sep=" ", header=None)
            #all_files = glob.glob(dirpath + "/*.csv") # change to previous value
            all_files = glob.glob(dirpath + "/*.zip")
            #print('For dirpath:',dirpath,'len is:',len(all_files))
            if len(all_files)>0:
                li = []
                if os.path.isfile(os.path.join('/home/ismayil/flask_dash/data', 'files.csv')):
                        existing_files = pd.read_csv(os.path.join('/home/ismayil/flask_dash/data', 'files.csv'), header=None)
                else: existing_files=pd.DataFrame()
                for filename in all_files:    
                    if (filename.replace('/home/ismayil/flask_dash/data/nokia/pool/','4G_') in existing_files.values) or ('CMG' in filename) : continue
                    files2=zipfile.ZipFile(filename,'r')
                    if ('01_37_00' in filename): continue
                    df=pd.read_csv(files2.open(files2.namelist()[4]),sep=';')
                    #if ('_37_00__' not in filename):
                    #    df = df[df['PERIOD_START_TIME'].str.contains('00:00:00')]
                    #if os.path.basename(filename) in existing_files: continue
                    #df = dd.read_csv(filename,sep=';',dtype=nokia_4G_1, assume_missing=True) # change to previous value
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
                    df['volte_sr_num'] = (df['ERAB_INI_SETUP_SUCC_QCI1 (M8006C206)']+df['ERAB_ADD_SETUP_SUCC_QCI1 (M8006C215)']-df['ERAB_REL_TEMP_QCI1 (M8006C301)'])
                    df['volte_sr_den'] = (df['ERAB_INI_SETUP_ATT_QCI1 (M8006C188)']+df['ERAB_ADD_SETUP_ATT_QCI1 (M8006C197)'])
                    df['volte_dr_num'] = df['ERAB_REL_HO_PART_QCI1 (M8006C273)']+df['ERAB_REL_ENB_QCI1 (M8006C266)']-df['ERAB_REL_ENB_RNL_INA_QCI1 (M8006C267)']-df['ERAB_REL_ENB_RNL_RED_QCI1 (M8006C270)']-df['ERAB_REL_ENB_RNL_RRNA_QCI1 (M8006C272)']-df['ERAB_REL_TEMP_QCI1 (M8006C301)']+df['ERAB_REL_ENB_INI_S1_G_R_QCI1 (M8006C316)']+df['ERAB_REL_MME_INI_S1_G_R_QCI1 (M8006C317)']+df['ERAB_REL_ENB_INI_S1_P_R_QCI1 (M8006C318)']+df['ERAB_REL_MME_INI_S1_P_R_QCI1 (M8006C319)']+df['ERAB_REL_S1_OUTAGE_QCI1 (M8006C320)']
                    df['volte_dr_den'] = df['ERAB_REL_ENB_QCI1 (M8006C266)']+df['ERAB_REL_HO_PART_QCI1 (M8006C273)']+df['EPC_EPS_BEAR_REL_REQ_N_QCI1 (M8006C89)']+df['EPC_EPS_BEAR_REL_REQ_D_QCI1 (M8006C98)']+df['EPC_EPS_BEAR_REL_REQ_R_QCI1 (M8006C107)']+df['EPC_EPS_BEAR_REL_REQ_O_QCI1 (M8006C116)']+df['ERAB_REL_EPC_PATH_SWITCH_QCI1 (M8006C278)']-df['ERAB_REL_TEMP_QCI1 (M8006C301)']+df['ERAB_REL_SUCC_HO_UTRAN_QCI1 (M8006C304)']+df['ERAB_REL_SUCC_HO_GERAN_QCI1 (M8006C307)']+df['ERAB_REL_ENB_INI_S1_G_R_QCI1 (M8006C316)']+df['ERAB_REL_MME_INI_S1_G_R_QCI1 (M8006C317)']+df['ERAB_REL_ENB_INI_S1_P_R_QCI1 (M8006C318)']+df['ERAB_REL_MME_INI_S1_P_R_QCI1 (M8006C319)']+df['ERAB_REL_S1_OUTAGE_QCI1 (M8006C320)']
                    df['volte_dl_ps_traf'] = df['IP_TPUT_VOL_DL_QCI_1 (M8012C117)']/8/1024/1024
                    df['volte_ul_ps_traf'] = df['IP_TPUT_VOL_UL_QCI_1 (M8012C91)']/8/1024/1024
                    df['volte_cs_traf'] = df['ERAB_IN_SESSION_TIME_QCI1 (M8006C181)']/60/60
                    df['rtwp']=df['MAX_RTWP (M8005C314)']
                    df['volte_srvcc_e2w_num'] = df['ISYS_HO_UTRAN_SRVCC_SUCC (M8016C30)']
                    df['volte_srvcc_e2w_den'] = df['ISYS_HO_UTRAN_SRVCC_ATT (M8016C29)']
                    df['Voice_VQI_DL_Accept_Times'] = 0
                    df['Voice_VQI_DL_Bad_Times'] = 0
                    df['Voice_VQI_DL_Excellent_Times'] = 0
                    df['Voice_VQI_DL_Good_Times'] = 0
                    df['Voice_VQI_DL_Poor_Times'] = 0
                    df['Voice_VQI_DL_TotalValue'] = 0
                    df['Voice_VQI_UL_Accept_Times'] = 0
                    df['Voice_VQI_UL_Bad_Times'] = 0
                    df['Voice_VQI_UL_Excellent_Times'] = 0
                    df['Voice_VQI_UL_Good_Times'] = 0
                    df['Voice_VQI_UL_Poor_Times'] = 0
                    df['Voice_VQI_UL_TotalValue'] = 0
                    df['Voice_DL_Silent_Num'] = 0
                    df['Voice_UL_Silent_Num'] = 0
                    df=df[['LNCEL name','PERIOD_START_TIME','cell_avail_num','cell_avail_den',
                           'cell_avail_blck_num','cell_avail_blck_den','csfb_sr_num','csfb_sr_den','rrc_sr_num','rrc_sr_den','rab_sr_num',
                           'rab_sr_den','dcr_num','dcr_den','dl_ps_traf','ul_ps_traf','dl_thrp_num','dl_thrp_den',
                           'ul_thrp_num','ul_thrp_den','intra_freq_ho_num','intra_freq_ho_den','irat_ho_num','irat_ho_den',
                           'volte_sr_num','volte_sr_den','volte_dr_num','volte_dr_den','volte_dl_ps_traf','volte_ul_ps_traf','volte_cs_traf','rtwp',
                           'E-UTRAN Avg PRB usage per TTI DL','Avg PRB usage per TTI UL','PDCP_SDU_LOSS_UL_FNA (M8026C254)',
                           'PDCP_SDU_LOSS_DL_FNA (M8026C259)','PDCP_SDU_DL_DISC (M8001C155)','UL_RLC_PDU_DISC (M8001C145)',
                           'volte_srvcc_e2w_num','volte_srvcc_e2w_den','Voice_VQI_DL_Accept_Times','Voice_VQI_DL_Bad_Times','Voice_VQI_DL_Excellent_Times',
                  'Voice_VQI_DL_Good_Times','Voice_VQI_DL_Poor_Times','Voice_VQI_DL_TotalValue','Voice_VQI_UL_Accept_Times',
                  'Voice_VQI_UL_Bad_Times','Voice_VQI_UL_Excellent_Times','Voice_VQI_UL_Good_Times','Voice_VQI_UL_Poor_Times',
                  'Voice_VQI_UL_TotalValue','Voice_DL_Silent_Num','Voice_UL_Silent_Num','SIGN_CONN_ESTAB_FAIL_MAXRRC (M8013C68)']]
                    li.append(df)

                    print(time.time()-c1,'utilization part begin')
                    try:
                        df['Site_name']=df['LNCEL name'].apply(lambda x: x[:8])
                        df['Date']=pd.to_datetime(df['PERIOD_START_TIME'], format='%m.%d.%Y %H:%M:%S')
                        df.rename(columns={'LNCEL name':'Cell_name','Economical Region':'Region'},inplace=True)
                        df['Vendor']='Nokia'                 
                        util=df[['Date','Vendor','Site_name','Cell_name','E-UTRAN Avg PRB usage per TTI DL','Avg PRB usage per TTI UL','PDCP_SDU_LOSS_UL_FNA (M8026C254)',
                                        'PDCP_SDU_LOSS_DL_FNA (M8026C259)','PDCP_SDU_DL_DISC (M8001C155)','UL_RLC_PDU_DISC (M8001C145)']].copy()
                        util.rename(columns={'E-UTRAN Avg PRB usage per TTI DL':'DL_PRB_Utilzation','Avg PRB usage per TTI UL':'UL_PRB_Utilzation'},inplace=True)
                        util[['IPPM_lost','IPPM_tx','IPPM_rx']]=np.NAN
                        util=util[['Date','Vendor','Site_name','Cell_name','DL_PRB_Utilzation','UL_PRB_Utilzation','PDCP_SDU_LOSS_UL_FNA (M8026C254)',
                                        'PDCP_SDU_LOSS_DL_FNA (M8026C259)','PDCP_SDU_DL_DISC (M8001C155)','UL_RLC_PDU_DISC (M8001C145)']]
                        util.columns=['Date','Vendor','Site_name','Cell_name','DL_PRB_Utilzation','UL_PRB_Utilzation','PDCP_SDU_LOSS_UL_FNA',
                                        'PDCP_SDU_LOSS_DL_FNA','PDCP_SDU_DL_DISC','UL_RLC_PDU_DISC']
                        
                        util.iloc[:,4:]=util.iloc[:,4:].astype(float)
                        for i in util['Date'].unique():
                            file_name = datetime.datetime.strftime(datetime.datetime.utcfromtimestamp(i.astype(datetime.datetime)/1e9), "%B_%Y")
                            file_name2 = datetime.datetime.strftime(datetime.datetime.utcfromtimestamp(i.astype(datetime.datetime)/1e9), "%Y-%m-%d")
                            try:
                                a=pd.read_hdf('/disk2/support_files/archive/util/'+file_name2 +'_util_hourly.h5', '/cell_4g', where='Vendor="Nokia" and Date=i')
                                if len(a)>0:
                                    print(i,'not appended')
                                    continue
                            except:
                                1
                            print(i)
                            util.loc[util['Date']==i].to_hdf('/disk2/support_files/archive/util/'+file_name2+'_util_hourly.h5', '/cell_4g', append=True, format='table',
                                            data_columns=['Date','Vendor','Site_name','Cell_name'], complevel=5, 
                                            min_itemsize={'Cell_name': 30,'Vendor':20,'Site_name':30})
                        util_site=df.groupby(['Date','Site_name']).sum()['SIGN_CONN_ESTAB_FAIL_MAXRRC (M8013C68)'].reset_index()
                        util_site.rename(columns={'SIGN_CONN_ESTAB_FAIL_MAXRRC (M8013C68)':'Fails'},inplace=True)
                        util_site['IPPM_lost']=0
                        util_site['IPPM_tx']=0
                        util_site['IPPM_rx']=0
                        util_site['RRC_User_License']=0
                        util_site['Fail_reason']='SIGN_CONN_ESTAB_FAIL_MAXRRC'
                        util_site['Fails']=util_site['Fails'].astype(int)
                        util_site[['IPPM_lost','IPPM_rx','IPPM_tx','RRC_User_License']]=util_site[['IPPM_lost','IPPM_rx','IPPM_tx','RRC_User_License']].astype(float)
                        for i in util_site['Date'].unique():
                            file_name2 = datetime.datetime.strftime(datetime.datetime.utcfromtimestamp(i.astype(datetime.datetime)/1e9), "%Y-%m-%d")
                            util_site.loc[util_site['Date']==i].to_hdf('/disk2/support_files/archive/util/'+file_name2+'_util_hourly.h5', '/site_4g', 
                                    append=True, format='table',data_columns=['Date','Site_name','Fail_reason'], complevel=5, 
                                            min_itemsize={'Site_name':30,'RRC_User_License':30,'Fail_reason':30,'IPPM_lost':10,'IPPM_tx':10,'IPPM_rx':10,'Fails':10})
                        print('Util site finished')
                        needed = datetime.datetime.strptime(datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(hours=1), '%d.%m.%y %H:00'), '%d.%m.%y %H:00')
                        util_site=util_site[(util_site['Date']==needed) & (util_site['Fail_reason']=='SIGN_CONN_ESTAB_FAIL_MAXRRC') & (util_site['Fails']>0)]
                        if len(util_site)>0:
                            from send_notification import send_mail
                            attachment=False
                            try:
                                import alarms
                                df2,df3,df8,df9=alarms.check_alarm(df)
                            except Exception as e:
                                print(e)
                                df2=pd.DataFrame()
                                1
                            df=util_site
                            if len(df8)>0:
                                df['lookup']=df['Site_name'].apply(lambda x: x[1:8])
                                df=df.merge(df8,left_on='lookup',right_on='Site',how='left').drop(columns='Site')
                            else: df['Down_tech']='Not down'
                            if len(df9)>0:
                                df=df.merge(df9,left_on='Site_name',right_on='origin_site',how='left').rename(columns={'Site':'Down_neighbors'})
                            else:
                                df['Down_neighbors']=0
                            if len(df2)>0:
                                df3.to_csv('/home/ismayil/flask_dash/support_files/down_info.csv')
                                df=df.merge(df2,left_on='Site_name',right_on='origin_site',how='left').rename(columns={'Unique down':'Down_alarm_count_on_neighbors'})
                                attachment=True
                            else:
                                df['Down_alarm_count_on_neighbors']=0
                            df['Down_neighbors']=df['Down_neighbors'].fillna(0).astype(int)
                            df['Down_alarm_count_on_neighbors']=df['Down_alarm_count_on_neighbors'].fillna(0).astype(int)
                            df.drop_duplicates(inplace=True)
                            send_mail(['AZRC_RNO@azerconnect.az','RAN_Capacity_Management_Unit@azerconnect.az','AZRC_RNP@azerconnect.az',
                        'AZRC_QA&R@azerconnect.az','raming@azerconnect.az','talehm@azerconnect.az','ismayilm@azerconnect.az'],
                                        'LTE SIGN_CONN_ESTAB_FAIL_MAXRRC','Fails increased on below sites for the last hour',
                                        util_site[['Site_name','Fail_reason','Fails','Down_tech','Down_neighbors','Down_alarm_count_on_neighbors']].\
                                            sort_values(by='Fails',ascending=False),datetime.datetime.strftime(needed,'%d.%m.%Y %H:%M'),attachment)
                    except Exception as e: 
                        print(e)
                        pass
                    print(time.time()-c1,'4G utilization finished')
                #nsn = dd.concat(li, axis=0, ignore_index=True) # change to previous value
                nsn = pd.concat(li, axis=0, ignore_index=True)

            #print('len of nsn:',len(nsn))
            an.append(nsn)
        print(time.time()-c1,'append finish')
        #nsn = dd.concat(an, axis=0, ignore_index=True,sort=False) # change to previous value
        nsn = pd.concat(an, axis=0, ignore_index=True, sort=False)
        print(time.time()-c1,'concat finish')
        #nsn=nsn.compute() # change to previous value
        print(time.time()-c1,'compute finish')

        try:
            nsn['Site_name']=nsn['Cell_name'].apply(lambda x: x[:8])
            nsn['Date']=pd.to_datetime(nsn['PERIOD_START_TIME'], format='%m.%d.%Y %H:%M:%S')
            nsn['lookup']=nsn['Site_name'].apply(lambda x: x[1:])
            nsn=pd.merge(nsn,tracker[['SITE_ID','Economical Region']],left_on='lookup',right_on='SITE_ID',how='left')
            nsn.rename(columns={'LNCEL name':'Cell_name','Economical Region':'Region'},inplace=True)
            nsn['Vendor']='Nokia'

            nsn_agr=nsn[['Date','Vendor','Site_name','Cell_name','Region','cell_avail_num','cell_avail_den',
                        'cell_avail_blck_num','cell_avail_blck_den','csfb_sr_num','csfb_sr_den','rrc_sr_num','rrc_sr_den','rab_sr_num',
                        'rab_sr_den','dcr_num','dcr_den','dl_ps_traf','ul_ps_traf','dl_thrp_num','dl_thrp_den',
                        'ul_thrp_num','ul_thrp_den','intra_freq_ho_num','intra_freq_ho_den','irat_ho_num','irat_ho_den',
                        'volte_sr_num','volte_sr_den','volte_dr_num','volte_dr_den','volte_dl_ps_traf','volte_ul_ps_traf','volte_cs_traf','rtwp',
                        'volte_srvcc_e2w_num','volte_srvcc_e2w_den','Voice_VQI_DL_Accept_Times','Voice_VQI_DL_Bad_Times','Voice_VQI_DL_Excellent_Times',
                    'Voice_VQI_DL_Good_Times','Voice_VQI_DL_Poor_Times','Voice_VQI_DL_TotalValue','Voice_VQI_UL_Accept_Times',
                    'Voice_VQI_UL_Bad_Times','Voice_VQI_UL_Excellent_Times','Voice_VQI_UL_Good_Times','Voice_VQI_UL_Poor_Times',
                    'Voice_VQI_UL_TotalValue','Voice_DL_Silent_Num','Voice_UL_Silent_Num']]
            keys=['cell_avail_num','cell_avail_den',
                        'cell_avail_blck_num','cell_avail_blck_den','csfb_sr_num','csfb_sr_den','rrc_sr_num','rrc_sr_den','rab_sr_num',
                        'rab_sr_den','dcr_num','dcr_den','dl_ps_traf','ul_ps_traf','dl_thrp_num','dl_thrp_den',
                        'ul_thrp_num','ul_thrp_den','intra_freq_ho_num','intra_freq_ho_den','irat_ho_num','irat_ho_den',
                        'volte_sr_num','volte_sr_den','volte_dr_num','volte_dr_den','volte_dl_ps_traf','volte_ul_ps_traf','volte_cs_traf',
                        'volte_srvcc_e2w_num','volte_srvcc_e2w_den','Voice_VQI_DL_Accept_Times','Voice_VQI_DL_Bad_Times','Voice_VQI_DL_Excellent_Times',
                    'Voice_VQI_DL_Good_Times','Voice_VQI_DL_Poor_Times','Voice_VQI_DL_TotalValue','Voice_VQI_UL_Accept_Times',
                    'Voice_VQI_UL_Bad_Times','Voice_VQI_UL_Excellent_Times','Voice_VQI_UL_Good_Times','Voice_VQI_UL_Poor_Times',
                    'Voice_VQI_UL_TotalValue','Voice_DL_Silent_Num','Voice_UL_Silent_Num']
            agg_dict=dict.fromkeys(keys, 'sum')
            agg_dict['rtwp']='mean'
            nsn_agr.iloc[:,5:]=nsn_agr.iloc[:,5:].astype(np.float64)
            nsn_agr.drop_duplicates(keep='first',inplace=True)
            print(time.time()-c1,'new table created')
            nsn_rnc=nsn_agr.groupby(['Date','Vendor','Region']).agg(agg_dict)
            nsn_rnc.reset_index(inplace=True)
            print(time.time()-c1,'time to save')
        except Exception as e:
            print(e)
            print(nsn.columns)
            1
        #file_name = datetime.datetime.strftime(nsn_rnc['Date'].iloc[0], "%B_%Y") it was working
        #file_name2 = datetime.datetime.strftime(nsn_rnc['Date'].iloc[0], "%Y-%m-%d") it was working
        #print('file name is ', file_name, ' and file name2 is ', file_name2)
        to_filter=[]
        for i in nsn_rnc['Date'].unique():
            file_name = datetime.datetime.strftime(datetime.datetime.utcfromtimestamp(i.astype(datetime.datetime)/1e9), "%B_%Y")
            file_name2 = datetime.datetime.strftime(datetime.datetime.utcfromtimestamp(i.astype(datetime.datetime)/1e9), "%Y-%m-%d")
            try:
                a=pd.read_hdf('/disk2/support_files/archive/ran/'+file_name2 +'.h5', 'fourG/bsc', where='Vendor="Nokia" and Date=i')
            except:
                a=pd.DataFrame()
                print('error happened during saving..., but passed with try-except clause')
                1
            if len(a)>0:
                print(i,'not appended')
                continue
            else: to_filter.append(i)
            print(i)
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
        
        nsn_rnc[['Date','Vendor','Region','volte_sr_num', 'volte_sr_den', 'volte_dr_num', 'volte_dr_den', 'volte_dl_ps_traf','volte_ul_ps_traf',
         'volte_cs_traf','volte_srvcc_e2w_num','volte_srvcc_e2w_den','Voice_VQI_DL_Accept_Times','Voice_VQI_DL_Bad_Times','Voice_VQI_DL_Excellent_Times',
                  'Voice_VQI_DL_Good_Times','Voice_VQI_DL_Poor_Times','Voice_VQI_DL_TotalValue','Voice_VQI_UL_Accept_Times',
                  'Voice_VQI_UL_Bad_Times','Voice_VQI_UL_Excellent_Times','Voice_VQI_UL_Good_Times','Voice_VQI_UL_Poor_Times',
                  'Voice_VQI_UL_TotalValue','Voice_DL_Silent_Num','Voice_UL_Silent_Num']].loc[nsn_rnc['Date'].isin(to_filter)].to_hdf(
            r'/disk2/support_files/archive/combined_bsc.h5', '/fourGn', append=True, format='table', data_columns=['Date', 'Vendor', 'Region'], complevel=5,
            min_itemsize={'Vendor': 10, 'Region': 15})
        
        p=datetime.datetime.strftime(datetime.datetime.now(),'%d.%m.%Y_%H')
        #nsn_rnc.to_csv('/mnt/raw_counters/Corporate Folder/CTO/Technology Governance and Central Support/RAN QA/Daily/Radio_Main_KPIs/4G_nsn_'+p+'.csv',
        #                        index=False)

        nsn_rnc.drop(columns=['volte_sr_num','volte_sr_den','volte_dr_num','volte_dr_den','volte_dl_ps_traf'
        ,'volte_ul_ps_traf','volte_cs_traf','rtwp',
        'volte_srvcc_e2w_num','volte_srvcc_e2w_den','Voice_VQI_DL_Accept_Times','Voice_VQI_DL_Bad_Times','Voice_VQI_DL_Excellent_Times',
                  'Voice_VQI_DL_Good_Times','Voice_VQI_DL_Poor_Times','Voice_VQI_DL_TotalValue','Voice_VQI_UL_Accept_Times',
                  'Voice_VQI_UL_Bad_Times','Voice_VQI_UL_Excellent_Times','Voice_VQI_UL_Good_Times','Voice_VQI_UL_Poor_Times',
                  'Voice_VQI_UL_TotalValue','Voice_DL_Silent_Num','Voice_UL_Silent_Num']).loc[nsn_rnc['Date'].isin(to_filter)].to_hdf(r'/disk2/support_files/archive/combined_bsc.h5', '/fourG',append=True,
                       format='table', data_columns=['Date', 'Vendor', 'Region'], complevel=5,
                       min_itemsize={'Vendor': 10, 'Region': 15})
        
        try:
            df4=nsn_rnc[['Date','Region','cell_avail_den', 'cell_avail_num','dl_ps_traf','ul_ps_traf','cell_avail_blck_num',
        'cell_avail_blck_den','rrc_sr_num','rrc_sr_den','rab_sr_num','rab_sr_den','csfb_sr_num','csfb_sr_den','dcr_num','dcr_den',
        'dl_thrp_num','dl_thrp_den','volte_dl_ps_traf','volte_ul_ps_traf','volte_sr_num','volte_sr_den','volte_dr_num','volte_dr_den','volte_cs_traf',
        'Voice_DL_Silent_Num','Voice_UL_Silent_Num','volte_srvcc_e2w_num','volte_srvcc_e2w_den',
                    'Voice_VQI_DL_Accept_Times','Voice_VQI_DL_Bad_Times','Voice_VQI_DL_Excellent_Times',
                    'Voice_VQI_DL_Good_Times','Voice_VQI_DL_Poor_Times','Voice_VQI_UL_Accept_Times',
                        'Voice_VQI_UL_Bad_Times','Voice_VQI_UL_Excellent_Times','Voice_VQI_UL_Good_Times','Voice_VQI_UL_Poor_Times']]           
            df4=df4[['Date','Region','cell_avail_den', 'cell_avail_num','dl_ps_traf','ul_ps_traf','cell_avail_blck_num',
                'cell_avail_blck_den','rrc_sr_num','rrc_sr_den','rab_sr_num','rab_sr_den','csfb_sr_num','csfb_sr_den','dcr_num','dcr_den',
                'dl_thrp_num','dl_thrp_den','volte_dl_ps_traf','volte_ul_ps_traf','volte_sr_num','volte_sr_den','volte_dr_num','volte_dr_den','volte_cs_traf',
                'Voice_DL_Silent_Num','Voice_UL_Silent_Num','volte_srvcc_e2w_num','volte_srvcc_e2w_den',
                        'Voice_VQI_DL_Accept_Times','Voice_VQI_DL_Bad_Times','Voice_VQI_DL_Excellent_Times',
                        'Voice_VQI_DL_Good_Times','Voice_VQI_DL_Poor_Times','Voice_VQI_UL_Accept_Times',
                        'Voice_VQI_UL_Bad_Times','Voice_VQI_UL_Excellent_Times','Voice_VQI_UL_Good_Times','Voice_VQI_UL_Poor_Times']]
            df4.columns=[*df4.columns[:2],*df4.columns[2:].map(lambda x: '4G_'+x)]
            df4=df4.groupby(['Date','Region'],as_index=False).sum()

            
            df4['Day']=df4['Date'].dt.date
            df4['Month']=df4['Date'].dt.month
            df4['Year']=df4['Date'].dt.year.astype(str)
            df4.dropna(inplace=True)
            df4.to_csv('/disk2/support_files/atoti/bi/4G_nokia.csv',index=False) #' + str(datetime.datetime.now()) + '
        except Exception as e:
            print(e)
            1

        print(time.time()-c1)
        pd.DataFrame(glob.glob(path1 + '/*.zip')).replace({'/home/ismayil/flask_dash/data/nokia/pool/': '4G_'}, regex=True).to_csv(os.path.join('/home/ismayil/flask_dash/data', 'files.csv'), mode='a', header=None, index=False)
        pd.DataFrame(glob.glob(path1 + '/*.zip')).replace({'/home/ismayil/flask_dash/data/nokia/pool/': '4G_util_'}, regex=True).to_csv(os.path.join('/home/ismayil/flask_dash/data', 'files.csv'), mode='a', header=None, index=False)
        return 1
except Exception as e:
    print(e)
