import pandas as pd
import os
import zipfile
import datetime
import time
import glob

#apn_mapping=pd.read_csv('/home/ismayil/flask_dash/support_files/apn_mapping.csv')
apn_mapping=pd.read_excel('/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/Core QA/Mapping for schedule/mapping.xlsx',sheet_name='APN')
def run(path1):
    c1=time.time()
    nsn=pd.DataFrame()
    mapping={'traf':['MNO','APN','Site'],
        'ps_pag':['Site','mode','LAC'],
        'ugw_pdp':['Site','gpname_id'],
        'usn_kpi':['Site','mode','vm_id']
}
    to_save=[]
    path1 = '/home/ismayil/flask_dash/data/nokia/pool'
    print('CMG started')
    traf,usn_kpi,ugw_pdp,ps_pag=([] for i in range(4))
    for dirpath,dirname,filenames in os.walk(path1):
    
        #existing_files=pd.read_csv(os.path.join(path1,'files.txt'), sep=" ", header=None)
        #all_files = glob.glob(dirpath + "/*.csv") working one ############
        all_files = glob.glob(dirpath + "/*Core_KPIs*.zip")
        #print('For dirpath:',dirpath,'len is:',len(all_files))
        if len(all_files)>0:
            for filename in all_files:
                try:
                    if os.path.isfile(os.path.join('/home/ismayil/flask_dash/data', 'files.csv')):
                        existing_files = pd.read_csv(os.path.join('/home/ismayil/flask_dash/data', 'files.csv'), header=None)
                    else: existing_files=pd.DataFrame()
                    files2 = zipfile.ZipFile(filename, 'r')
                    try:
                        if (filename.replace('/home/ismayil/flask_dash/data/nokia/pool/','traf_') in existing_files.values): continue
                        if ('01_14_00' in filename) or ('00_14_00' in filename) or ('02_14_00' in filename): continue
                        df = pd.read_csv(files2.open(files2.namelist()[0]), sep=';')
                        df['Date']=pd.to_datetime(df['PERIOD_START_TIME'],format='%m.%d.%Y %H:%M:%S')
                        if ('04:00' in filename):
                            df = df[df['Date'].str.contains('00:00:00|01:|23:')]
                        df.rename(columns={'PGW Downlink Data Volume, APN':'4G DL',
                        'PGW Uplink Data Volume, APN':'4G UL','GGSN Downlink Data Volume, APN':'2G/3G DL','GGSN Uplink Data Volume, APN':'2G/3G UL',
                        'maxNumberOfActiveGeranBearers (maxNumberOfActiveGeranBearers)':'Max_active_geran_bearers',
                        'maxNumberOfActiveUtranBearers (maxNumberOfActiveUtranBearers)':'Max_active_utran_bearers',
                        'maxNumberOfActiveEutranBearers (maxNumberOfActiveEutranBearers)':'Max_active_eutran_bearers',
                            'apn_id':'APN','CMG name':'Site'},inplace=True)
                        df['2G/3G DL']*=1024
                        df['2G/3G UL']*=1024
                        df['4G DL']=df['4G DL']*1024
                        df['4G UL']=df['4G UL']*1024
                        df['Max_simult_act_PDP_context']=0
                        df['Max_simult_act_PGW_EPS_bearer']=0
                        df['Max_simult_act_S_PGW_EPS_bearer']=0
                        #df=df[['Date','APN','Site','4G DL','4G UL','2G/3G DL','2G/3G UL']]
                        df.loc[df['Site'].str.contains('CMG-CP'),['4G DL','4G UL','2G/3G DL','2G/3G UL']]=0
                        df=df.merge(apn_mapping,how='left', left_on='APN', right_on='APN ID')
                        df['Date']=pd.to_datetime(df['Date'],format='%m.%d.%Y %H:%M:%S') 
                        df=df[['Date','MNO','APN','Site','2G/3G DL','2G/3G UL','4G DL','4G UL','Max_simult_act_PDP_context','Max_simult_act_PGW_EPS_bearer',
                        'Max_simult_act_S_PGW_EPS_bearer','Max_active_utran_bearers','Max_active_eutran_bearers','Max_active_geran_bearers']]
                        df[['2G/3G DL','2G/3G UL','4G DL','4G UL']]=df[['2G/3G DL','2G/3G UL','4G DL','4G UL']].astype('float')
                        df.iloc[:,-6:].fillna(0,inplace=True)
                        df.iloc[:,-6:]=df.iloc[:,-6:].astype('int')
                        traf.append(df)
                        to_save.append(filename.replace('/home/ismayil/flask_dash/data/nokia/pool/','traf_'))
                        print('traf append finished')
                    except Exception as e:
                        print(e,' from traf part')
                        1
                    # PDP and Bearer addition
                    try:
                        if (filename.replace('/home/ismayil/flask_dash/data/nokia/pool/','usnkpi_') in existing_files.values): continue
                        if ('01_14_00' in filename) or ('00_14_00' in filename) or ('02_14_00' in filename): continue
                        df = pd.read_csv(files2.open(files2.namelist()[2]), sep=';')
                        df['Date']=pd.to_datetime(df['PERIOD_START_TIME'],format='%m.%d.%Y %H:%M:%S')
                        if ('04:00' in filename):
                            df = df[df['Date'].str.contains('00:00:00|01:|23:')]
                        df.loc[:,'Gb mode Intra_SGSN RAU success']=df['Gb intra SGSN RA update success ratio, e2e']*df['Gb intra SGSN RA update attempts, e2e']/100
                        df.loc[:,'Iu mode Intra_SGSN RAU success']=df['Iu intra SGSN RA update success ratio, e2e']*df['Iu intra SGSN RA update attempts, e2e']/100
                        df.loc[:,'Gb_auth_succ']=df['GSM_SUCC_MM_AUTH (M22C001)']+df['GSM_SUCC_SM_AUTH (M22C002)']
                        df.loc[:,'Iu_auth_succ']=df['UMTS_SUCC_MM_AUTH (M23C001)']+df['UMTS_SUCC_SM_AUTH (M23C002)']
                        df.loc[:,'S1_paging fail']=df['PAGE_FAIL_NON_SYS_REL (M122C024)']+df['PAGE_FAIL_SYS_REL (M122C025)']+df['PAGE_FAIL_TO (M122C026)']
                        df.loc[:,'Intra_MME X2 HO req']=df['ATT_HO_PATH_SW_NEW_SGW (M114C092)']+df['ATT_HO_PATH_SW_SAME_SGW (M114C101)']
                        df.loc[:,'Intra_MME X2 HO suc']=df['HO_SUCC_PATH_SW_NEW_SGW (M114C096)']+df['HO_SUCC_PATH_SW_SAME_SGW (M114C104)']
                        df.loc[:,'Intra_MME S1 HO req']=df['ATT_S1_HO_SAME_MME_NEW_SGW (M114C070)']+df['ATT_HO_REQ_NO_RELOC (M114C097)']
                        df.loc[:,'Intra_MME S1 HO suc']=df['S1_HO_SUCC_SAME_MME_NEW_SGW (M114C073)']+df['HO_SUCC_HO_REQ_NO_RELOC (M114C100)']
                        df1=df[['Date','CMM name','vm_id','Gb GPRS attach attempts, e2e','SUCC_GPRS_ATTACH (M01C000)','Gb intra SGSN RA update attempts, e2e',
                                'Gb mode Intra_SGSN RAU success','Gb inter SGSN RA update attempts, e2e','SUCC_INTER_SGSN_RA_UPDAT (M01C019)',
                                'SGSN_LEVEL_PS_PAGINGS (M26C000)','SGSN_LEVEL_UNSUCC_PS_PAG (M26C001)','Gb MO PDP context activation attempts',
                                'SUCC_MO_PDP_CONTEXT_ACT (M02C000)','2G peak attach users','MAX_HOME_SUBS (M108C033)','MAX_VISITING_SUBS (M108C035)',
                                'GSM_AUTH_ATTEMPTS (M22C000)','Gb_auth_succ']]
                        df1.insert(13,'USIM Auth success',0)
                        df1.insert(13,'SIM Auth success',0)
                        df1.insert(13,'USIM Auth request',0)
                        df1.insert(13,'SIM Auth request',0)
                        df1.loc[:,['Intra_MME X2 HO req','Intra_MME X2 HO suc','Intra_MME S1 HO req','Intra_MME S1 HO suc','Inter_MME HO req','Inter_MME HO suc']]=0
                        df1.loc[:,'mode']='Gb mode'
                        df1.columns=['Date','Site','vm_id','Data attach','Data accept','Intra_SGSN_MME RAU request','Intra_SGSN_MME RAU success',
                        'Inter_SGSN_MME RAU request','Inter_SGSN_MME RAU success','packet paging request','packet paging none_response',
                        'MS init PDP_bear context act','MS init PDP_bear context act suc','SIM Auth request','USIM Auth request','SIM Auth success',
                        'USIM Auth success','Max attach user','Max_home_subs','Max_visiting_subs','Total Auth request','Total Auth success','Intra_MME X2 HO req',
                        'Intra_MME X2 HO suc','Intra_MME S1 HO req','Intra_MME S1 HO suc','Inter_MME HO req','Inter_MME HO suc','mode']
                        df2=df[['Date','CMM name','vm_id','Iu GPRS attach attempts, e2e','IU_SUCC_GPRS_ATTACH (M16C000)','Iu intra SGSN RA update attempts, e2e',
                                'Iu mode Intra_SGSN RAU success','Iu inter SGSN RA update attempts, e2e','IU_SUCC_IN_INTER_SGSN_RA_UPD (M16C026)',
                                'SGSN_LEVEL_IU_PS_PAGINGS (M24C000)','SGSN_LEVEL_UNSUCC_IU_PS_PAG (M24C001)','Iu MO PDP activation attempts, e2e',
                                'IU_SUCC_MO_PDP_CON_ACT (M17C000)','3G peak attach users','UMTS_AUTH_ATTEMPTS (M23C000)','Iu_auth_succ']]
                        df2.insert(13,'USIM Auth success',0)
                        df2.insert(13,'SIM Auth success',0)
                        df2.insert(13,'USIM Auth request',0)
                        df2.insert(13,'SIM Auth request',0)
                        df2.insert(18,'Max_visiting_subs',0)
                        df2.insert(18,'Max_home_subs',0)
                        df2.loc[:,['Intra_MME X2 HO req','Intra_MME X2 HO suc','Intra_MME S1 HO req','Intra_MME S1 HO suc','Inter_MME HO req','Inter_MME HO suc']]=0
                        df2.loc[:,'mode']='Iu mode'
                        df2.columns=df1.columns
                        df3=df[['Date','CMM name','vm_id','ATT_NON_EPS_ATCH (M118C299)','SUCC_NON_EPS_ATCH (M118C321)','ATT_TAU (M118C255)','SUCC_TAU (M118C278)',
                                'TAU_INTER_MME_ATT (M118C246)','TAU_INTER_MME_SUCC (M118C248)','ATT_PAGE (M122C019)','S1_paging fail',
                                'ACT_DFLT_EPS_BRR_ATT (M103C193)','ACT_DFLT_EPS_BRR_SUCC (M103C194)','Peak EPS EMM-REGISTERED users, MME','ATT_AUTH_REQ_UE (M101C013)',
                                'SUCC_AUTH_REQ_UE (M101C021)','Intra_MME X2 HO req','Intra_MME X2 HO suc','Intra_MME S1 HO req','Intra_MME S1 HO suc']]
                        df3.insert(13,'USIM Auth success',0)
                        df3.insert(13,'SIM Auth success',0)
                        df3.insert(13,'USIM Auth request',0)
                        df3.insert(13,'SIM Auth request',0)
                        df3.insert(18,'Max_visiting_subs',0)
                        df3.insert(18,'Max_home_subs',0)
                        df3.loc[:,['Inter_MME HO req','Inter_MME HO suc']]=0
                        df3.loc[:,'mode']='S1 mode'
                        df3.columns=df1.columns
                        df=pd.concat([df1,df2,df3])
                        df['Date']=pd.to_datetime(df['Date'],format='%m.%d.%Y %H:%M:%S')
                        df=df[['Date','Site','mode','vm_id','Data attach','Data accept','Intra_SGSN_MME RAU request','Intra_SGSN_MME RAU success',
                        'Inter_SGSN_MME RAU request','Inter_SGSN_MME RAU success','packet paging request','packet paging none_response',
                        'MS init PDP_bear context act','MS init PDP_bear context act suc','SIM Auth request','SIM Auth success','USIM Auth request',
                        'USIM Auth success','Max attach user','Max_home_subs','Max_visiting_subs','Total Auth request','Total Auth success','Intra_MME X2 HO req',
                        'Intra_MME X2 HO suc','Intra_MME S1 HO req','Intra_MME S1 HO suc','Inter_MME HO req','Inter_MME HO suc']]
                        df.iloc[:,4:].fillna(0,inplace=True)
                        df.iloc[:,4:]=df.iloc[:,4:].astype('float')
                        df=df[df.iloc[:,4:].sum(axis=1)>0]
                        usn_kpi.append(df)
                        to_save.append(filename.replace('/home/ismayil/flask_dash/data/nokia/pool/','usnkpi_'))
                        print('CMM part finished')
                    except Exception as e:
                        print(e,' from cmm part')
                        1
                    #cmm 4G pag begin
                    try:
                        if (filename.replace('/home/ismayil/flask_dash/data/nokia/pool/','pag_') in existing_files.values): continue
                        if ('01_14_00' in filename) or ('00_14_00' in filename) or ('02_14_00' in filename): continue
                        df = pd.read_csv(files2.open(files2.namelist()[3]), sep=';')
                        df['Date']=pd.to_datetime(df['PERIOD_START_TIME'],format='%m.%d.%Y %H:%M:%S')
                        if ('04:00' in filename):
                            df = df[df['Date'].str.contains('00:00:00|01:|23:')]
                        df.rename(columns={'ATT_PAGE_TA (M124C005)':'Packet Paging request','CMM name':'Site','tac_id':'LAC'},inplace=True)
                        df['Packet Paging response']=df['PAGE_SUCC_FIRST_ATT_TA (M124C006)']+df['PAGE_SUCC_FOURTH_ATT_TA (M124C009)']+\
                                        df['PAGE_SUCC_SECOND_ATT_TA (M124C007)']+df['PAGE_SUCC_THIRD_ATT_TA (M124C008)']
                        df[['Max attached subs','RAC']]=0
                        df['mode']='S1 mode'
                        df1=df[['Date','Site','mode','LAC','RAC','Packet Paging request','Packet Paging response','Max attached subs']]
                    except Exception as e:
                        print(e,' from cmm 4G pag part')
                        df1=pd.DataFrame()
                        1
                    try:
                        if (filename.replace('/home/ismayil/flask_dash/data/nokia/pool/','pag_') in existing_files.values): continue
                        if ('01_14_00' in filename) or ('00_14_00' in filename) or ('02_14_00' in filename): continue
                        df = pd.read_csv(files2.open(files2.namelist()[4]), sep=';')
                        df['Date']=pd.to_datetime(df['PERIOD_START_TIME'],format='%m.%d.%Y %H:%M:%S')
                        if ('04:00' in filename):
                            df = df[df['Date'].str.contains('00:00:00|01:|23:')]
                        df.rename(columns={'IU_PS_PAGING_PROCS (M025C001)':'Packet Paging request','CMM name':'Site','iulac_id':'LAC'},inplace=True)
                        df['Packet Paging response']=df['Packet Paging request']-df['UNSUCC_IU_PS_PAGING_PROCS (M025C002)']
                        df[['Max attached subs','RAC']]=0
                        df['mode']='Iu mode'
                        df2=df[['Date','Site','mode','LAC','RAC','Packet Paging request','Packet Paging response','Max attached subs']]
                    except Exception as e:
                        print(e,' from cmm 3G pag part')
                        df2=pd.DataFrame()
                        1
                    try:
                        if (filename.replace('/home/ismayil/flask_dash/data/nokia/pool/','pag_') in existing_files.values): continue
                        if ('01_14_00' in filename) or ('00_14_00' in filename) or ('02_14_00' in filename): continue
                        df = pd.read_csv(files2.open(files2.namelist()[5]), sep=';')
                        df['Date']=pd.to_datetime(df['PERIOD_START_TIME'],format='%m.%d.%Y %H:%M:%S')
                        if ('04:00' in filename):
                            df = df[df['Date'].str.contains('00:00:00|01:|23:')]
                        df.rename(columns={'GB_PS_PAGING_PROCS (M27C001)':'Packet Paging request','CMM name':'Site','lac_id':'LAC'},inplace=True)
                        df['Packet Paging response']=df['Packet Paging request']-df['UNSUCC_GB_PS_PAGING_PROCS (M27C002)']
                        df[['Max attached subs','RAC']]=0
                        df['mode']='Gb mode'
                        df3=df[['Date','Site','mode','LAC','RAC','Packet Paging request','Packet Paging response','Max attached subs']]
                    except Exception as e:
                        print(e,' from cmm 2G pag part')
                        df3=pd.DataFrame()
                        1
                    try:
                        df=pd.concat([df1,df2,df3])
                        df['RAC']=None
                        df['LAC']=df['LAC'].astype('str')
                        df.iloc[:,5:].fillna(0,inplace=True)
                        df.iloc[:,5:]=df.iloc[:,5:].astype('int')
                        ps_pag.append(df)
                        to_save.append(filename.replace('/home/ismayil/flask_dash/data/nokia/pool/','pag_'))
                        print('ps paging finished')
                    except Exception as e: 
                        print(e)
                        1
                    try:
                        if (filename.replace('/home/ismayil/flask_dash/data/nokia/pool/','ugwpdp_') in existing_files.values): continue
                        if ('01_14_00' in filename) or ('00_14_00' in filename) or ('02_14_00' in filename): continue
                        df = pd.read_csv(files2.open(files2.namelist()[1]), sep=';')
                        df['Date']=pd.to_datetime(df['PERIOD_START_TIME'],format='%m.%d.%Y %H:%M:%S')
                        if ('04:00' in filename):
                            df = df[df['Date'].str.contains('00:00:00|01:|23:')]
                        df.rename(columns={'Downlink packets':'DL_packets','Uplink packets':'UL_packets',
                                            'CreatePdpReq (CreatePdpReq)':'PDP request','CreatePdpRsp (CreatePdpRsp)':'PDP success','CMG name':'Site'},inplace=True)
                        df.loc[:,'DL_Drop_packets']=df['Downlink Packet Drop ratio']/100*df['DL_packets']
                        df.loc[:,'UL_Drop_packets']=df['Uplink Packet Drop ratio']/100*df['UL_packets']
                        #df.loc[:,'PDP success']=df['PDP request']-df['CreatePdpRspFail (CreatePdpRspFail)']
                        df=df[['Date','Site','gpname_id','PDP request','PDP success','DL_packets','UL_packets','DL_Drop_packets','UL_Drop_packets']]
                        df=df[df.iloc[:,3:].sum(axis=1)>0]
                        df.iloc[:,3:].fillna(0,inplace=True)
                        df.iloc[:,3:]=df.iloc[:,3:].astype('float')
                        ugw_pdp.append(df)
                        to_save.append(filename.replace('/home/ismayil/flask_dash/data/nokia/pool/','ugwpdp_'))
                        print('CMG part finished')
                    except Exception as e:
                        print(e,' from cmg part')
                        1
                    to_save.append()
                except Exception as e:
                    print(e, filename,' outer exception was raised')
                    1
    print(time.time()-c1,'time to save traf')
    
    for i in ['traf','ps_pag','ugw_pdp','usn_kpi']:
        try:
            if len(i)>0:
                df= pd.concat(eval(i)).reset_index(drop=True)
                for j in df['Date'].unique():
                        try:
                            #file_name = datetime.datetime.strftime(datetime.datetime.utcfromtimestamp(j.astype(datetime.datetime) / 1e9),"%B_%Y")
                            file_name2 = datetime.datetime.strftime(datetime.datetime.utcfromtimestamp(j.astype(datetime.datetime) / 1e9),"%Y-%m-%d")
                            try:
                                a=pd.read_hdf('/disk2/support_files/archive/core/core_new_'+file_name2+'.h5',i, where='Date=j and Site="10.34.170.1@CSN3_CMG-UP"')
                                if len(a)>0:
                                    print(j,'not appended')
                                    continue
                            except:
                                1
                            df.loc[df['Date']==j].to_hdf('/disk2/support_files/archive/core/core_new_'+file_name2+'.h5',i,append=True,
                                format='table', data_columns=['Date',*mapping[i]], min_itemsize={u:100 for u in mapping[i]},complevel=5)
                        except Exception as e:
                            print(e)
                            print(j)
                            1
        except Exception as e:
            print(e)
            print(i)

    pd.DataFrame(glob.glob(path1 + '/*Core_KPIs*.zip')).replace({'/home/ismayil/flask_dash/data/nokia/pool/': ''}, regex=True).to_csv(os.path.join('/home/ismayil/flask_dash/data', 'files.csv'), mode='a', header=None, index=False)
    
    print(time.time()-c1,'all finished')

    return 1
