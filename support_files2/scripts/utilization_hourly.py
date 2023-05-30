import os,time,datetime
import pandas as pd
import numpy as np
c1=time.time()

def run():
    tcp,prb,rrc,frame,ippm,rrc_lic=([] for i in range(6))
    path='/home/ismayil/flask_dash/data/utilization'

    #WBTS level monitoring (M5008).HSDPA_MAX_MACHS_THR (M5008C0) / WBTS level monitoring (M5008).LIC_HSDPA_THR (M5008C29)) - RNC_5396a

    #WBTS level monitoring (M5008).HSUPA_MAX_MACE_THR (M5008C2) / WBTS level monitoring (M5008).LIC_HSUPA_THR (M5008C30)) - RNC_5397a

    #WBTS level monitoring (M5008).MAX_BTS_HSDPA_USERS (M5008C21) / WBTS level monitoring (M5008).LIC_NUM_HSDPA_USERS (M5008C31)) - RNC_5398a

    #(cellres.tx_cr_pwr_class_9 + cellres.tx_cr_pwr_class_10) /
    #(cellres.tx_cr_pwr_class_0 + cellres.tx_cr_pwr_class_1 + cellres.tx_cr_pwr_class_2 + cellres.tx_cr_pwr_class_3 + cellres.tx_cr_pwr_class_4 +
    #cellres.tx_cr_pwr_class_5 + cellres.tx_cr_pwr_class_6 + cellres.tx_cr_pwr_class_7 + cellres.tx_cr_pwr_class_8 + cellres.tx_cr_pwr_class_9 + cellres.tx_cr_pwr_class_10)) - RNC_5202a

    lookup=pd.read_csv('/home/ismayil/flask_dash/support_files/Local_Cell_ID.csv')
    if os.path.isfile(os.path.join('/home/ismayil/flask_dash/data', 'files.csv')):
            existing_files = pd.read_csv(os.path.join('/home/ismayil/flask_dash/data', 'files.csv'), header=None)
    else: existing_files=pd.DataFrame()
    for file in os.listdir(path):               
        if (file in existing_files.values): continue
        if 'IPPM' in file:
            ippm.append(pd.read_csv(os.path.join(path, file), skiprows=[1]))
        elif 'Frame_Loss' in file:
            frame.append(pd.read_csv(os.path.join(path, file), skiprows=[1]))
        elif 'LTE_PRB' in file:
            prb.append(pd.read_csv(os.path.join(path, file), skiprows=[1]))
        elif 'TCP' in file:
            tcp.append(pd.read_csv(os.path.join(path, file), skiprows=[1]))
        elif 'RRC Users eNodeB' in file:
            rrc.append(pd.read_csv(os.path.join(path, file), skiprows=[1]))
        elif 'LTE_RRC_Users' in file:
            r=pd.read_csv(os.path.join(path, file), skiprows=[1])
            if len(r.columns)>15:
                rrc_lic.append(r)


        # RRC Lic part: RAB failures
    try:
        if len(rrc_lic)>0:
            df = pd.concat(rrc_lic)
            df.drop(columns=['1526727379','1526728446','1526728456','1526728976','Reliability','Granularity Period'],inplace=True)
            df['Result Time'] = pd.to_datetime(df['Result Time'])
            df.rename(columns={'Object Name': 'Site_name', 'Result Time': 'Date','1526730878':'RRC_User_License',
            '1526732726':'RAB_failure_Conflict_Hofail','526728276':'RAB_failure_MME','1526729958':'RAB_failure_MME_VoIP',
            '1526728279':'RAB_failure_NoRadioRes','1526729955':'RAB_failure_NoRadioRes_DLSatis','1526729968':'RAB_failure_NoRadioRes_DLSatis_VoIP',
            '1526729953':'RAB_failure_NoRadioRes_DLThrpLic','1526729966':'RAB_failure_NoRadioRes_DLThrpLic_VoIP','1526729546':'RAB_failure_NoRadioRes_PUCCH',
            '1526729965':'RAB_failure_NoRadioRes_PUCCH_VoIP','1526742167':'RAB_failure_NoRadioRes_RrcUserLic','1526729545':'RAB_failure_NoRadioRes_SRS',
            '1526729964':'RAB_failure_NoRadioRes_SRS_VoIP','1526729956':'RAB_failure_NoRadioRes_ULSatis','1526729969':'RAB_failure_NoRadioRes_ULSatis_VoIP',
            '1526729954':'RAB_failure_NoRadioRes_ULThrpLic','1526729967':'RAB_failure_NoRadioRes_ULThrpLic_VoIP','1526742166':'RAB_failure_NoRadioRes_UserSpec',
            '1526729961':'RAB_failure_NoRadioRes_VoIP','1526726717':'RAB_failure_NoReply','1526729957':'RAB_failure_NoReply_VoIP',
            '1526728278':'RAB_failure_RNL','1526729960':'RAB_failure_RNL_VoIP','1526728280':'RAB_failure_SecurModeFail',
            '1526729962':'RAB_failure_SecurModeFail_VoIP','1526747893':'RAB_failure_SpecialUE','1526729950':'RAB_failure_SRBReset',
            '1526729963':'RAB_failure_SRBReset_VoIP','1526728277':'RAB_failure_TNL','1526729970':'RAB_failure_TNL_DLRes_VoIP',
            '1526729951':'RAB_failure_TNL_DLRes','1526729952':'RAB_failure_TNL_ULRes','1526729971':'RAB_failure_TNL_ULRes_VoIP',
            '1526729959':'RAB_failure_TNL_VoIP','1526739740':'RAB_failure_X2AP','1526739741':'RAB_failure_X2AP_VoIP','1526728276':'RAB_failure_MME'}, inplace=True)
            df['Site_name'] = df['Site_name'].apply(lambda x: x[:x.find('/')])
            keys=list(df.columns)
            keys.remove('Site_name')
            keys.remove('Date')
            keys.remove('RRC_User_License')
            agg_dict=dict.fromkeys(keys, 'sum')
            agg_dict['RRC_User_License']='sum'
            df=df.groupby(['Date','Site_name']).agg(agg_dict)
            df.reset_index(inplace=True)
            df=df.melt(id_vars=['Date','Site_name','RRC_User_License'],value_vars=keys,var_name='Fail_reason',value_name='Fails')
            df.reset_index(inplace=True)
            df['Fails']=df['Fails'].astype(int)
            df['IPPM_lost']=0
            df['IPPM_tx']=0
            df['IPPM_rx']=0
            df[['IPPM_lost','IPPM_rx','IPPM_tx','RRC_User_License']]=df[['IPPM_lost','IPPM_rx','IPPM_tx','RRC_User_License']].astype(float)
            df=df[['Date','Site_name','IPPM_lost','IPPM_tx','IPPM_rx','RRC_User_License','Fail_reason','Fails']]
            for i in df['Date'].unique():
                file_name = datetime.datetime.strftime(datetime.datetime.utcfromtimestamp(i.astype(datetime.datetime)/1e9), "%B_%Y")
                file_name2 = datetime.datetime.strftime(datetime.datetime.utcfromtimestamp(i.astype(datetime.datetime)/1e9), "%Y-%m-%d")
                df.loc[df['Date']==i].to_hdf('/disk2/support_files/archive/util/'+file_name2+'_util_hourly.h5', '/site_4g', append=True, format='table',
                                data_columns=['Date','Site_name','Fail_reason'], complevel=5, 
                                min_itemsize={'Site_name':30,'RRC_User_License':30,'Fail_reason':30,'IPPM_lost':10,'IPPM_tx':10,'IPPM_rx':10,'Fails':10})
            needed = datetime.datetime.strptime(datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(hours=1), '%d.%m.%y %H:00'), '%d.%m.%y %H:00')
            df=df[(df['Date']==needed) & (df['Fail_reason']=='RAB_failure_NoRadioRes') & (df['Fails']>0)]
            if len(df)>0:
                from send_notification import send_mail
                attachment=False
                try:
                    import alarms
                    df2,df3,df8,df9=alarms.check_alarm(df)
                except Exception as e:
                    print(e)
                    df2=pd.DataFrame()
                    1
                df['RRC_User_License']=df['RRC_User_License'].fillna(0).astype(int)
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
                            'LTE No Radio Resource fails','Fails increased on below sites for the last hour',
                            round(df[['Site_name','RRC_User_License','Fail_reason','Fails','Down_tech','Down_neighbors','Down_alarm_count_on_neighbors']].\
                                sort_values(by='Fails',ascending=False),0),datetime.datetime.strftime(needed,'%d.%m.%Y %H:%M'),attachment)
            print(time.time() - c1, 'huawei rrc_rab fails finish')
    except Exception as e:
        print(e)
        pass

        # Frame loss part
    try:
        if len(frame)>0:
            df = pd.concat(frame)
            df['Frame_error_huawei'] = df['1542460324'] + df['1542460323']
            df['Result Time'] = pd.to_datetime(df['Result Time'])
            df.rename(columns={'Object Name': 'Cell', 'Result Time': 'Date'}, inplace=True)
            df['WBTS_name'] = df['Cell'].apply(lambda x: x[:x.find('/')])
            df=pd.merge(df,lookup[['NODEBNAME','BSCName']],left_on='WBTS_name',right_on='NODEBNAME',how='left')
            df[['HSDPA_MAX_MACHS_THR','LIC_HSDPA_THR','HSUPA_MAX_MACE_THR',
                    'LIC_HSUPA_THR','MAX_BTS_HSDPA_USERS','LIC_NUM_HSDPA_USERS','MAX_USED_CE_R99_DL','MAX_USED_CE_R99_UL',
                    'MAX_AVAIL_R99_CE','Max_HSDPA_user_util_ratio','Max_HSUPA_thr_util_ratio','Max_HSDPA_thr_util_ratio','HS_DSCH_CREDIT_RDCT_FRM_LOSS (M5000C178)']]=np.NAN
            df.rename(columns={'BSCName':'RNC_name'},inplace=True)
            df=df[['Date','RNC_name','WBTS_name','HSDPA_MAX_MACHS_THR','LIC_HSDPA_THR','HSUPA_MAX_MACE_THR',
                    'LIC_HSUPA_THR','MAX_BTS_HSDPA_USERS','LIC_NUM_HSDPA_USERS','MAX_USED_CE_R99_DL','MAX_USED_CE_R99_UL',
                    'MAX_AVAIL_R99_CE','Max_HSDPA_user_util_ratio','Max_HSUPA_thr_util_ratio','Max_HSDPA_thr_util_ratio','HS_DSCH_CREDIT_RDCT_FRM_LOSS (M5000C178)','Frame_error_huawei']]
            for i in df['Date'].unique():
                file_name = datetime.datetime.strftime(datetime.datetime.utcfromtimestamp(i.astype(datetime.datetime)/1e9), "%B_%Y")
                file_name2 = datetime.datetime.strftime(datetime.datetime.utcfromtimestamp(i.astype(datetime.datetime)/1e9), "%Y-%m-%d")
                df.loc[df['Date']==i].to_hdf('/disk2/support_files/archive/util/'+file_name2+'_util_hourly.h5', '/site_3g', append=True, format='table',
                                data_columns=['Date','RNC_name','WBTS_name'], complevel=5, min_itemsize={'RNC_name':20,'WBTS_name':30})
            print(time.time() - c1, 'huawei frame loss finish')
    except Exception as e:
        print(e)
        pass

        # Huawei TCP Utilization part
    try:
        if len(tcp)>0:
            frame3 = pd.concat(tcp)
            frame3['Date'] = pd.to_datetime(frame3['Result Time'])
            import re
            func = lambda x: re.search(r'\w{4}\d{4}\w', x).group()
            frame3['WCEL_name'] = frame3['Object Name'].apply(func)
            frame3['67199618'] = pd.to_numeric(frame3['67199618'], errors='coerce')
            frame3 = pd.merge(frame3, lookup[['CELLNAME', 'MAXTXPOWER','NODEBNAME','BSCName']], left_on='WCEL_name', right_on='CELLNAME', how='left')
            frame3['TCP_Utilization'] = round(10 ** (frame3['67199618'] / 10) / 10 ** (frame3['MAXTXPOWER'] / 100) * 100, 2)
            frame3.rename(columns={'67199618': 'VS.MeanTCP_dBm','NODEBNAME':'WBTS_name','BSCName':'RNC_name'}, inplace=True)
            frame3[['IUB_LOSS_CC_FRAME_LOSS_IND',
                    'TX_CR_PWR_CLASS_0','TX_CR_PWR_CLASS_1','TX_CR_PWR_CLASS_2','TX_CR_PWR_CLASS_3','TX_CR_PWR_CLASS_4','TX_CR_PWR_CLASS_5',
                    'TX_CR_PWR_CLASS_6','TX_CR_PWR_CLASS_7','TX_CR_PWR_CLASS_8','TX_CR_PWR_CLASS_9','TX_CR_PWR_CLASS_10','Ovl_TxCrPwr_time_share_DL']]=np.NAN
            frame3=frame3[['Date','RNC_name','WBTS_name','WCEL_name','IUB_LOSS_CC_FRAME_LOSS_IND',
                    'TX_CR_PWR_CLASS_0','TX_CR_PWR_CLASS_1','TX_CR_PWR_CLASS_2','TX_CR_PWR_CLASS_3','TX_CR_PWR_CLASS_4','TX_CR_PWR_CLASS_5',
                    'TX_CR_PWR_CLASS_6','TX_CR_PWR_CLASS_7','TX_CR_PWR_CLASS_8','TX_CR_PWR_CLASS_9','TX_CR_PWR_CLASS_10','Ovl_TxCrPwr_time_share_DL',
                    'MAXTXPOWER','VS.MeanTCP_dBm','TCP_Utilization']]
            frame3.iloc[:,4:]=frame3.iloc[:,4:].astype(float)
            for i in frame3['Date'].unique():
                file_name = datetime.datetime.strftime(datetime.datetime.utcfromtimestamp(i.astype(datetime.datetime)/1e9), "%B_%Y")
                file_name2 = datetime.datetime.strftime(datetime.datetime.utcfromtimestamp(i.astype(datetime.datetime)/1e9), "%Y-%m-%d")
                frame3.loc[frame3['Date']==i].to_hdf('/disk2/support_files/archive/util/'+file_name2+'_util_hourly.h5', '/cell_3g', append=True, format='table',
                                data_columns=['Date','RNC_name','WBTS_name','WCEL_name'], complevel=5, min_itemsize={'WCEL_name': 30,'RNC_name':20,'WBTS_name':30})
            print(time.time() - c1, 'huawei tcp finish')
    except Exception as e:
        print(e)
        pass

        # IPPM part
    try:
        if len(ippm)>0:
            df = pd.concat(ippm)
            df.rename(columns={'1542455911':'IPPM_tx','1542455912':'IPPM_rx'},inplace=True)
            df['IPPM_lost'] = df['IPPM_tx'] - df['IPPM_rx']
            df['Date'] = pd.to_datetime(df['Result Time'])
            df['Site_name'] = df['Object Name'].apply(lambda x: x[:x.find('/')])
            df['RRC_User_License']=0
            df['Fail_reason']=None
            df['Fails']=0
            df[['IPPM_lost','IPPM_rx','IPPM_tx','RRC_User_License']]=df[['IPPM_lost','IPPM_rx','IPPM_tx','RRC_User_License']].astype(float)
            df=df[['Date','Site_name','IPPM_lost','IPPM_tx','IPPM_rx','RRC_User_License','Fail_reason','Fails']]
            for i in df['Date'].unique():
                file_name = datetime.datetime.strftime(datetime.datetime.utcfromtimestamp(i.astype(datetime.datetime)/1e9), "%B_%Y")
                file_name2 = datetime.datetime.strftime(datetime.datetime.utcfromtimestamp(i.astype(datetime.datetime)/1e9), "%Y-%m-%d")
                df.loc[df['Date']==i].to_hdf('/disk2/support_files/archive/util/'+file_name2+'_util_hourly.h5', '/site_4g', append=True, format='table',
                                data_columns=['Date','Site_name','Fail_reason'], complevel=5, min_itemsize={'Site_name':30,'RRC_User_License':30,'Fail_reason':30,'IPPM_lost':10,
                                'IPPM_tx':10,'IPPM_rx':10,'Fails':10})

            print(time.time() - c1, 'huawei ippm finish')
    except Exception as e:
        print(e)
        pass

        # Huawei PRB Utilization part
    try:
        if len(prb)>0:
            df = pd.concat(prb)
            func = lambda x: re.search('\w{4}\d{4}\w\w?\d', x).group()
            df['Cell_name'] = df['Object Name'].apply(
                lambda x: x[x.find('=', x.find('Cell Name')) + 1:x.find(',', x.find('Cell Name'))])
            df['DL_PRB_Utilzation'] = df['1526726740'] / df['1526728433'] * 100
            df['UL_PRB_Utilzation'] = df['1526726737']/df['1526728434']*100
            func = lambda x: x.replace(' DST', '', na=True)
            filt = df['Result Time'].str.contains('DST', na=False)
            df.loc[filt, 'Result Time'] = df.loc[filt, 'Result Time'].str.replace(' DST', '')
            filt = (df['Result Time'].str.contains(r'\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}', na=False))
            df.loc[filt, 'Result Time'] = pd.to_datetime(df.loc[filt]['Result Time'])
            func = lambda x: str.isdigit(str(x))
            filt = df['Result Time'].apply(func)
            df.loc[filt, 'Result Time'] = pd.to_datetime(df.loc[filt, 'Result Time'])
            func = lambda x: isinstance(x, datetime.date)
            filt = df['Result Time'].apply(func)
            df.loc[filt, 'Result Time'] = pd.to_datetime(df.loc[filt, 'Result Time'])
            df['Date'] = pd.to_datetime(df['Result Time'])
            df['Site_name']=df['Cell_name'].apply(lambda x: x[:8])
            df['Vendor']='Huawei'
            df[['PDCP_SDU_LOSS_UL_FNA','PDCP_SDU_LOSS_DL_FNA','PDCP_SDU_DL_DISC','UL_RLC_PDU_DISC']]=np.NAN
            df=df[['Date','Vendor','Site_name','Cell_name','DL_PRB_Utilzation','UL_PRB_Utilzation','PDCP_SDU_LOSS_UL_FNA',
                            'PDCP_SDU_LOSS_DL_FNA','PDCP_SDU_DL_DISC','UL_RLC_PDU_DISC']]
            df.columns=['Date','Vendor','Site_name','Cell_name','DL_PRB_Utilzation','UL_PRB_Utilzation','PDCP_SDU_LOSS_UL_FNA',
                            'PDCP_SDU_LOSS_DL_FNA','PDCP_SDU_DL_DISC','UL_RLC_PDU_DISC']

            for i in df['Date'].unique():
                file_name = datetime.datetime.strftime(datetime.datetime.utcfromtimestamp(i.astype(datetime.datetime)/1e9), "%B_%Y")
                file_name2 = datetime.datetime.strftime(datetime.datetime.utcfromtimestamp(i.astype(datetime.datetime)/1e9), "%Y-%m-%d")
                df.loc[df['Date']==i].to_hdf('/disk2/support_files/archive/util/'+file_name2+'_util_hourly.h5', '/cell_4g', append=True, format='table',
                                data_columns=['Date','Vendor','Site_name','Cell_name'], complevel=5, min_itemsize={'Cell_name': 30,'Vendor':20,'Site_name':30})
        
            print(time.time() - c1, 'huawei prb finish')
    except Exception as e:
        print(e)
        pass


    print(time.time()-c1,'h5 data take finish')
    pd.DataFrame(os.listdir(os.path.join('/home/ismayil/flask_dash/data', 'utilization'))).to_csv(os.path.join('/home/ismayil/flask_dash/data', 'files.csv'), mode='a', header=None, index=False)
    print(time.time()-c1)
