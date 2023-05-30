import pandas as pd
import datetime
import os,glob
import new_core_reports_nokia

new_core_reports_nokia.run('fsdf')
print('nokia core finished')
existing_files = pd.read_csv(os.path.join('/home/ismayil/flask_dash/data', 'files.csv'), header=None)


#apn_mapping=pd.read_csv('/home/ismayil/flask_dash/support_files/apn_mapping.csv')
apn_mapping=pd.read_excel('/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/Core QA/Mapping for schedule/mapping.xlsx',sheet_name='APN')

mapping={'traf':['MNO','APN','Site'],
        'auth_sms_vlr':['Site'],
        'ho_intramsc':['Site','HO_type'],
        'mo_mt_ccr':['Site','Direction'],
        'pag_per_lac':['Site','LAC','BSC_RNC'],
        'hss_kpi':['Site','Type'],
        'hss_cpu_mem':['Site','Slot'],
        'hss_subs':['Site'],
        'in_kpi':['Site','SRVKEY'],
        'interconnect':['Site','MTP_Link'],
        'mgw_mua':['Site','M3UA_Link'],
        'ab_int':['Site','Direction','Trunk_group','TG'],
        'ab_vtraff':['Site','NE','Office','Office_dir'],
        'ats_basics':['Site'],
        'abcf_basics':['Site'],
        'scsf':['Site','Type','Identifier'],
        'srvcc':['Site','Peer_entity'],
        'ims_usn':['Site'],
        'ps_pag':['Site','mode','LAC'],
        'usn_pdp_plmn':['Site'],
        'csfb_pag':['Site','Entity_name'],
        'ugw_pdp':['Site','gpname_id'],
        'usn_kpi':['Site','mode','vm_id'],
        'int_inc_out_sip':['Site','Direction','TG','Destination']
}

traf,auth_sms_vlr,ho_intramsc,mo_mt_ccr,pag_per_lac,hss_kpi,hss_cpu_mem,hss_subs,in_kpi,interconnect,mgw_mua,ab_int,ab_vtraff,ats_basics,abcf_basics,\
        scsf,srvcc,ims_usn,ps_pag,usn_pdp_plmn,csfb_pag,ugw_pdp,usn_kpi,int_inc_out_sip=([] for i in range(24))
to_save=[]
for file in os.listdir('/home/ismayil/flask_dash/data/core'):
    try:
        if file in existing_files:continue
        if ('_qar' in file) :
            df=pd.read_csv(os.path.join('/home/ismayil/flask_dash/data/core',file),skiprows=[1])
            if ('qar_UGW_DT_bearers' in file) or ('qar_vUGW_DT_bearers' in file):
                df.rename(columns={'134706896':'2G/3G UL','134706897':'2G/3G DL','1911621400':'2G/3G UL','1911621401':'2G/3G DL'},inplace=True)
                if '_UGW' in file:
                    df.loc[:,'4G UL']=df['138413069']+df['142608004'] #KB
                    df.loc[:,'4G DL']=df['138413071']+df['142608006']
                    df.loc[:,'Max_simult_act_PDP_context']=df['134706620']
                    df.loc[:,'Max_simult_act_PGW_EPS_bearer']=df['138414007']
                    df.loc[:,'Max_simult_act_S_PGW_EPS_bearer']=df['142606506']
                else:
                    df.loc[:,'4G UL']=df['1911620086']+df['1911621989']
                    df.loc[:,'4G DL']=df['1911620088']+df['1911621991']
                    df.loc[:,'Max_simult_act_PDP_context']=df['1911621334']
                    df.loc[:,'Max_simult_act_PGW_EPS_bearer']=df['1911620255']
                    df.loc[:,'Max_simult_act_S_PGW_EPS_bearer']=df['1911621695']
                df['Max_active_utran_bearers']=0
                df['Max_active_eutran_bearers']=0
                df['Max_active_geran_bearers']=0
                df['Date']=pd.to_datetime(df['Result Time'],format='%Y-%m-%d %H:%M')
                df['Site']=df['Object Name'].map(lambda x: x.split('/')[0])
                df['APN']=df['Object Name'].map(lambda x: x.split('/')[1])
                filter_apn=df['APN'].str.contains('APN=')
                df.loc[filter_apn,'APN']=df.loc[filter_apn,'APN'].map(lambda x: x[x.find('APN=')+4:])
                df=df.merge(apn_mapping,how='left', left_on='APN', right_on='APN ID')
                df=df[['Date','MNO','APN','Site','2G/3G DL','2G/3G UL','4G DL','4G UL','Max_simult_act_PDP_context','Max_simult_act_PGW_EPS_bearer',
                    'Max_simult_act_S_PGW_EPS_bearer','Max_active_utran_bearers','Max_active_eutran_bearers','Max_active_geran_bearers']]
                df[['2G/3G DL','2G/3G UL','4G DL','4G UL']]=df[['2G/3G DL','2G/3G UL','4G DL','4G UL']].astype('float')
                df.iloc[:,-6:]=df.iloc[:,-6:].astype('int')
                traf.append(df)                
            elif 'qar_Auth_sms_vlr_subs' in file:
                df.rename(columns={'84149330':'Auth_requests','84149331':'Auth_success','84149345':'SMS MO Attempt','84149346':'SMS MO Success',
                        '84149355':'SMS MT Attempt','84149356':'SMS MT Success','84149595':'VLR Local Subs','84154270':'VLR National Roam Subs',
                        '84154271':'VLR International Roam Subs','84154272':'VLR IMSI Attached National Roam Subs','84154273':'VLR IMSI Attached Inter Roam Subs',
                        '84154274':'VLR Total Subscribers','84154275':'VLR IMSI attached Subscribers'},inplace=True)
                df['Site']=df['Object Name'].map(lambda x: x.split('/')[0])
                df['Date']=pd.to_datetime(df['Result Time'],format='%Y-%m-%d %H:%M')
                df=df[['Date','Site','Auth_requests','Auth_success','SMS MO Attempt','SMS MO Success','SMS MT Attempt','SMS MT Success','VLR Local Subs',
                        'VLR National Roam Subs','VLR International Roam Subs','VLR IMSI Attached National Roam Subs','VLR IMSI Attached Inter Roam Subs',
                        'VLR Total Subscribers','VLR IMSI attached Subscribers']]
                auth_sms_vlr.append(df)
            elif 'qar_HO_IntraMSC' in file:
                df['Site']=df['Object Name'].map(lambda x: x.split('/')[0])
                df['HO_type']=df['Object Name'].map(lambda x: x[x.find('HO Type:')+8:])
                df['Date']=pd.to_datetime(df['Result Time'],format='%Y-%m-%d %H:%M')
                df.rename(columns={'84152308':'Intra_MSC_HO_request','84152309':'Intra_MSC_HO_success'},inplace=True)
                df=df[['Date','Site','HO_type','Intra_MSC_HO_request','Intra_MSC_HO_success']]
                ho_intramsc.append(df)
            elif ('qar_MO_CCR' in file) or ('qar_MT_CCR' in file):
                df['Direction']=file[-10:-8]
                df.rename(columns={'84148225':'TwoG Call Attempt','84148235':'TwoG Call Completion','84148237':'TwoG Seized Traffic_Erl',
                            '84148248':'TwoG Call Setup time','84148255':'ThreeG Call Attempt','84148265':'ThreeG Call Completion',
                            '84148267':'ThreeG Seized Traffic_Erl','84148278':'ThreeG Call Setup time','84148505':'TwoG Call Attempt',
                            '84148510':'TwoG Call Completion','84148515':'TwoG Seized Traffic_Erl','84148535':'ThreeG Call Attempt',
                            '84148540':'ThreeG Call Completion','84148545':'ThreeG Seized Traffic_Erl'},inplace=True)
                if file[-10:-8]=='MT':
                    df['ThreeG Call Setup time']=0
                    df['TwoG Call Setup time']=0
                df['Site']=df['Object Name'].map(lambda x: x.split('/')[0])
                df['Date']=pd.to_datetime(df['Result Time'],format='%Y-%m-%d %H:%M')
                df=df[['Date','Site','Direction','TwoG Call Attempt','TwoG Call Completion','TwoG Seized Traffic_Erl','TwoG Call Setup time','ThreeG Call Attempt',
                    'ThreeG Call Completion','ThreeG Seized Traffic_Erl','ThreeG Call Setup time',]]
                mo_mt_ccr.append(df)
            elif 'qar_PAG-LU-Subs_per_LAI' in file:
                df.loc[:,'TwoG_CS Paging SR num']=df['84152188']+df['84152186']
                df.loc[:,'ThreeG_CS Paging SR num']=df['84152190']+df['84152192']
                df.rename(columns={'84151989':'VLR Location Update Requests','84151990':'VLR Location Update Success','84151991':'Roaming Location Update Requests',
                        '84151992':'Roaming Location Update Success','84152185':'TwoG_CS Paging SR den','84152189':'ThreeG_CS Paging SR den',
                        '84162781':'Periodic Location Update Success','84162790':'Periodic Location Update Requests','84166045':'VLR 2G LAI Subs',
                        '84166046':'VLR 3G LAI Subs','84166047':'IMSI Attached 2G LAI Subs','84166048':'IMSI Attached 3G LAI Subs','84166049':'National Roam 2G LAI Subs',
                        '84166050':'National Roam 3G LAI Subs','84166051':'IMSI Attached National Roam 2G LAI Subs','84166052':'IMSI Attached National Roam 3G LAI Subs',
                        '84166053':'International Roam 2G LAI Subs','84166054':'International Roam 3G LAI Subs',
                        '84166055':'IMSI Attached International Roam 2G LAI Subs','84166056':'IMSI Attached International Roam 3G LAI Subs','84176471':'SGs Associated Subs'},inplace=True)
                df['Site']=df['Object Name'].map(lambda x: x.split('/')[0])
                df=df[~df['Object Name'].str.contains('LABEL=MSS')]
                df['LAC']=df['Object Name'].apply(lambda x: int(x[-4:],16))
                df.loc[~df['Object Name'].str.contains('LAI|LABEL=TEST'),'BSC_RNC']=df.loc[~df['Object Name'].str.contains('LAI|LABEL=TEST'),'Object Name']\
                                                                                    .apply(lambda x: x.split('_')[3].split(',')[0])
                df.loc[df['Object Name'].str.contains('LAI|LABEL=TEST'),'BSC_RNC']=None
                df['Date']=pd.to_datetime(df['Result Time'],format='%Y-%m-%d %H:%M')
                df=df[['Date','Site','LAC','BSC_RNC','VLR Location Update Requests','VLR Location Update Success','Roaming Location Update Requests',
                        'Roaming Location Update Success','TwoG_CS Paging SR den','TwoG_CS Paging SR num','ThreeG_CS Paging SR den',
                        'ThreeG_CS Paging SR num','Periodic Location Update Success','Periodic Location Update Requests',
                        'VLR 2G LAI Subs','VLR 3G LAI Subs','IMSI Attached 2G LAI Subs','IMSI Attached 3G LAI Subs','National Roam 2G LAI Subs',
                        'National Roam 3G LAI Subs','IMSI Attached National Roam 2G LAI Subs','IMSI Attached National Roam 3G LAI Subs',
                        'International Roam 2G LAI Subs','International Roam 3G LAI Subs','IMSI Attached International Roam 2G LAI Subs',
                        'IMSI Attached International Roam 3G LAI Subs','SGs Associated Subs']]
                pag_per_lac.append(df)
            elif ('qar_BKC_HSS_KPI' in file) or ('qar_HSS_License_Control' in file) :
                df.rename(columns={'1693800900':'Operation Requests','1693800901':'Positive Responses','1693800902':'Operation Aborted Times',
                                '1693800903':'Operation Timeout Times','1693800925':'System Failure times','1693800936':'Or Operation Not Allowed',
                                '1693809870':'Number of subs using the service','1693809871':'Number of authorized subs'},inplace=True)
                df['Site']=df['Object Name'].map(lambda x: x.split('/')[0])
                df['Service_id_License_control_item']=df['Object Name'].apply(lambda x: x[x.find(':')+1:])
                df['Date']=pd.to_datetime(df['Result Time'],format='%Y-%m-%d %H:%M')
                if 'BKC_HSS_KPI' in file: 
                    df['Type']='Service'
                    df[['Number of subs using the service','Number of authorized subs']]=0
                else: 
                    df['Type']='License_control'
                    df[['Operation Requests','Positive Responses','Operation Aborted Times',
                                'Operation Timeout Times','System Failure times','Or Operation Not Allowed']]=0
                df=df[['Date','Site','Type','Service_id_License_control_item','Operation Requests','Positive Responses','Operation Aborted Times',
                        'Operation Timeout Times','System Failure times','Or Operation Not Allowed','Number of subs using the service','Number of authorized subs']]
                hss_kpi.append(df)
            elif ('qar_HSS_CPU' in file) or ('qar_HSS_Memory' in file):
                df.rename(columns={'1693801001':'Max CPU Load','1693801003':'Duratin for peak CPU load_seconds','1693990014':'Mean CPU Load',
                                    '1693821023':'Total Memory','1693821024':'Used Memory'},inplace=True)
                df['Slot']=df['Object Name'].apply(lambda x: x[x.find('Slot=')+5:x.rfind(',')])
                df['Site']=df['Object Name'].map(lambda x: x.split('/')[0])
                df['Date']=pd.to_datetime(df['Result Time'],format='%Y-%m-%d %H:%M')
                if 'CPU' in file: 
                    df['Type']='CPU'
                    df[['Total Memory','Used Memory']]=0
                else: 
                    df['Type']='Memory'
                    df[['Max CPU Load','Duratin for peak CPU load_seconds','Mean CPU Load']]=0
                df=df[['Date','Site','Slot','Max CPU Load','Duratin for peak CPU load_seconds','Mean CPU Load','Total Memory','Used Memory']]
                hss_cpu_mem.append(df)
            elif 'qar_HSS_Subs' in file:
                df.rename(columns={'1683000480':'Total Subs','1683000481':'ThreeG Subs','1683000482':'TwoG Subs','1683000483':'Authentication messages',
                                    '1683000592':'USIM Auth records','1683000593':'Sim Auth records','1683000594':'Multi_number user group',
                                    '1683000595':'Enhanced multi_number user group','1683000596':'Enhanced multi_number subs','1683000597':'Number of 2_3_4G subs'},inplace=True)        
                df['Site']=df['Object Name'].map(lambda x: x.split('/')[0])
                df['Date']=pd.to_datetime(df['Result Time'],format='%Y-%m-%d %H:%M')
                df=df[['Date','Site','Total Subs','ThreeG Subs','TwoG Subs','Authentication messages','USIM Auth records','Sim Auth records',
                        'Multi_number user group','Enhanced multi_number user group','Enhanced multi_number subs','Number of 2_3_4G subs']]
                hss_subs.append(df)
            elif ('qar_IN_dial' in file) or ('qar_IN_fail' in file):
                df.rename(columns={'84149715':'SSF Tx Dialog','84149716':'SSF Rx Dialog','84149717':'SSF Tx Dialog Abort',
                                '84149718':'SSF Rx Dialog Abort','84149719':'Total Dialog time_seconds','84149721':'Total Dialogs',
                                '84149722':'Average Dialog time_seconds','84152786':'Call Reject due to Overload','84152787':'Call Failures Before InitialDP',
                                '84152788':'Call Failures After InitialDP','84152789':'REJECT Primitives Received by SSF Modules'},inplace=True)
                df['Site']=df['Object Name'].map(lambda x: x.split('/')[0])
                df['Date']=pd.to_datetime(df['Result Time'],format='%Y-%m-%d %H:%M')
                if 'fail' in file:
                    df['SRVKEY']=df['Object Name'].apply(lambda x: x[x.find('=')+1:])
                    df[['SSF Tx Dialog','SSF Rx Dialog','SSF Tx Dialog Abort','SSF Rx Dialog Abort','Total Dialog time_seconds',
                        'Total Dialogs','Average Dialog time_seconds']]=0
                else:
                    df['SRVKEY']=None
                    df[['Call Reject due to Overload','Call Failures Before InitialDP','Call Failures After InitialDP','REJECT Primitives Received by SSF Modules']]=0
                df=df[['Date','Site','SRVKEY','SSF Tx Dialog','SSF Rx Dialog','SSF Tx Dialog Abort','SSF Rx Dialog Abort',
                        'Total Dialog time_seconds','Total Dialogs','Average Dialog time_seconds','Call Reject due to Overload','Call Failures Before InitialDP',
                        'Call Failures After InitialDP','REJECT Primitives Received by SSF Modules']]
                in_kpi.append(df)
            elif 'qar_Interconnect_SL' in file:
                df['Rx Bandwith byte']=df['84151164']/df['84151173']/100
                df['Tx Bandwith byte']=df['84151163']/df['84151174']/100
                df.rename(columns={'84151149':'Fault times','84151150':'Fault duration_sec','84151161':'Tx messages','84151162':'Rx messages',
                        '84151163':'Tx byte','84151164':'Rx byte','84151167':'Congestion times','84151168':'Congestion duration_sec',
                        '84158088':'ISUP Tx messages','84158089':'ISUP Rx messages','84158090':'ISUP Tx byte','84158091':'ISUP Rx byte'},inplace=True)
                df['Site']=df['Object Name'].map(lambda x: x.split('/')[0])
                df['Date']=pd.to_datetime(df['Result Time'],format='%Y-%m-%d %H:%M')
                df['MTP_Link']=df['Object Name'].apply(lambda x: x[x.find('LABEL=')+6:])
                df=df[['Date','Site','MTP_Link','Fault times','Fault duration_sec','Tx messages','Rx messages','Tx byte','Rx byte','Congestion times',
                'Congestion duration_sec','ISUP Tx messages','ISUP Rx messages','ISUP Tx byte','ISUP Rx byte','Rx Bandwith byte','Tx Bandwith byte']]
                interconnect.append(df)
            elif 'qar_MGW_M3UA_SL' in file:
                df.rename(columns={'100991178':'Congestion duration_sec','100991179':'Link disconnect time','100991180':'Tx ASPM messages',
                            '100991181':'Tx ASPM byte','100991182':'Tx MGMT messages','100991183':'Link unavailable time','100991184':'Tx MGMT byte',
                            '100991185':'Link unavailable duration_sec','100991186':'Rx messages','100991187':'Rx byte','100991188':'Tx SSNM messages',
                            '100991189':'Tx SSNM byte','100991190':'Tx Data messages','100991191':'Tx Data byte','100991192':'Tx messages',
                            '100991193':'Tx byte','100991194':'Rx Data messages','100991195':'Rx Data byte','100991196':'Rx ASPM messages','100991197':'Rx ASPM byte',
                            '100991198':'Rx SSNM messages','100991199':'Rx SSNM byte','100991200':'Rx MGMT messages','100991201':'Rx MGMT byte',
                            '100991202':'Link Congestion times','100991395':'Tx Link Bandwith_byte_sec','100991396':'Rx Link Bandwith_byte_sec'},inplace=True)
                df['Site']=df['Object Name'].map(lambda x: x.split('/')[0])
                df['Date']=pd.to_datetime(df['Result Time'],format='%Y-%m-%d %H:%M')
                df['M3UA_Link']=df['Object Name'].apply(lambda x: x[x.find('Label=')+6:])
                df=df[['Date','Site','M3UA_Link','Congestion duration_sec','Link disconnect time','Tx ASPM messages','Tx ASPM byte','Tx MGMT messages',
                    'Link unavailable time','Tx MGMT byte','Link unavailable duration_sec','Rx messages','Rx byte','Tx SSNM messages',
                    'Tx SSNM byte','Tx Data messages','Tx Data byte','Tx messages','Tx byte','Rx Data messages','Rx Data byte','Rx ASPM messages','Rx ASPM byte',
                    'Rx SSNM messages','Rx SSNM byte','Rx MGMT messages','Rx MGMT byte','Link Congestion times','Tx Link Bandwith_byte_sec','Rx Link Bandwith_byte_sec']]
                mgw_mua.append(df)
            elif ('qar_AB_int_inc' in file) or ('qar_AB_int_out' in file):
                df.rename(columns={'84150724':'Installed circuits','84150725':'Available circuits','84150726':'Blocked circuits','84150737':'Seizure Traffic_erl',
                            '84150888':'Total Call Duration_sec','84150779':'Installed circuits','84150780':'Available circuits','84150781':'Blocked circuits',
                            '84150793':'Seizure Traffic_erl','84150874':'Total Call Duration_sec'},inplace=True)
                df['Site']=df['Object Name'].map(lambda x: x.split('/')[0])
                df[['Trunk_group','TG']]=df['Object Name'].apply(lambda x: x[x.find('LABEL=')+6:]).str.split(',',expand=True)
                df['TG']=df['TG'].replace({'TG=':''},regex=True)
                df['Direction']=file[-7:-4]
                df['Date']=pd.to_datetime(df['Result Time'],format='%Y-%m-%d %H:%M')
                df=df[['Date','Site','Direction','Trunk_group','TG','Installed circuits','Available circuits','Blocked circuits','Seizure Traffic_erl',
                        'Total Call Duration_sec']]
                ab_int.append(df)
            elif ('qar_AB_CC_vtraffic' in file) or ('qar_BSC_RNC_vtraffic_CSFB_delay' in file):
                df.rename(columns={'84154650':'Incoming seizure traf_erl','84154695':'Outgoing seizure traf_erl','84163324':'Outgoing installed circuits',
                            '84163325':'Outgoing available circuits','84163326':'Outgoing blocked circuits','84163338':'Outgoing seizure traf_erl',
                            '84164787':'Terminated CSFB Call setup duration_sec','100653174':'Inc_out and HO seizure traffic_erl'},inplace=True)
                df['Site']=df['Object Name'].map(lambda x: x.split('/')[0])
                df[['Office','Office_dir']]=df['Object Name'].apply(lambda x: x[x.find('LABEL=')+6:]).str.split(',',expand=True)
                df['Office_dir']=df['Office_dir'].replace({'OFFICEDIR=':''},regex=True)
                if 'AB_CC' in file: 
                    df['NE']='AB_CC'
                    df[['Outgoing installed circuits','Outgoing available circuits','Outgoing blocked circuits','Terminated CSFB Call setup duration_sec',
                    'Inc_out and HO seizure traffic_erl']]=0
                else: 
                    df['NE']='BSC_RNC'
                    df['Incoming seizure traf_erl']=0
                df['Date']=pd.to_datetime(df['Result Time'],format='%Y-%m-%d %H:%M')
                df=df[['Date','Site','NE','Office','Office_dir','Incoming seizure traf_erl','Outgoing seizure traf_erl','Outgoing installed circuits',
                    'Outgoing available circuits','Outgoing blocked circuits','Terminated CSFB Call setup duration_sec',
                    'Inc_out and HO seizure traffic_erl']]
                ab_vtraff.append(df)
            elif 'qar_ats_basic' in file:
                df.rename(columns={'478154827':'MO Attempts','478154828':'MO Connected','478154829':'MO Answered','478154830':'MT Attempts','478154831':'MT Connected',
                        '478154832':'MT Answered','478154839':'Mean duration of MO seized sessions_sec','478154849':'Mean duration of MT seized sessions_sec',
                        '478154861':'Mean duration of connected MO session_ms','478154862':'Mean duration of connected MT session_ms',
                        '478155383':'Peak num of ATS simultaneous sessions','478155389':'BHCA per Subscriber','478157343':'ATS VoLTE registered subs'},inplace=True)
                df['Date']=pd.to_datetime(df['Result Time'],format='%Y-%m-%d %H:%M')
                df['Site']=df['Object Name'].map(lambda x: x.split('/')[0])
                df=df[['Date','Site','MO Attempts','MO Connected','MO Answered','MT Attempts','MT Connected','MT Answered','Mean duration of MO seized sessions_sec',
                        'Mean duration of MT seized sessions_sec','Mean duration of connected MO session_ms','Mean duration of connected MT session_ms',
                        'Peak num of ATS simultaneous sessions','BHCA per Subscriber','ATS VoLTE registered subs']]
                ats_basics.append(df)
            elif 'qar_abcf_sess' in file:
                df.rename(columns={'1912981070':'MO Attempts','1912981071':'MO Connected','1912981072':'MO Answered','1912981073':'MT Attempts',
                            '1912981074':'MT Connected','1912981075':'MT Answered','1912981104':'Mean duration of ABCF MO sessions_sec',
                            '1912981105':'Mean duration of ABCF MT sessions_sec','1912981109':'Connected ABCF Traf_erl','1912981112':'Answered ABCF Traf_erl',
                            '1912981143':'MO Interruptions','1912981144':'MT Interruptions'},inplace=True)
                df['Date']=pd.to_datetime(df['Result Time'],format='%Y-%m-%d %H:%M')
                df['Site']=df['Object Name'].map(lambda x: x.split('/')[0])
                df=df[['Date','Site','MO Attempts','MO Connected','MO Answered','MT Attempts','MT Connected','MT Answered','Mean duration of ABCF MO sessions_sec',
                            'Mean duration of ABCF MT sessions_sec','Connected ABCF Traf_erl','Answered ABCF Traf_erl','MO Interruptions','MT Interruptions']]
                abcf_basics.append(df)
            elif ('qar_scscf_access' in file) or ('qar_scscf_sess' in file):
                if 'access' in file:
                    df['Type']='access'
                    df[['Refer message','Refer success','Publish message','Publish success','MO Answered','MT Answered','MO Call Drop',
                        'MT Call Drop','Subscribe Requests for Registration den','Subscribe Requests for Registration num',
                        'Notify Messages for Reg den','Notify Messages for Reg num','Third_party deregistration attempt','Third_party deregistration success',
                        'Third_party registration success','Third_party registration unsuccess','Authentication attempt','Authentication success']]=0
                    df['Identifier']=df['Object Name'].apply(lambda x: x[x.find('Access type=')+12:])
                else:
                    df['Type']='sess'
                    df[['Initial register attempt','Initial register success','Re-registration attempt','Re-registration success',
                        'Deregistration attempt','Deregistration success']]=0
                    df['Identifier']=df['Object Name'].apply(lambda x: x[x.find('ID=')+3:])
                    df.loc[:,'Notify Messages for Reg den']=df['335664432']+df['335664436']+df['335664440']
                    df.loc[:,'Notify Messages for Reg num']=df['335664433']+df['335664437']+df['335664441']
                    df.loc[:,'Subscribe Requests for Registration den']=df['335664430']+df['335664434']+df['335664438']
                    df.loc[:,'Subscribe Requests for Registration num']=df['335664439']+df['335664435']+df['335664431']

                df.rename(columns={'335655507':'Refer message','335655508':'Refer success','335655511':'Publish message','335655512':'Publish success',
                        '335655578':'MO Answered','335655583':'MT Answered','335663440':'MO Call Drop','335663441':'MT Call Drop',
                        '335664450':'Third_party deregistration attempt','335664451':'Third_party deregistration success',
                        '335664456':'Third_party registration success','335664457':'Third_party registration unsuccess',
                        '335672443':'Authentication attempt','335672444':'Authentication success','335672721':'Initial register attempt',
                        '335672722':'Initial register success','335672724':'Re-registration attempt','335672725':'Re-registration success',
                        '335672727':'Deregistration attempt','335672728':'Deregistration success'},inplace=True)
                
                df['Date']=pd.to_datetime(df['Result Time'],format='%Y-%m-%d %H:%M')
                df['Site']=df['Object Name'].map(lambda x: x.split('/')[0])
                df=df[['Date','Site','Type','Identifier','Refer message','Refer success','Publish message','Publish success','MO Answered','MT Answered',
                        'MO Call Drop','MT Call Drop','Subscribe Requests for Registration den','Subscribe Requests for Registration num',
                        'Notify Messages for Reg den','Notify Messages for Reg num','Third_party deregistration attempt','Third_party deregistration success',
                        'Third_party registration success','Third_party registration unsuccess','Authentication attempt','Authentication success',
                        'Initial register attempt','Initial register success','Re-registration attempt','Re-registration success',
                        'Deregistration attempt','Deregistration success']]
                scsf.append(df)
            elif 'qar_2g3g_esrvcc' in file:
                df.loc[:,'LTE_to_GSM SRVCC den']=df['84164965']+df['84164967']
                df.loc[:,'LTE_to_GSM SRVCC num']=df['84175689']+df['84176435']+df['84176698']
                df.loc[:,'LTE_to_UMTS SRVCC den']=df['84164975']+df['84164977']
                df.loc[:,'LTE_to_UMTS SRVCC num']=df['84175691']+df['84176438']+df['84176700']
                df['Date']=pd.to_datetime(df['Result Time'],format='%Y-%m-%d %H:%M')
                df['Site']=df['Object Name'].map(lambda x: x.split('/')[0])
                df['Peer_entity']=df['Object Name'].apply(lambda x: x[x.find('name=')+5:])
                df=df[['Date','Site','Peer_entity','LTE_to_GSM SRVCC den','LTE_to_GSM SRVCC num','LTE_to_UMTS SRVCC den','LTE_to_UMTS SRVCC num']]
                srvcc.append(df)
            elif 'qar_volte_ims_USNs' in file:
                df.loc[:,'X2 Handover den']=df['117499812']+df['117499813']
                df.loc[:,'X2 Handover num']=df['117499814']+df['117499815']
                df.loc[:,'Intra S1 Handover den']=df['117499816']+df['117499817']
                df.loc[:,'Intra S1 Handover num']=df['117499818']+df['117499819']
                df.loc[:,'Inter S1 Handover den']=df['117499820']+df['117499821']
                df.loc[:,'Inter S1 Handover num']=df['117499822']+df['117499823']
                df.loc[:,'IMS_Voice_dr_num']=df['117499842']-df['117499843']-df['117499844']-df['117499845']-df['117499846']
                df.loc[:,'IMS_Video_dr_num']=df['117499868']-df['117499869']-df['117499870']-df['117499871']-df['117499872']
                df.rename(columns={'117499712':'PDN Connect request','117499713':'PDN Connect success','117499752':'Voice bearer request',
                        '117499753':'Voice bearer success','117499774':'Maximum number of PDN connections','117499837':'Video bearer success',
                        '117499842':'Voice bearer deactivation','117499843':'Voice bearer deactivation trigger by enodeb',
                        '117499844':'Voice bearer deactivation trigger by ue','117499845':'Voice bearer deactivation trigger by sgw_pgw',
                        '117499846':'Voice bearer deactivation trigger erab update failure','117499853':'Voice bearer request over S11',
                        '117499854':'Voice bearer success over S11','117499868':'Video bearer deactivation','117499869':'Video bearer deactivation trigger by enodeb',
                        '117499870':'Video bearer deactivation trigger by ue','117499871':'Video bearer deactivation trigger by sgw_pgw',
                        '117499872':'Video bearer deactivation trigger erab update failure'},inplace=True)
                df['Date']=pd.to_datetime(df['Result Time'],format='%Y-%m-%d %H:%M')
                df['Site']=df['Object Name'].map(lambda x: x.split('/')[0])
                df=df[['Date','Site','PDN Connect request','PDN Connect success','Voice bearer request','Voice bearer success',
                    'Maximum number of PDN connections','Video bearer success','Voice bearer deactivation','Voice bearer deactivation trigger by enodeb',
                    'Voice bearer deactivation trigger by ue','Voice bearer deactivation trigger by sgw_pgw','Voice bearer deactivation trigger erab update failure',
                    'Voice bearer request over S11','Voice bearer success over S11','Video bearer deactivation','Video bearer deactivation trigger by enodeb',
                    'Video bearer deactivation trigger by ue','Video bearer deactivation trigger by sgw_pgw','Video bearer deactivation trigger erab update failure',
                    'X2 Handover den','X2 Handover num','Intra S1 Handover den','Intra S1 Handover num','Inter S1 Handover den','Inter S1 Handover num',
                    'IMS_Voice_dr_num','IMS_Video_dr_num']]
                ims_usn.append(df)
            elif ('qar_2GPag_Uv' in file) or ('qar_3GPag_Uv' in file) or ('qar_4GPag_U' in file) or ('qar_4GPag_vU' in file):
                df.rename(columns={'117456013':'Packet Paging request','117456017':'Packet Paging response','117456114':'Max attached subs',
                            '117458313':'Packet Paging request','117458317':'Packet Paging response','117458414':'Max attached subs',
                            '117496912':'Packet Paging request','117496913':'Packet Paging response','117497113':'Max attached subs',
                            '117494562':'Packet Paging request','117494563':'Packet Paging response','117494567':'Max attached subs'},inplace=True)
                if '2G' in file:
                    df['mode']='Gb mode'
                    df['RAC']=df['Object Name'].map(lambda x: x[-2:])
                elif '3G' in file:
                    df['mode']='Iu mode'
                    df['RAC']=df['Object Name'].map(lambda x: x[-2:])
                elif '4G' in file:
                    df['mode']='S1 mode'
                    df['RAC']=None
                df['Date']=pd.to_datetime(df['Result Time'],format='%Y-%m-%d %H:%M')
                df['Site']=df['Object Name'].map(lambda x: x.split('/')[0])
                df['LAC']=df['Object Name'].map(lambda x: int(x[x.find('4000')+5:x.find('4000')+9],16))
                df=df[['Date','Site','mode','LAC','RAC','Packet Paging request','Packet Paging response','Max attached subs']]
                df['LAC']=df['LAC'].astype('str')
                df.iloc[:,5:]=df.iloc[:,5:].astype('int')
                ps_pag.append(df)
            elif 'qar_USNs_PLMN_Att_PDP_user' in file:
                df.rename(columns={'117470614':'Gb mode max attached user','117470714':'Iu mode max attached user',
                        '117476223':'Iu mode max user with pdp activated context','117476253':'Gb mode max user with pdp activated context',
                        '117493314':'S1 mode max attached user','117493817':'S1 mode max PDN connection num'},inplace=True)
                df['Date']=pd.to_datetime(df['Result Time'],format='%Y-%m-%d %H:%M')
                df['Site']=df['Object Name'].map(lambda x: x.split('/')[0])
                df['Mobile_country_code']=df['Object Name'].apply(lambda x: x.replace(' ','')[x.replace(' ','').find('code=')+5:x.replace(' ','').find('code=')+8])
                df['Mobile_network_code']=df['Object Name'].apply(lambda x: x[x.rfind('=')+1:])
                df=df[['Date','Site','Mobile_country_code','Mobile_network_code','Gb mode max attached user','Iu mode max attached user',
                        'Iu mode max user with pdp activated context','Gb mode max user with pdp activated context',
                        'S1 mode max attached user','S1 mode max PDN connection num']]
                usn_pdp_plmn.append(df)
            elif 'qar_CSFB_paging' in file:
                df.rename(columns={'84175965':'CSFB First paging request','84175967':'CSFB Second paging request','84175969':'CSFB Third paging request',
                                    '84175983':'Paging response'},inplace=True)
                df['Date']=pd.to_datetime(df['Result Time'],format='%Y-%m-%d %H:%M')
                df['Site']=df['Object Name'].map(lambda x: x.split('/')[0])
                df['Entity_name']=df['Object Name'].apply(lambda x: x[x.find('name=')+5:])
                df=df[['Date','Site','Entity_name','CSFB First paging request','CSFB Second paging request','CSFB Third paging request','Paging response']]
                csfb_pag.append(df)
            elif 'qar_UGW_PDP_Act' in file:
                df.rename(columns={'134686590':'PDP request','134686591':'PDP success'},inplace=True)
                df['Date']=pd.to_datetime(df['Result Time'],format='%Y-%m-%d %H:%M')
                df['Site']=df['Object Name'].map(lambda x: x.split('/')[0])
                df['gpname_id']=None
                df[['DL_packets','UL_packets','DL_Drop_packets','UL_Drop_packets']]=0
                df=df[['Date','Site','gpname_id','PDP request','PDP success','DL_packets','UL_packets','DL_Drop_packets','UL_Drop_packets']]
                df.iloc[:,3:]=df.iloc[:,3:].astype('float')
                ugw_pdp.append(df)
            elif 'qar_USNs_KPIs' in file:
                df_l=pd.read_csv(glob.glob('/home/ismayil/flask_dash/data/core/'+file[:7]+'*'+file[file.find('_qar')-12:file.find('_qar')+4]+'_USNs_LTE_KPIs.csv')[0],skiprows=[1])
                df=df.merge(df_l.drop(columns=['Granularity Period','Reliability']),on=['Result Time','Object Name'])
                try:
                    df_user=pd.read_csv(glob.glob('/home/ismayil/flask_dash/data/core/'+file[:7]+'*'+file[file.find('_qar')-12:file.find('_qar')+4]+'_USN_attach_users.csv')[0],skiprows=[1])
                    df=df.merge(df_user.drop(columns=['Granularity Period','Reliability']),on=['Result Time','Object Name'])
                except Exception as nese:
                    print(nese,' on usn user kpi')
                    df[['117455021','117457219','117492014']]=0
                    1
                df.loc[:,'S1 combined attach success']=df['117491128']+df['117491129']
                df.loc[:,'Intra_MME TAU request']=df['117491312']+df['117491327']+df['117491342']+df['117491357']
                df.loc[:,'Intra_MME TAU success']=df['117491313']+df['117491328']+df['117491343']+df['117491358']
                df.loc[:,'Inter_MME TAU request']=df['117491412']+df['117491427']
                df.loc[:,'Inter_MME TAU success']=df['117491413']+df['117491428']
                df.loc[:,'Intra_MME X2 HO req']=df['117491512']+df['117491514']
                df.loc[:,'Intra_MME X2 HO suc']=df['117491513']+df['117491515']
                df.loc[:,'Intra_MME S1 HO req']=df['117491516']+df['117491518']
                df.loc[:,'Intra_MME S1 HO suc']=df['117491517']+df['117491519']
                df.loc[:,'Inter_MME HO req']=df['117491612']+df['117491614']
                df.loc[:,'Inter_MME HO suc']=df['117491613']+df['117491615']
                df.loc[:,'paging_non_respons']=df['117491812']-df['117491813']
                df1=df[['Result Time','Object Name','117454513', '117454514', '117454713', '117454714', '117454813','117454814',
                        '117455116', '117455117','117458513', '117458514','117454925','117454926','117454927','117454928','117455021']]
                df1.loc[:,'Total Auth request']=df1['117454925']+df1['117454927']
                df1.loc[:,'Total Auth success']=df1['117454926']+df1['117454928']
                df1[['Intra_MME X2 HO req','Intra_MME X2 HO suc','Intra_MME S1 HO req','Intra_MME S1 HO suc','Inter_MME HO req','Inter_MME HO suc']]=0
                df1.loc[:,'mode']='Gb mode'
                df1.columns=['Result Time','Object Name','Data attach','Data accept','Intra_SGSN_MME RAU request','Intra_SGSN_MME RAU success',
                    'Inter_SGSN_MME RAU request','Inter_SGSN_MME RAU success','packet paging request','packet paging none_response',
                    'MS init PDP_bear context act','MS init PDP_bear context act suc','SIM Auth request','SIM Auth success','USIM Auth request',
                    'USIM Auth success','Max attach user','Total Auth request','Total Auth success','Intra_MME X2 HO req','Intra_MME X2 HO suc',
                    'Intra_MME S1 HO req','Intra_MME S1 HO suc','Inter_MME HO req','Inter_MME HO suc','mode']
                df2=df[['Result Time','Object Name','117456613', '117456614','117456813', '117456814', '117456913', '117456914', 
                        '117457114','117457115','117459413', '117459414','117457025','117457026','117457027','117457028','117457219']]
                df2.loc[:,'Total Auth request']=df2['117457025']+df2['117457027']
                df2.loc[:,'Total Auth success']=df2['117457026']+df2['117457028']
                df2[['Intra_MME X2 HO req','Intra_MME X2 HO suc','Intra_MME S1 HO req','Intra_MME S1 HO suc','Inter_MME HO req','Inter_MME HO suc']]=0
                df2.loc[:,'mode']='Iu mode'
                df2.columns=df1.columns
                df3=df[['Result Time','Object Name','117491127','S1 combined attach success','Intra_MME TAU request','Intra_MME TAU success',
                    'Inter_MME TAU request','Inter_MME TAU success','117491812','paging_non_respons','117495952','117495953','117491712','117491713']]
                df3.insert(12,'Max attach user',df['117492014'].values)
                df3.insert(12,'USIM Auth success',0)
                df3.insert(12,'SIM Auth success',0)
                df3.insert(12,'USIM Auth request',0)
                df3.insert(12,'SIM Auth request',0)
                df3[['Intra_MME X2 HO req','Intra_MME X2 HO suc','Intra_MME S1 HO req','Intra_MME S1 HO suc','Inter_MME HO req','Inter_MME HO suc']]=\
                        df[['Intra_MME X2 HO req','Intra_MME X2 HO suc','Intra_MME S1 HO req','Intra_MME S1 HO suc','Inter_MME HO req','Inter_MME HO suc']].values
                df3.loc[:,'mode']='S1 mode'
                df3.columns=df1.columns
                df=pd.concat([df1,df2,df3])
                df['vm_id']=None
                df[['Max_home_subs','Max_visiting_subs']]=0
                df['Date']=pd.to_datetime(df['Result Time'],format='%Y-%m-%d %H:%M')
                df['Site']=df['Object Name'].map(lambda x: x.split('/')[0])
                #df.insert('')
                df=df[['Date','Site','mode','vm_id','Data attach','Data accept','Intra_SGSN_MME RAU request','Intra_SGSN_MME RAU success',
                    'Inter_SGSN_MME RAU request','Inter_SGSN_MME RAU success','packet paging request','packet paging none_response',
                    'MS init PDP_bear context act','MS init PDP_bear context act suc','SIM Auth request','SIM Auth success','USIM Auth request',
                    'USIM Auth success','Max attach user','Max_home_subs','Max_visiting_subs','Total Auth request','Total Auth success','Intra_MME X2 HO req',
                    'Intra_MME X2 HO suc','Intra_MME S1 HO req','Intra_MME S1 HO suc','Inter_MME HO req','Inter_MME HO suc']]
                df.iloc[:,4:]=df.iloc[:,4:].astype('float')
                usn_kpi.append(df)
            elif ('qar_Inter_out_SIP' in file) or ('qar_Inter_inc_SIP' in file):
                tg_mapping=pd.read_excel('/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/Core QA/Mapping for schedule/mapping.xlsx',sheet_name='Interconnect Route')
                df['Date']=pd.to_datetime(df['Result Time'],format='%Y-%m-%d %H:%M')
                df['Direction']=file[-11:-8]
                df['Site']=df['Object Name'].map(lambda x: x.split('/')[0])
                df['TG']=df['Object Name'].map(lambda x: x.split('/')[1].split(':')[1])
                df=df.merge(tg_mapping[['TG','Description']],on='TG')
                df.rename(columns={'1912981991':'Number_of_conn_sessions_IBCF',
                                '1912982008':'Number_of_conn_sessions_IBCF',
                                '1912981992':'Traffic_of_sessions_connected_IBCF',
                                '1912982009':'Traffic_of_sessions_connected_IBCF',
                                '1912981995':'Mean_seiz_duration_of_sessions',
                                '1912982012':'Mean_seiz_duration_of_sessions',
                                '1912981996':'Originated_session_attempts_IBCF',
                                '1912982013':'Originated_session_attempts_IBCF',
                                '1912981998':'Answered_sessions_IBCF',
                                '1912982015':'Answered_sessions_IBCF',
                                '1912981999':'Traffic_of_sessions_answered_IBCF',
                                '1912982016':'Traffic_of_sessions_answered_IBCF',
                                '1912982001':'Traffic_of_sessions_seized',
                                '1912982018':'Traffic_of_sessions_seized',
                                '1912982003':'Call_release_success',
                                '1912982020':'Call_release_success',
                                '1912982004':'Call_release_failures',
                                '1912982021':'Call_release_failures','Description':'Destination'},inplace=True)
                df=df[['Date','Site','Direction','TG','Destination','Traffic_of_sessions_seized','Traffic_of_sessions_answered_IBCF','Traffic_of_sessions_connected_IBCF',
                        'Number_of_conn_sessions_IBCF','Originated_session_attempts_IBCF','Answered_sessions_IBCF','Mean_seiz_duration_of_sessions','Call_release_success',
                        'Call_release_failures']]
                df.iloc[:,5:]=df.iloc[:,5:].astype(float)
                int_inc_out_sip.append(df)
            to_save.append(file)
    except Exception as e:
        print(e)
        continue

print('append finished from huawei core')
for i in ['traf','auth_sms_vlr','ho_intramsc','mo_mt_ccr','pag_per_lac','hss_kpi','hss_cpu_mem','hss_subs','in_kpi','interconnect',
        'mgw_mua','ab_int','ab_vtraff','ats_basics','abcf_basics','scsf','srvcc','ims_usn','ps_pag','usn_pdp_plmn','csfb_pag','ugw_pdp','usn_kpi','int_inc_out_sip']:
    try:
        if len(i)>0:
            concated= pd.concat(eval(i)).reset_index(drop=True)
            #concated.to_csv('/home/ismayil/flask_dash/data/core2/'+i+'.csv')
            for j in concated['Date'].unique():
                try:
                    #file_name = datetime.datetime.strftime(datetime.datetime.utcfromtimestamp(j.astype(datetime.datetime) / 1e9),"%B_%Y")
                    file_name2 = datetime.datetime.strftime(datetime.datetime.utcfromtimestamp(j.astype(datetime.datetime) / 1e9),"%Y-%m-%d")
                    concated.loc[concated['Date']==j].to_hdf('/disk2/support_files/archive/core/core_new_'+file_name2+'.h5',i,append=True,
                            format='table', data_columns=['Date',*mapping[i]], min_itemsize={j:100 for j in mapping[i]},complevel=5)
                    
                except Exception as u:
                    print(u, ' exception raised ',i)
                    continue
    except Exception as e: 
        print(e)
        print('error from tradditional part ',i)
        continue
print(' pdp part finished.')
pd.DataFrame(to_save).to_csv(os.path.join('/home/ismayil/flask_dash/data', 'files.csv'), mode='a', header=None, index=False)
print('huawei core save finished')
try:
    import core_inputs_for_bi
    core_inputs_for_bi.run()
except Exception as ll: 
    print(ll,' from core inputs')
    1
try:     
    import cem
    cem.run()
except Exception as e:
    print(e)
    1
try:
    import utilization_hourly
    utilization_hourly.run()
except Exception as e:
    print(e)
    1
