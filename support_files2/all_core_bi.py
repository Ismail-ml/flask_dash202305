import pandas as pd
import atoti as tt
#
df=pd.read_csv('nedddd.csv')
df['Date']=pd.to_datetime(df['Date'])
a=['Site', 'HO_type', 'Entity_name', 'Direction', 'Type', 'Identifier',
       'gpname_id', 'mode', 'vm_id', 'Mobile_country_code',
       'Mobile_network_code', 'MNO', 'APN', 'LAC', 'BSC_RNC', 'RAC']
#
df[a]=df[a].fillna('All')
df[['LAC','RAC']]=df[['LAC','RAC']].replace({'All':100}).astype(int)
df['Day']=df['Date'].dt.date
df['Month']=df['Date'].dt.month
df['Year']=df['Date'].dt.year.astype(str)
df['Hour']=df['Date'].dt.hour
#
df.columns=df.columns.map(lambda x: x.replace('/','_'))
#
session=tt.Session(port=55707)
#
a.append('Date')
core = session.read_pandas(df,table_name='Core_KPIs',keys=a)
#
cube=session.create_cube(core,mode='no_measures')
#cube2=session.create_cube(core)
h,l,m = cube.hierarchies, cube.levels, cube.measures
core['Month'].default_value=1
core['Hour'].default_value=22
h['Month']=[core['Month']]
h['Year']=[core['Year']]
h['Hour']=[core['Hour']]
#l['LAC']=[core['LAC']]
#h['Date_hierarchy']=[three['Year'],three['Month'],three['Day']]
cube.create_date_hierarchy("Date_hierarchy",column=core['Date'],levels={'Year':'yyyy',"Month": "MM",'Day':'dd'})
#
def add_measures():
    try:
        m['2G_Call_Completion_SR']=tt.agg.sum(core['TwoG Call Completion'])/tt.agg.sum(core['TwoG Call Attempt'])*100
        m['3G_Call_Completion_SR']=tt.agg.sum(core['ThreeG Call Completion'])/tt.agg.sum(core['ThreeG Call Attempt'])*100
        m['2G_3G_Call_Completion_SR']=(tt.agg.sum(core['TwoG Call Completion'])+tt.agg.sum(core['ThreeG Call Completion']))/\
                                        (tt.agg.sum(core['TwoG Call Attempt'])+tt.agg.sum(core['ThreeG Call Attempt']))*100
    #    
        m['2G_Call_Setup_Time_sec']=tt.agg.sum(core['TwoG Call Setup time'])/tt.agg.sum(core['TwoG Call Completion'])
        m['3G_Call_Setup_Time_sec']=tt.agg.sum(core['ThreeG Call Setup time'])/tt.agg.sum(core['ThreeG Call Completion'])
        m['2G_3G_Call_Setup_Time_sec']=(tt.agg.sum(core['TwoG Call Setup time'])+tt.agg.sum(core['ThreeG Call Setup time']))/\
                                            (tt.agg.sum(core['TwoG Call Completion'])+tt.agg.sum(core['ThreeG Call Completion']))
    #   
        m['2G_pag_att']=tt.agg.sum(core['TwoG_CS Paging SR den'])
        m['2G_pag_suc']=tt.agg.sum(core['TwoG_CS Paging SR num'])
        m['3G_pag_att']=tt.agg.sum(core['ThreeG_CS Paging SR den'])
        m['3G_pag_suc']=tt.agg.sum(core['ThreeG_CS Paging SR num'])
        m['2G_CS_Paging_SR']=tt.filter(m['2G_pag_suc'],(l['LAC']!=1505) & (l['LAC']!=100))/tt.filter(m['2G_pag_att'],(l['LAC']!=1505) & (l['LAC']!=100))*100
        m['3G_CS_Paging_SR']=tt.filter(m['3G_pag_suc'],(l['LAC']!=1505) & (l['LAC']!=100))/tt.filter(m['3G_pag_att'],(l['LAC']!=1505) & (l['LAC']!=100))*100
        m['2G_3G_CS_Paging_SR']=(tt.agg.sum(core['TwoG_CS Paging SR num'])+tt.agg.sum(core['ThreeG_CS Paging SR num']))/\
                                (tt.agg.sum(core['TwoG_CS Paging SR den'])+tt.agg.sum(core['ThreeG_CS Paging SR den']))*100
        m['CommonLac_CS_Paging_SR']=(tt.filter(m['2G_pag_suc'],l['LAC']==1505)+tt.filter(m['3G_pag_suc'],l['LAC']==1505))/tt.filter(m['3G_pag_att'],l['LAC']==1505)*100
        #
        m['Location_Update_SR_VLR']=tt.agg.sum(core['VLR Location Update Success'])/tt.agg.sum(core['VLR Location Update Requests'])*100
        m['Location_Update_SR_Roaming']=tt.agg.sum(core['Roaming Location Update Success'])/tt.agg.sum(core['Roaming Location Update Requests'])*100
        m['Location_Update_SR_total']=(tt.agg.sum(core['VLR Location Update Success'])+tt.agg.sum(core['Roaming Location Update Success']))/\
                                        (tt.agg.sum(core['VLR Location Update Requests'])+tt.agg.sum(core['Roaming Location Update Requests']))*100
        #
        m['MO_SMS_SR']=tt.agg.sum(core['SMS MO Success'])/tt.agg.sum(core['SMS MO Attempt'])*100
        m['MT_SMS_SR']=tt.agg.sum(core['SMS MT Success'])/tt.agg.sum(core['SMS MT Attempt'])*100
        m['Authentication_SR']=tt.agg.sum(core['Auth_success'])/tt.agg.sum(core['Auth_requests'])*100
        #
        m['Intra_MSC_HOSR']=tt.agg.sum(core['Intra_MSC_HO_success'])/tt.agg.sum(core['Intra_MSC_HO_request'])*100
        m['CSFB_Paging_SR']=tt.agg.sum(core['Paging response'])/(tt.agg.sum(core['CSFB First paging request'])+tt.agg.sum(core['CSFB Second paging request'])+\
                                                                    tt.agg.sum(core['CSFB Third paging request']))*100
        m['CSFB_1st_Paging_SR']=tt.agg.sum(core['Paging response'])/tt.agg.sum(core['CSFB First paging request'])*100
        for element in [m['2G_Call_Completion_SR'],m['3G_Call_Completion_SR'],m['2G_3G_Call_Completion_SR'],m['2G_Call_Setup_Time_sec'],m['3G_Call_Setup_Time_sec'],
                        m['2G_3G_Call_Setup_Time_sec'],m['2G_CS_Paging_SR'],m['3G_CS_Paging_SR'],m['2G_3G_CS_Paging_SR'],m['CommonLac_CS_Paging_SR'],
                        m['Location_Update_SR_VLR'],m['Location_Update_SR_Roaming'],m['Location_Update_SR_total'],m['MO_SMS_SR'],m['MT_SMS_SR'],
                        m['Authentication_SR'],m['Intra_MSC_HOSR'],m['CSFB_Paging_SR'],m['CSFB_1st_Paging_SR'],m['2G_pag_att'],m['3G_pag_att'],
                        m['2G_pag_suc'],m['3G_pag_suc']]:
                element.folder='Voice KPIs'
        m['Data_Attach_SR']=tt.agg.sum(core['Data accept'])/tt.agg.sum(core['Data attach'])*100
        m['Inter_SGSN_MME_RAU_SR']=tt.agg.sum(core['Inter_SGSN_MME RAU success'])/tt.agg.sum(core['Inter_SGSN_MME RAU request'])*100
        m['Intra_SGSN_MME_RAU_SR']=tt.agg.sum(core['Intra_SGSN_MME RAU success'])/tt.agg.sum(core['Intra_SGSN_MME RAU request'])*100
        m['PS_Paging_SR']=(1-tt.agg.sum(core['packet paging none_response'])/tt.agg.sum(core['packet paging request']))*100
        m['Initial_PDP_Context_Activation_SR']=tt.agg.sum(core['MS init PDP_bear context act suc'])/tt.agg.sum(core['MS init PDP_bear context act'])*100
        m['PDP_SR']=tt.agg.sum(core['PDP success'])/tt.agg.sum(core['PDP request'])*100
        for element in [m['Data_Attach_SR'],m['Inter_SGSN_MME_RAU_SR'],m['Intra_SGSN_MME_RAU_SR'],m['PS_Paging_SR'],m['Initial_PDP_Context_Activation_SR'],m['PDP_SR']]:
            element.folder='Data KPIs'
        #
        m['Local_VLR_Attach_subscribers']=tt.agg.sum(core['VLR IMSI attached Subscribers'])-tt.agg.sum(core['VLR IMSI Attached Inter Roam Subs'])-\
                                            tt.agg.sum(core['VLR IMSI Attached National Roam Subs'])
        m['Local_2G_subscribers']=tt.agg.sum(core['VLR 2G LAI Subs'])-tt.agg.sum(core['International Roam 2G LAI Subs'])-tt.agg.sum(core['National Roam 2G LAI Subs'])
        m['Local_3G_subscribers']=tt.agg.sum(core['VLR 3G LAI Subs'])-tt.agg.sum(core['International Roam 3G LAI Subs'])-tt.agg.sum(core['National Roam 3G LAI Subs'])
        m['VLR_Total_Subscribers']=tt.agg.sum(core['VLR Total Subscribers'])
        m['VLR_2G_LAI_Subs']=tt.agg.sum(core['VLR 2G LAI Subs'])
        m['VLR_3G_LAI_Subs']=tt.agg.sum(core['VLR 3G LAI Subs'])
        m['VLR_IMSI_attached_Subs']=tt.agg.sum(core['VLR IMSI attached Subscribers'])
        m['IMSI_Attached_2G_LAI_Subs']=tt.agg.sum(core['IMSI Attached 2G LAI Subs'])
        m['IMSI_Attached_3G_LAI_Subs']=tt.agg.sum(core['IMSI Attached 3G LAI Subs'])
        m['VLR_Local_Subs']=tt.agg.sum(core['VLR Local Subs'])
        m['VLR_International_Roam_Subs']=tt.agg.sum(core['VLR International Roam Subs'])
        m['International_Roam_2G_LAI_Subs']=tt.agg.sum(core['International Roam 2G LAI Subs'])
        m['International_Roam_3G_LAI_Subs']=tt.agg.sum(core['International Roam 3G LAI Subs'])
        m['VLR_IMSI_Attached_Inter_Roam_Subs']=tt.agg.sum(core['VLR IMSI Attached Inter Roam Subs'])
        m['IMSI_Attached_Inter_Roam_2G_LAI_Subs']=tt.agg.sum(core['IMSI Attached International Roam 2G LAI Subs'])
        m['IMSI_Attached_Inter_Roam_3G_LAI_Subs']=tt.agg.sum(core['IMSI Attached International Roam 3G LAI Subs'])
        m['National_Roam_2G_LAI_Subs']=tt.agg.sum(core['National Roam 2G LAI Subs'])
        m['National_Roam_3G_LAI_Subs']=tt.agg.sum(core['National Roam 3G LAI Subs'])
        m['VLR_National_Roam_Subs']=tt.agg.sum(core['VLR National Roam Subs'])
        m['VLR_IMSI_Attached_National_Roam_Subs']=tt.agg.sum(core['VLR IMSI Attached National Roam Subs'])
        m['IMSI_Attached_National_Roam_2G_LAI_Subs']=tt.agg.sum(core['IMSI Attached National Roam 2G LAI Subs'])
        m['IMSI_Attached_National_Roam_3G_LAI_Subs']=tt.agg.sum(core['IMSI Attached National Roam 3G LAI Subs'])
        m['EPS_bearers']=tt.agg.sum(core['Max_simult_act_PGW_EPS_bearer'])+tt.agg.sum(core['Max_simult_act_S_PGW_EPS_bearer'])
        m['2G_3G_bearers']=tt.agg.sum(core['Max_active_utran_bearers'])+tt.agg.sum(core['Max_active_geran_bearers'])
        m['2G_bearers']=tt.agg.sum(core['Max_active_geran_bearers'])
        m['3G_bearers']=tt.agg.sum(core['Max_active_utran_bearers'])
        m['4G_bearers']=tt.agg.sum(core['Max_active_eutran_bearers'])
        m['Max_simult_activ_PDP_ctx']=tt.agg.sum(core['Max_simult_act_PDP_context'])
        m['Total_bearers']=m['EPS_bearers']+m['Max_simult_activ_PDP_ctx']+m['3G_bearers']+m['2G_bearers']+m['4G_bearers']
        m['Gb_mode_max_attached_user']=tt.agg.sum(core['Gb mode max attached user'])
        m['Iu_mode_max_attached_user']=tt.agg.sum(core['Iu mode max attached user'])
        m['S1_mode_max_attached_user']=tt.agg.sum(core['S1 mode max attached user'])
        m['Total_attach_user']=m['Gb_mode_max_attached_user']+m['Iu_mode_max_attached_user']+m['S1_mode_max_attached_user']
        m['Gb_mode_max_user_with_pdp_activated_ctx']=tt.agg.sum(core['Gb mode max user with pdp activated context'])
        m['Iu_mode_max_user_with_pdp_activated_ctx']=tt.agg.sum(core['Iu mode max user with pdp activated context'])
        m['S1_mode_max_user_with_pdn_connection']=tt.agg.sum(core['S1 mode max PDN connection num'])
        m['Total_user_with_pdp_activated_ctx']=m['Gb_mode_max_user_with_pdp_activated_ctx']+m['Iu_mode_max_user_with_pdp_activated_ctx']+m['S1_mode_max_user_with_pdn_connection']
        for element in [m['Local_VLR_Attach_subscribers'],m['Local_2G_subscribers'],m['Local_3G_subscribers'],m['VLR_Total_Subscribers'],m['VLR_2G_LAI_Subs'],
                        m['VLR_3G_LAI_Subs'],m['VLR_IMSI_attached_Subs'],m['IMSI_Attached_2G_LAI_Subs'],m['IMSI_Attached_3G_LAI_Subs'],m['VLR_Local_Subs'],
                        m['VLR_International_Roam_Subs'],m['International_Roam_2G_LAI_Subs'],m['International_Roam_3G_LAI_Subs'],
                        m['VLR_IMSI_Attached_Inter_Roam_Subs'],m['IMSI_Attached_Inter_Roam_2G_LAI_Subs'],m['IMSI_Attached_Inter_Roam_3G_LAI_Subs'],
                        m['National_Roam_2G_LAI_Subs'],m['National_Roam_3G_LAI_Subs'],m['VLR_National_Roam_Subs'],m['VLR_IMSI_Attached_National_Roam_Subs'],
                        m['IMSI_Attached_National_Roam_2G_LAI_Subs'],m['IMSI_Attached_National_Roam_3G_LAI_Subs'],m['EPS_bearers'],m['2G_3G_bearers'],
                        m['2G_bearers'],m['3G_bearers'],m['4G_bearers'],m['Max_simult_activ_PDP_ctx'],m['Total_bearers'],m['Gb_mode_max_attached_user'],
                        m['Iu_mode_max_attached_user'],m['S1_mode_max_attached_user'],m['Total_attach_user'],m['Gb_mode_max_user_with_pdp_activated_ctx'],
                        m['Iu_mode_max_user_with_pdp_activated_ctx'],m['S1_mode_max_user_with_pdn_connection'],m['Total_user_with_pdp_activated_ctx']]:
             element.folder='Users'
        #
        m['VoLTE_MO_Call_Completion_SR']=tt.agg.sum(core['MO Connected'])/tt.agg.sum(core['MO Attempts'])*100
        m['VoLTE_MT_Call_Completion_SR']=tt.agg.sum(core['MT Connected'])/tt.agg.sum(core['MT Attempts'])*100
        m['VoLTE_MO_Call_Answer_Rate']=tt.agg.sum(core['MO Answered'])/tt.agg.sum(core['MO Attempts'])*100
        m['VoLTE_MT_Call_Answer_Rate']=tt.agg.sum(core['MT Answered'])/tt.agg.sum(core['MT Attempts'])*100
        m['VoLTE_MO_Call_Interrupt_Rate']=tt.agg.sum(core['MO Interruptions'])/tt.agg.sum(core['MO Connected'])*100
        m['VoLTE_MT_Call_Interrupt_Rate']=tt.agg.sum(core['MT Interruptions'])/tt.agg.sum(core['MT Connected'])*100
        m['VoLTE_MO_Call_setup_time_sec']=tt.agg.sum(core['Mean duration of connected MO session_ms'])/1000
        m['VoLTE_MT_Call_setup_time_sec']=tt.agg.sum(core['Mean duration of connected MT session_ms'])/1000
        m['VoLTE_BHCA_per_subs']=tt.agg.sum(core['BHCA per Subscriber'])
        m['VoLTE_Mean_duration_of_MO_calls_sec']=tt.agg.sum(core['Mean duration of MO seized sessions_sec'])
        m['VoLTE_Mean_duration_of_MT_calls_sec']=tt.agg.sum(core['Mean duration of MT seized sessions_sec'])
        m['VoLTE_peak_simultaneous_sessions']=tt.agg.sum(core['Peak num of ATS simultaneous sessions'])
        m['VoLTE_subscribers']=tt.agg.sum(core['ATS VoLTE registered subs'])
        m['VoLTE_Initial_Registration_SR']=tt.agg.sum(core['Initial register success'])/tt.agg.sum(core['Initial register attempt'])*100
        m['VoLTE_Re_registration_SR']=tt.agg.sum(core['Re-registration success'])/tt.agg.sum(core['Re-registration attempt'])*100
        m['VoLTE_Deregistration_SR']=tt.agg.sum(core['Deregistration success'])/tt.agg.sum(core['Deregistration attempt'])*100
        m['VoLTE_Notify_messages_for_Reg_SR']=tt.agg.sum(core['Notify Messages for Reg num'])/tt.agg.sum(core['Notify Messages for Reg den'])*100
        m['VoLTE_Subscrieb_requests_for_Reg_SR']=tt.agg.sum(core['Subscribe Requests for Registration num'])/tt.agg.sum(core['Subscribe Requests for Registration den'])*100
        m['VoLTE_Third_party_Deregistrations_SR']=tt.agg.sum(core['Third_party deregistration success'])/tt.agg.sum(core['Third_party deregistration attempt'])*100
        m['VoLTE_Third_party_Registrations_SR']=tt.agg.sum(core['Third_party registration success'])/(tt.agg.sum(core['Third_party registration success'])+\
                                                                                                        tt.agg.sum(core['Third_party registration unsuccess']))*100
        m['VoLTE_Authentication_SR']=tt.agg.sum(core['Authentication success'])/tt.agg.sum(core['Authentication attempt'])*100
        m['VoLTE_MO_Call_Drop_Rate']=tt.agg.sum(core['MO Call Drop'])/tt.agg.sum(core['MO Answered'])*100
        m['VoLTE_MT_Call_Drop_Rate']=tt.agg.sum(core['MT Call Drop'])/tt.agg.sum(core['MT Answered'])*100
        m['LTE_to_GSM_SRVCC_HOSR']=tt.agg.sum(core['LTE_to_GSM SRVCC num'])/tt.agg.sum(core['LTE_to_GSM SRVCC den'])*100
        m['LTE_to_UMTS_SRVCC_HOSR']=tt.agg.sum(core['LTE_to_UMTS SRVCC num'])/tt.agg.sum(core['LTE_to_UMTS SRVCC den'])*100
        m['S1_mode_IMS_PDN_connections']=tt.agg.sum(core['Maximum number of PDN connections'])
        m['IMS_bearer_activation_SR']=tt.agg.sum(core['Voice bearer success over S11'])/tt.agg.sum(core['Voice bearer request over S11'])*100
        m['VoLTE_Voice_Bearer_Activation_SR(IMS)']=tt.agg.sum(core['Voice bearer success'])/tt.agg.sum(core['Voice bearer request'])*100
        m['VoLTE_PDN_Connect_SR(IMS)']=tt.agg.sum(core['PDN Connect success'])/tt.agg.sum(core['PDN Connect request'])*100
        m['IMS_Voice_Drop_Rate']=tt.agg.sum(core['IMS_Voice_dr_num'])/tt.agg.sum(core['Voice bearer success'])*100
        m['VoLTE_Inter_MME_Voice_Bearer_HOSR']=tt.agg.sum(core['Inter S1 Handover num'])/tt.agg.sum(core['Inter S1 Handover den'])*100
        m['VoLTE_Intra_MME_Voice_Bearer_HOSR']=(tt.agg.sum(core['X2 Handover num'])+tt.agg.sum(core['Intra S1 Handover num']))/\
                                                (tt.agg.sum(core['X2 Handover den'])+tt.agg.sum(core['Intra S1 Handover den']))*100
        #
        for element in [m['VoLTE_MO_Call_Completion_SR'],m['VoLTE_MT_Call_Completion_SR'],m['VoLTE_MO_Call_Answer_Rate'],m['VoLTE_MT_Call_Answer_Rate'],
        m['VoLTE_MO_Call_Interrupt_Rate'],m['VoLTE_MT_Call_Interrupt_Rate'],m['VoLTE_MO_Call_setup_time_sec'],m['VoLTE_MT_Call_setup_time_sec'],
        m['VoLTE_BHCA_per_subs'],m['VoLTE_Mean_duration_of_MO_calls_sec'],m['VoLTE_Mean_duration_of_MT_calls_sec'],m['VoLTE_peak_simultaneous_sessions'],
        m['VoLTE_subscribers'],m['VoLTE_Initial_Registration_SR'],m['VoLTE_Re_registration_SR'],m['VoLTE_Deregistration_SR'],m['VoLTE_Notify_messages_for_Reg_SR'],
        m['VoLTE_Subscrieb_requests_for_Reg_SR'],m['VoLTE_Third_party_Deregistrations_SR'],m['VoLTE_Third_party_Registrations_SR'],m['VoLTE_Authentication_SR'],
        m['VoLTE_MO_Call_Drop_Rate'],m['VoLTE_MT_Call_Drop_Rate'],m['LTE_to_GSM_SRVCC_HOSR'],m['LTE_to_UMTS_SRVCC_HOSR'],m['S1_mode_IMS_PDN_connections'],
        m['IMS_bearer_activation_SR'],m['VoLTE_Voice_Bearer_Activation_SR(IMS)'],m['VoLTE_PDN_Connect_SR(IMS)'],m['IMS_Voice_Drop_Rate'],
        m['VoLTE_Inter_MME_Voice_Bearer_HOSR'],m['VoLTE_Intra_MME_Voice_Bearer_HOSR']]:
             element.folder='VoLTE'
        #
        m['2G_3G_DL_traf_GB']=tt.agg.sum(core['2G_3G DL'])/1024/1024
        m['2G_3G_UL_traf_GB']=tt.agg.sum(core['2G_3G UL'])/1024/1024
        m['2G_3G_traf_GB']=m['2G_3G_DL_traf_GB']+m['2G_3G_UL_traf_GB']
        m['4G_DL_traf_GB']=tt.agg.sum(core['4G DL'])/1024/1024
        m['4G_UL_traf_GB']=tt.agg.sum(core['4G UL'])/1024/1024
        m['4G_traf_GB']=m['4G_DL_traf_GB']+m['4G_UL_traf_GB']
        m['Total_DL_traf_TB']=(m['2G_3G_DL_traf_GB']+m['4G_DL_traf_GB'])/1024
        m['Total_UL_traf_TB']=(m['2G_3G_UL_traf_GB']+m['4G_UL_traf_GB'])/1024
        m['Total_traf_TB']=m['Total_DL_traf_TB']+m['Total_UL_traf_TB']
        m['Total_DL_traf_Gbps']=m['Total_DL_traf_TB']*1024*1024*8/3600
        m['Total_UL_traf_Gbps']=m['Total_UL_traf_TB']*1024*1024*8/3600
        m['Total_traf_Gbps']=m['Total_DL_traf_Gbps']+m['Total_UL_traf_Gbps']
        #
        for element in [m['2G_3G_DL_traf_GB'],m['2G_3G_UL_traf_GB'],m['2G_3G_traf_GB'],m['4G_DL_traf_GB'],m['4G_UL_traf_GB'],m['4G_traf_GB'],m['Total_DL_traf_TB'],
                        m['Total_UL_traf_TB'],m['Total_traf_TB'],m['Total_DL_traf_Gbps'],m['Total_UL_traf_Gbps'],m['Total_traf_Gbps']]:
             element.folder='Data Traffic'
    except Exception as e:
        print(e)
        1
    #tt.where(l["City"] == "Paris", m["Value.SUM"], 0)
#    
add_measures()

while True:
     1

    
