import atoti as tt
#import webbrowser
import pandas as pd
import time, os
import numpy as np
import glob

session=tt.Session(port=55708,user_content_storage="/disk2/support_files/atoti/content")


# join tables
#three.join(two, mapping={'Date':'Date','Region':'Region','Day':'Day','Month':'Month','Year':'Year'})
#three.join(four, mapping={'Date':'Date','Region':'Region','Day':'Day','Month':'Month','Year':'Year'})

#h,l,m = cube.hierarchies, cube.levels, cube.measures
#h['Month']=[three['Month']]
#h['Year']=[three['Year']]

#cube.create_date_hierarchy("Date_hierarchy",column=three['Date'],levels={'Year':'yyyy',"Month": "MM",'Day':'dd'})

def add_measures():
	h,l,m =cube8.hierarchies, cube8.levels, cube8.measures
	for i in m.keys():
		if 'Count' in i:continue
		del m[i]

	h['Hour']=[t_three_grp['Hour']]
	h['Minute']=[t_three_grp['Minute']]

	m['2G_Availability']=(tt.agg.sum(t_two_grp['2G_cell_avail_num'])+tt.agg.sum(t_two_grp['2G_cell_avail_blck_num']))/ \
	(tt.agg.sum(t_two_grp['2G_cell_avail_den'])-tt.agg.sum(t_two_grp['2G_cell_avail_blck_den']))*100 
	m['3G_Availability']=(tt.agg.sum(t_three_grp['3G_cell_avail_num'])+tt.agg.sum(t_three_grp['3G_cell_avail_blck_num']))/ \
		(tt.agg.sum(t_three_grp['3G_cell_avail_den'])-tt.agg.sum(t_three_grp['3G_cell_avail_blck_den']))*100
	m['4G_Availability']=(tt.agg.sum(t_four_grp['4G_cell_avail_num'])+tt.agg.sum(t_four_grp['4G_cell_avail_blck_num']))/  \
		(tt.agg.sum(t_four_grp['4G_cell_avail_den'])-tt.agg.sum(t_four_grp['4G_cell_avail_blck_den']))*100
	m['Total_Availability']=(tt.agg.sum(t_four_grp['4G_cell_avail_num'])+tt.agg.sum(t_four_grp['4G_cell_avail_blck_num'])+ \
							tt.agg.sum(t_two_grp['2G_cell_avail_num'])+tt.agg.sum(t_two_grp['2G_cell_avail_blck_num'])+ \
							tt.agg.sum(t_three_grp['3G_cell_avail_num'])+tt.agg.sum(t_three_grp['3G_cell_avail_blck_num']))/  \
		(tt.agg.sum(t_four_grp['4G_cell_avail_den'])-tt.agg.sum(t_four_grp['4G_cell_avail_blck_den'])+ \
		tt.agg.sum(t_two_grp['2G_cell_avail_den'])-tt.agg.sum(t_two_grp['2G_cell_avail_blck_den']) + \
		tt.agg.sum(t_three_grp['3G_cell_avail_den'])-tt.agg.sum(t_three_grp['3G_cell_avail_blck_den']))*100

	m['3G_Voice_share']=(tt.agg.sum(t_three_grp['3G_cs_traf'])/(tt.agg.sum(t_three_grp['3G_cs_traf'])+tt.agg.sum(t_two_grp['2G_cs_traf'])+tt.agg.sum(t_four_grp['4G_volte_cs_traf'])))*100
	m['4G_Data_share']=(tt.agg.sum(t_four_grp['4G_ps_traf'])/(tt.agg.sum(t_four_grp['4G_ps_traf'])+tt.agg.sum(t_three_grp['3G_ps_traf'])))*100
	m['3G_Voice_share_twoG_threeG']=(tt.agg.sum(t_three_grp['3G_cs_traf'])/(tt.agg.sum(t_three_grp['3G_cs_traf'])+tt.agg.sum(t_two_grp['2G_cs_traf'])))*100
	m['VoLTE_Voice_share']=(tt.agg.sum(t_four_grp['4G_volte_cs_traf'])/(tt.agg.sum(t_three_grp['3G_cs_traf'])+tt.agg.sum(t_two_grp['2G_cs_traf'])+tt.agg.sum(t_four_grp['4G_volte_cs_traf'])))*100

	m['2G_Voice_Traffic_Erl']=tt.agg.sum(t_two_grp['2G_cs_traf'])
	m['3G_Voice_Traffic_Erl']=tt.agg.sum(t_three_grp['3G_cs_traf'])
	m['Total_Voice_Traffic_Erl']=tt.agg.sum(t_two_grp['2G_cs_traf'])+tt.agg.sum(t_three_grp['3G_cs_traf'])+tt.agg.sum(t_four_grp['4G_volte_cs_traf'])

	m['2G_Data_Traffic_GB']=tt.agg.sum(t_two_grp['2G_ps_traf'])/1024
	m['3G_Data_Traffic_GB']=tt.agg.sum(t_three_grp['3G_ps_traf'])/1024
	m['4G_Data_Traffic_GB']=tt.agg.sum(t_four_grp['4G_ps_traf'])/1024
	m['5G_Data_Traffic_GB']=(tt.agg.sum(t_five_grp['5G_dl_ps_traf'])+tt.agg.sum(t_five_grp['5G_ul_ps_traf']))/1024
	m['Total_Data_Traffic_GB']=(tt.agg.sum(t_two_grp['2G_ps_traf'])+tt.agg.sum(t_three_grp['3G_ps_traf'])+tt.agg.sum(t_four_grp['4G_ps_traf']))/1024

	m['2G_Call_Setup_SR']=(1-(tt.agg.sum(t_two_grp['2G_cssr_num1'])/tt.agg.sum(t_two_grp['2G_cssr_den1'])))* \
						(tt.agg.sum(t_two_grp['2G_cssr_num2'])/tt.agg.sum(t_two_grp['2G_cssr_den2'])) * \
						(tt.agg.sum(t_two_grp['2G_cssr_num3'])/tt.agg.sum(t_two_grp['2G_cssr_den3']))*100
	m['2G_Call_blocks'] =tt.agg.sum(t_two_grp['2G_cssr_den2'])-tt.agg.sum(t_two_grp['2G_cssr_num2'])
	m['2G_SDCCH_blocks']=tt.agg.sum(t_two_grp['2G_cssr_den3'])-tt.agg.sum(t_two_grp['2G_cssr_num3'])

	m['3G_Call_Setup_SR']=(tt.agg.sum(t_three_grp['3G_voice_sr_num1'])/tt.agg.sum(t_three_grp['3G_voice_sr_den1'])) * \
							(tt.agg.sum(t_three_grp['3G_voice_sr_num2'])/tt.agg.sum(t_three_grp['3G_voice_sr_den2']))*100
							
	m['3G_Call_Setup_fails']=(tt.agg.sum(t_three_grp['3G_voice_sr_den1'])-tt.agg.sum(t_three_grp['3G_voice_sr_num1'])) + \
							(tt.agg.sum(t_three_grp['3G_voice_sr_den2'])-tt.agg.sum(t_three_grp['3G_voice_sr_num2']))
	m['Total_Call_Setup_SR']=(m['2G_Call_Setup_SR']*(100-m['3G_Voice_share_twoG_threeG'])+m['3G_Call_Setup_SR']*m['3G_Voice_share_twoG_threeG'])/100
						
	m['2G_Call_DR']=tt.agg.sum(t_two_grp['2G_drop_rate_num'])/tt.agg.sum(t_two_grp['2G_drop_rate_den'])*100
	m['2G_Call_drops']=tt.agg.sum(t_two_grp['2G_drop_rate_num'])
	m['3G_Call_DR']=tt.agg.sum(t_three_grp['3G_voice_dr_num'])/tt.agg.sum(t_three_grp['3G_voice_dr_den'])*100
	m['3G_Call_drops']=tt.agg.sum(t_three_grp['3G_voice_dr_num'])
	m['Total_Call_DR']=(m['2G_Call_DR']*(100-m['3G_Voice_share_twoG_threeG'])+m['3G_Call_DR']*m['3G_Voice_share_twoG_threeG'])/100

	m['4G_Data_Setup_SR']=(tt.agg.sum(t_four_grp['4G_rrc_sr_num'])/tt.agg.sum(t_four_grp['4G_rrc_sr_den'])) * \
						(tt.agg.sum(t_four_grp['4G_rab_sr_num'])/tt.agg.sum(t_four_grp['4G_rab_sr_den']))*100
	m['3G_Data_Setup_SR']=tt.agg.sum(t_three_grp['3G_hsdpa_sr_num'])/tt.agg.sum(t_three_grp['3G_hsdpa_sr_den'])*100
	m['5G_Data_Setup_SR']=tt.agg.sum(t_five_grp['5G_rab_sr_num'])/tt.agg.sum(t_five_grp['5G_rab_sr_den'])*100
	m['Total_Data_Setup_SR']=(m['3G_Data_Setup_SR']*(100-m['4G_Data_share'])+m['4G_Data_Setup_SR']*m['4G_Data_share'])/100

	m['4G_Data_Setup_attempt']=tt.agg.sum(t_four_grp['4G_rab_sr_den'])
	m['3G_Data_Setup_attempt']=tt.agg.sum(t_three_grp['3G_hsdpa_sr_den'])
	m['5G_Data_Setup_attempt']=tt.agg.sum(t_five_grp['5G_rab_sr_den'])

	m['4G_Data_DR']=tt.agg.sum(t_four_grp['4G_dcr_num'])/tt.agg.sum(t_four_grp['4G_dcr_den'])*100
	m['3G_Data_DR']=tt.agg.sum(t_three_grp['3G_hsdpa_dr_num'])/tt.agg.sum(t_three_grp['3G_hsdpa_dr_den'])*100
	m['5G_Data_DR']=tt.agg.sum(t_five_grp['5G_dcr_num'])/tt.agg.sum(t_five_grp['5G_dcr_den'])*100
	m['Total_Data_DR']=(m['3G_Data_DR']*(100-m['4G_Data_share'])+m['4G_Data_DR']*m['4G_Data_share'])/100
	m['5G_Data_drops']=tt.agg.sum(t_five_grp['5G_dcr_num'])

	m['4G_DL_thrp(Mbps)']=tt.agg.sum(t_four_grp['4G_dl_thrp_num'])/tt.agg.sum(t_four_grp['4G_dl_thrp_den'])/1024
	m['3G_DL_thrp(Mbps)']=tt.agg.sum(t_three_us_grp['h_thrp_num'])/tt.agg.sum(t_three_us_grp['h_thrp_den'])/1024
	m['5G_DL_thrp(Mbps)']=tt.agg.sum(t_five_grp['5G_dl_thrp_num'])/tt.agg.sum(t_five_grp['5G_dl_thrp_den'])
	m['5G_UL_thrp(Mbps)']=tt.agg.sum(t_five_grp['5G_ul_thrp_num'])/tt.agg.sum(t_five_grp['5G_ul_thrp_den'])
	m['Total_DL_thrp(Mbps)']=(m['3G_DL_thrp(Mbps)']*(100-m['4G_Data_share'])+m['4G_DL_thrp(Mbps)']*m['4G_Data_share'])/100/1024
	m['4G_DL_thrp(Mbps)'].formatter="DOUBLE[0.00]"
	m['3G_DL_thrp(Mbps)'].formatter="DOUBLE[0.00]"
	m['5G_DL_thrp(Mbps)'].formatter="DOUBLE[0.00]"
	m['5G_UL_thrp(Mbps)'].formatter="DOUBLE[0.00]"

	m['VoLTE_Data_Traffic_GB']=tt.agg.sum(t_four_grp['4G_volte_ps_traf'])/1024
	m['VoLTE_Voice_Traffic_Erl']=tt.agg.sum(t_four_grp['4G_volte_cs_traf'])
	m['VoLTE_Call_Setup_SR']=(tt.agg.sum(t_four_grp['4G_volte_sr_num'])/tt.agg.sum(t_four_grp['4G_volte_sr_den']))*100
	m['VoLTE_Call_fails']=tt.agg.sum(t_four_grp['4G_volte_sr_den'])-tt.agg.sum(t_four_grp['4G_volte_sr_num'])
	m['VoLTE_Call_DR']=(tt.agg.sum(t_four_grp['4G_volte_dr_num'])/tt.agg.sum(t_four_grp['4G_volte_dr_den']))*100

	#m['3G_Voice_share']=(tt.agg.sum(t_three_grp['3G_cs_traf'])/(tt.agg.sum(three['3G_cs_traf'])+tt.agg.sum(two['2G_cs_traf'])+tt.agg.sum(four['4G_volte_cs_traf'])))*100
	m['VoLTE_Voice_share']=(tt.agg.sum(t_four_grp['4G_volte_cs_traf'])/(tt.agg.sum(t_three_grp['3G_cs_traf'])+tt.agg.sum(t_two_grp['2G_cs_traf'])+tt.agg.sum(t_four_grp['4G_volte_cs_traf'])))*100
	m['VoLTE_DL_silent_calls']= tt.agg.sum(t_four_grp['4G_Voice_DL_Silent_Num'])
	m['VoLTE_UL_silent_calls']= tt.agg.sum(t_four_grp['4G_Voice_UL_Silent_Num'])
	m['VoLTE_SRVCC_SR']= (tt.agg.sum(t_four_grp['4G_volte_srvcc_e2w_num'])/tt.agg.sum(t_four_grp['4G_volte_srvcc_e2w_den']))*100

	m['4G_DL_PRB_utilization']=(tt.agg.sum(t_four_grp['4G_dl_prb_num'])/tt.agg.sum(t_four_grp['4G_dl_prb_den']))*100
	m['4G_UL_PRB_utilization']=(tt.agg.sum(t_four_grp['4G_ul_prb_num'])/tt.agg.sum(t_four_grp['4G_ul_prb_den']))*100
	m['LTE_Max_active_user']=tt.agg.sum(t_four_grp['4G_Max_active_user'])
	m['LTE_RRC_user_license']=tt.agg.sum(t_four_grp['4G_RRC_user_license'])
	m['LTE_Max_active_user_DL_QCI1']=tt.agg.sum(t_four_grp['4G_Max_active_user_dl_qci1'])
	m['LTE_Max_active_user_UL_QCI1']=tt.agg.sum(t_four_grp['4G_Max_active_user_ul_qci1'])
	m['5G_Max_NSA_user']=tt.agg.sum(t_five_grp['5G_Max_nsa_user'])
	m['5G_Max_DL_DRB_user']=tt.agg.sum(t_five_grp['5G_Max_dl_drb_user'])
	m['5G_Max_UL_DRB_user']=tt.agg.sum(t_five_grp['5G_Max_ul_drb_user'])
	m['4G_RAB_fails_MME']=tt.agg.sum(t_four_grp['4G_Rab_fail_MME'])
	m['4G_RAB_fails_NoRadioRes']=tt.agg.sum(t_four_grp['4G_Rab_fail_NoRadioRes'])
	m['4G_RAB_fails_NoReply']=tt.agg.sum(t_four_grp['4G_Rab_fail_NoReply'])
	m['4G_RAB_fails_RNL']=tt.agg.sum(t_four_grp['4G_Rab_fail_RNL'])
	m['4G_RAB_fails_SecurModeFails']=tt.agg.sum(t_four_grp['4G_Rab_fail_SecurModeFail'])
	m['4G_RAB_fails_TNL']=tt.agg.sum(t_four_grp['4G_Rab_fail_TNL'])
	m['4G_RTWP_mean']=tt.agg.mean(t_four_grp['4G_rtwp'])
	m['4G_CSFB_SR']=tt.agg.sum(t_four_grp['4G_csfb_sr_num'])/tt.agg.sum(t_four_grp['4G_csfb_sr_den'])*100
	m['4G_Max_connected_user']=tt.agg.sum(t_four_grp['4G_Max_connected_user'])

	m['3G_RTWP_mean']=10*tt.math.log10(tt.agg.sum(t_three_grp['3G_rtwp_num'])/tt.agg.sum(t_three_grp['3G_rtwp_den']))
	m['3G_Overload']=tt.agg.mean(t_three_uc_grp['Ovl_TxCrPwr_time_share_DL'])
	m['HS_DSCH_CREDIT_RDCT_FRM_LOSS']=tt.agg.sum(t_three_uc_grp['HS_DSCH_CREDIT_RDCT_FRM_LOSS'])
	m['IUB_LOSS_CC_FRAME_LOSS_IND']=tt.agg.sum(t_three_uc_grp['IUB_LOSS_CC_FRAME_LOSS_IND'])
	m['HSDPA_stp_fails_BTS']=tt.agg.sum(t_three_uc_grp['HSDPA_stp_fails, BTS'])
	m['HSDPA_stp_fails_MaxUsers']=tt.agg.sum(t_three_uc_grp['HSDPA_stp_fails, MaxUsers'])
	m['HSUPA_stp_fails_BTS']=tt.agg.sum(t_three_uc_grp['HSUPA_stp_fails, BTS'])
	m['HSUPA_stp_fails_BTS_HW']=tt.agg.sum(t_three_uc_grp['HSUPA_stp_fails, BTS HW'])
	m['HSUPA_stp_fails_MaxUsers']=tt.agg.sum(t_three_uc_grp['HSUPA_stp_fails, MaxUsers'])
	m['RAB_setup_fail_voice_AC']=tt.agg.sum(t_three_uc_grp['rab_stp_fail_cs_voice_ac (M1001C80)'])
	m['RAB_setup_fail_voice_BTS']=tt.agg.sum(t_three_uc_grp['rab_stp_fail_cs_voice_bts (M1001C81)'])
	m['RAB_setup_fail_voice_Frozen_BTS']=tt.agg.sum(t_three_uc_grp['rab_stp_fail_cs_voice_frozbs (M1001C84)'])
	m['RAB_setup_fail_voice_RNC']=tt.agg.sum(t_three_uc_grp['rab_stp_fail_cs_voice_rnc (M1001C83)'])
	m['RAB_setup_fail_voice_Trans']=tt.agg.sum(t_three_uc_grp['rab_stp_fail_cs_voice_trans (M1001C82)'])
	m['RRC_conn_setup_fail_AC']=tt.agg.sum(t_three_uc_grp['rrc_conn_stp_fail_ac (M1001C3)'])
	m['RRC_conn_setup_fail_BTS']=tt.agg.sum(t_three_uc_grp['rrc_conn_stp_fail_bts (M1001C4)'])
	m['RRC_conn_setup_fail_Frozen_BTS']=tt.agg.sum(t_three_uc_grp['rrc_conn_stp_fail_frozbs (M1001C7)'])
	m['RRC_conn_setup_fail_Handover_control']=tt.agg.sum(t_three_uc_grp['rrc_conn_stp_fail_hc (M1001C2)'])
	m['RRC_conn_setup_fail_RNC']=tt.agg.sum(t_three_uc_grp['rrc_conn_stp_fail_rnc (M1001C6)'])
	m['RRC_conn_setup_fail_Trans']=tt.agg.sum(t_three_uc_grp['rrc_conn_stp_fail_trans (M1001C5)'])
	m['BTS_HSUPA_NO_HW_CAPA_DUR']=tt.agg.sum(t_three_uc_grp['BTS_HSUPA_NO_HW_CAPA_DUR (M1000C270)'])
	m['BTS_HSUPA_HW_LIMITED_DUR']=tt.agg.sum(t_three_uc_grp['BTS_HSUPA_HW_LIMITED_DUR (M1000C269)'])

	m['3G_HSDPA_Throughput_utilization']=tt.agg.sum(t_three_us_grp['HSDPA_MAX_MACHS_THR'])/tt.agg.sum(t_three_us_grp['LIC_HSDPA_THR'])*100
	m['3G_HSUPA_Throughput_utilization']=tt.agg.sum(t_three_us_grp['HSUPA_MAX_MACE_THR'])/tt.agg.sum(t_three_us_grp['LIC_HSUPA_THR'])*100
	m['3G_HSDPA_User_utilization']=tt.agg.sum(t_three_us_grp['MAX_BTS_HSDPA_USERS'])/tt.agg.sum(t_three_us_grp['LIC_NUM_HSDPA_USERS'])*100
	m['3G_HSUPA_User_utilization']=tt.agg.sum(t_three_ul_grp['LCG_MAX_HSUPA_USERS'])/tt.agg.sum(t_three_ul_grp['LCG_AVAIL_HSUPA_USERS'])*100
	m['3G_max_HSDPA_users']=tt.agg.sum(t_three_us_grp['MAX_BTS_HSDPA_USERS'])
	m['3G_max_HSUPA_users']=tt.agg.sum(t_three_ul_grp['LCG_MAX_HSUPA_USERS'])
	m['3G_HSDPA_Throughput_utilization'].formatter="DOUBLE[0.00]"
	m['3G_HSUPA_Throughput_utilization'].formatter="DOUBLE[0.00]"
	m['3G_HSDPA_User_utilization'].formatter="DOUBLE[0.00]"
	m['3G_HSUPA_User_utilization'].formatter="DOUBLE[0.00]"
	m['LTE_Max_active_user'].formatter="INT[0]"
	m['5G_Max_NSA_user'].formatter="INT[0]"
	m['3G_max_HSDPA_users'].formatter="INT[0]"
	m['3G_max_HSUPA_users'].formatter="INT[0]"
	m['4G_Max_connected_user'].formatter="INT[0]"

	m['DCH_OK_FP_DATA_FRMS']=tt.agg.sum(t_three_ul_grp['DCH_OK_FP_DATA_FRMS'])
	m['DCH_FP_REC_FRMS_W_CRC_ERR']=tt.agg.sum(t_three_ul_grp['DCH_FP_REC_FRMS_W_CRC_ERR'])
	m['DCH_FP_REC_FRMS_W_DELAY']=tt.agg.sum(t_three_ul_grp['DCH_FP_REC_FRMS_W_DELAY'])
	m['DCH_FP_REC_FRMS_W_OTH_ERR']=tt.agg.sum(t_three_ul_grp['DCH_FP_REC_FRMS_W_OTH_ERR'])
	m['DCH_OK_FP_DATA_FRMS']=tt.agg.sum(t_three_ul_grp['DCH_OK_FP_DATA_FRMS'])
	m['HS_DSCH_OK_FP_FRMS']=tt.agg.sum(t_three_ul_grp['HS_DSCH_FP_FRMS_W_CRC'])
	m['HS_DSCH_FP_FRMS_W_OTH_ERR']=tt.agg.sum(t_three_ul_grp['HS_DSCH_FP_FRMS_W_OTH_ERR'])
	m['DCH_DATA_TO_IUB']=tt.agg.sum(t_three_ul_grp['DCH_DATA_TO_IUB'])
	m['DCH_DATA_FROM_IUB']=tt.agg.sum(t_three_ul_grp['DCH_DATA_FROM_IUB'])

	#m['VoLTE_DL_VQI(Voice_Quality_Index)'] = (tt.agg.sum(four['4G_Voice_VQI_DL_Excellent_Times'])/tt.agg.sum(four['4G_dl_vqi_tot']))*5 + \
	#					(tt.agg.sum(four['4G_Voice_VQI_DL_Good_Times'])/tt.agg.sum(four['4G_dl_vqi_tot']))*4 + \
	#					(tt.agg.sum(four['4G_Voice_VQI_DL_Accept_Times'])/tt.agg.sum(four['4G_dl_vqi_tot']))*3 + \
	#					(tt.agg.sum(four['4G_Voice_VQI_DL_Poor_Times'])/tt.agg.sum(four['4G_dl_vqi_tot']))*2 + \
	#					(tt.agg.sum(four['4G_Voice_VQI_DL_Bad_Times'])/tt.agg.sum(four['4G_dl_vqi_tot']))*1
	#m['VoLTE_UL_VQI(Voice_Quality_Index)'] = (tt.agg.sum(four['4G_Voice_VQI_UL_Excellent_Times'])/tt.agg.sum(four['4G_ul_vqi_tot']))*5 + \
	#					(tt.agg.sum(four['4G_Voice_VQI_UL_Good_Times'])/tt.agg.sum(four['4G_ul_vqi_tot']))*4 + \
	#					(tt.agg.sum(four['4G_Voice_VQI_UL_Accept_Times'])/tt.agg.sum(four['4G_ul_vqi_tot']))*3 + \
	#					(tt.agg.sum(four['4G_Voice_VQI_UL_Poor_Times'])/tt.agg.sum(four['4G_ul_vqi_tot']))*2 + \
	#					(tt.agg.sum(four['4G_Voice_VQI_UL_Bad_Times'])/tt.agg.sum(four['4G_ul_vqi_tot']))*1	
	#m['VoLTE_Total_VQI(Voice_Quality_Index)'] = ((tt.agg.sum(four['4G_Voice_VQI_DL_Excellent_Times'])+tt.agg.sum(four['4G_Voice_VQI_UL_Excellent_Times']))/ \
	#					(tt.agg.sum(four['4G_dl_vqi_tot'])+ tt.agg.sum(four['4G_ul_vqi_tot'])))*5 + \
	#					((tt.agg.sum(four['4G_Voice_VQI_DL_Good_Times'])+tt.agg.sum(four['4G_Voice_VQI_UL_Good_Times']))/ \
	#					(tt.agg.sum(four['4G_dl_vqi_tot'])+ tt.agg.sum(four['4G_ul_vqi_tot'])))*4 + \
	#					((tt.agg.sum(four['4G_Voice_VQI_DL_Accept_Times'])+tt.agg.sum(four['4G_Voice_VQI_UL_Accept_Times']))/ \
	#					(tt.agg.sum(four['4G_dl_vqi_tot'])+ tt.agg.sum(four['4G_ul_vqi_tot'])))*3 + \
	#					((tt.agg.sum(four['4G_Voice_VQI_DL_Poor_Times'])+tt.agg.sum(four['4G_Voice_VQI_UL_Poor_Times']))/ \
	#					(tt.agg.sum(four['4G_dl_vqi_tot'])+ tt.agg.sum(four['4G_ul_vqi_tot'])))*2 + \
	#					((tt.agg.sum(four['4G_Voice_VQI_DL_Bad_Times'])+tt.agg.sum(four['4G_Voice_VQI_UL_Bad_Times']))/ \
	#					(tt.agg.sum(four['4G_dl_vqi_tot'])+ tt.agg.sum(four['4G_ul_vqi_tot'])))*1
	print('end')
#add_measures()				


texno_2G=pd.read_csv('/disk2/support_files/archive/twoG.csv')
texno_3G=pd.read_csv('/disk2/support_files/archive/threeG.csv')
texno_4G=pd.concat([pd.read_csv('/disk2/support_files/archive/fourG.csv'),pd.read_csv('/disk2/support_files/archive/fourG_n.csv')],ignore_index=True)
texno_3G_util_cell=pd.read_csv('/disk2/support_files/archive/threeG_util_cell.csv')
texno_3G_util_site=pd.read_csv('/disk2/support_files/archive/threeG_util_site.csv')
texno_3G_util_lcg=pd.read_csv('/disk2/support_files/archive/threeG_util_lcg.csv')
texno_5G=pd.read_csv('/disk2/support_files/archive/fiveG.csv')

def process(twG,thG,foG,thG_c,thG_s,thG_l,fvG):
	texno_2G=twG
	texno_3G=thG
	texno_4G=foG
	texno_3G_util_cell=thG_c
	texno_3G_util_site=thG_s
	texno_3G_util_lcg=thG_l
	texno_5G=fvG

	texno_2G.drop_duplicates(inplace=True)
	texno_3G.drop_duplicates(inplace=True)
	texno_4G.drop_duplicates(inplace=True)
	texno_3G_util_cell.drop_duplicates(inplace=True)
	texno_3G_util_site.drop_duplicates(inplace=True)
	texno_3G_util_lcg.drop_duplicates(inplace=True)
	texno_5G.drop_duplicates(inplace=True)

	texno_2G['Date']=pd.to_datetime(texno_2G['Date'],dayfirst=True)
	texno_2G.iloc[:,6:]=texno_2G.iloc[:,6:].astype(float)
	texno_2G['Day']=texno_2G['Date'].dt.date
	texno_2G['Minute']=texno_2G['Date'].dt.minute
	texno_2G['Hour']=texno_2G['Date'].dt.hour
	texno_2G.rename(columns={'ps_traffic_mb':'ps_traf','cs_traffic_erl':'cs_traf'},inplace=True)

	cluster=pd.read_csv('/disk2/support_files/archive/texnofest_cluster.csv')
	texno_2G['Site']=texno_2G['Site_name'].apply(lambda x: x[1:8])
	#texno_2G_grp = texno_2G.groupby([pd.Grouper(key='Date', freq='1H'),'Cluster']).sum().reset_index()
	texno_2G_grp = texno_2G.groupby(['Date','Site']).sum().reset_index()
	texno_2G_grp.columns=[*texno_2G_grp.columns[:2],*texno_2G_grp.columns[2:].map(lambda x: '2G_'+x)]
	texno_2G_grp=texno_2G_grp.merge(cluster,left_on='Site',right_on='Site_name',how='outer')
	texno_2G_grp.loc[texno_2G_grp['Site'].isnull(),'Site']=texno_2G_grp.loc[texno_2G_grp['Site'].isnull(),'Site_name']

	texno_3G['Date']=pd.to_datetime(texno_3G['Date'],dayfirst=True)
	texno_3G.sort_values(by='Date',inplace=True)
	texno_3G.iloc[:,6:]=texno_3G.iloc[:,6:].astype(float)
	texno_3G['Day']=texno_3G['Date'].dt.date
	texno_3G['Minute']=texno_3G['Date'].dt.minute
	texno_3G['Hour']=texno_3G['Date'].dt.hour

	#texno_3G=texno_3G.merge(cluster,on='Site_name',how='left')
	texno_3G['Site']=texno_3G['Site_name'].apply(lambda x: x[1:8])
	texno_3G_grp = texno_3G.groupby(['Date','Site']).sum().reset_index()
	texno_3G_grp.columns=[*texno_3G_grp.columns[:2],*texno_3G_grp.columns[2:].map(lambda x: '3G_'+x)]
	texno_3G_grp=texno_3G_grp.merge(cluster,left_on='Site',right_on='Site_name',how='outer')
	texno_3G_grp.loc[texno_3G_grp['Site'].isnull(),'Site']=texno_3G_grp.loc[texno_3G_grp['Site'].isnull(),'Site_name']

	texno_4G['Date']=pd.to_datetime(texno_4G['Date'],dayfirst=True)
	texno_4G.iloc[:,5:]=texno_4G.iloc[:,5:].astype(float)
	texno_4G['Day']=texno_4G['Date'].dt.date
	texno_4G['Minute']=texno_4G['Date'].dt.minute
	texno_4G['Hour']=texno_4G['Date'].dt.hour
	texno_4G['ps_traf']=texno_4G['dl_ps_traf']+texno_4G['ul_ps_traf']
	texno_4G['volte_ps_traf']=texno_4G['volte_dl_ps_traf']+texno_4G['volte_ul_ps_traf']

	#texno_4G=texno_4G.merge(cluster,on='Site_name',how='left')
	texno_4G['Site']=texno_4G['Site_name'].apply(lambda x: x[1:8])
	texno_4G_grp = texno_4G.groupby(['Date','Site']).sum().reset_index().drop(columns='rtwp')
	texno_4G_grp1 = texno_4G.groupby(['Date','Site']).mean()['rtwp'].reset_index()
	texno_4G_grp=texno_4G_grp.merge(texno_4G_grp1,on=['Date','Site'],how='left')
	texno_4G_grp.columns=[*texno_4G_grp.columns[:2],*texno_4G_grp.columns[2:].map(lambda x: '4G_'+x)]
	texno_4G_grp=texno_4G_grp.merge(cluster,left_on='Site',right_on='Site_name',how='outer')
	texno_4G_grp.loc[texno_4G_grp['Site'].isnull(),'Site']=texno_4G_grp.loc[texno_4G_grp['Site'].isnull(),'Site_name']

	texno_5G['Date']=pd.to_datetime(texno_5G['Date'],dayfirst=True)
	texno_5G.iloc[:,4:]=texno_5G.iloc[:,4:].astype(float)
	texno_5G['Day']=texno_5G['Date'].dt.date
	texno_5G['Minute']=texno_5G['Date'].dt.minute
	texno_5G['Hour']=texno_5G['Date'].dt.hour

	#texno_5G=texno_5G.merge(cluster,on='Site_name',how='outer')
	texno_5G['Site']=texno_5G['Site_name'].apply(lambda x: x[1:8])
	#texno_5G=texno_5G[texno_5G['Cluster'].notnull()]
	texno_5G_grp = texno_5G.groupby(['Date','Site']).sum().reset_index()
	texno_5G_grp.columns=[*texno_5G_grp.columns[:2],*texno_5G_grp.columns[2:].map(lambda x: '5G_'+x)]
	texno_5G_grp=texno_5G_grp.merge(cluster,left_on='Site',right_on='Site_name',how='outer')
	texno_5G_grp=texno_5G_grp[texno_5G_grp['Cluster'].notnull()]
	texno_5G_grp.loc[texno_5G_grp['Site'].isnull(),'Site']=texno_5G_grp.loc[texno_5G_grp['Site'].isnull(),'Site_name']

	texno_3G_util_cell['Date']=pd.to_datetime(texno_3G_util_cell['Date'],dayfirst=True)
	texno_3G_util_cell['Day']=texno_3G_util_cell['Date'].dt.date
	texno_3G_util_cell['Minute']=texno_3G_util_cell['Date'].dt.minute
	texno_3G_util_cell['Hour']=texno_3G_util_cell['Date'].dt.hour

	#texno_3G_util_cell=texno_3G_util_cell.merge(cluster,left_on='WBTS_name',right_on='Site_name',how='left')
	texno_3G_util_cell['Site']=texno_3G_util_cell['WBTS_name'].apply(lambda x: x[1:8])
	texno_3G_util_cell_grp = texno_3G_util_cell.groupby(['Date','Site']).sum().reset_index().drop(columns=['Hour','Minute','Ovl_TxCrPwr_time_share_DL'])
	texno_3G_util_cell_grp1 = texno_3G_util_cell.groupby(['Date','Site']).mean()['Ovl_TxCrPwr_time_share_DL'].reset_index()
	texno_3G_util_cell_grp=texno_3G_util_cell_grp.merge(texno_3G_util_cell_grp1,on=['Date','Site'],how='left')
	texno_3G_util_cell_grp=texno_3G_util_cell_grp.merge(cluster,left_on='Site',right_on='Site_name',how='outer')
	texno_3G_util_cell_grp.loc[texno_3G_util_cell_grp['Site'].isnull(),'Site']=texno_3G_util_cell_grp.loc[texno_3G_util_cell_grp['Site'].isnull(),'Site_name']

	texno_3G_util_site['Date']=pd.to_datetime(texno_3G_util_site['Date'],dayfirst=True)
	texno_3G_util_site['Day']=texno_3G_util_site['Date'].dt.date
	texno_3G_util_site['Minute']=texno_3G_util_site['Date'].dt.minute
	texno_3G_util_site['Hour']=texno_3G_util_site['Date'].dt.hour

	#texno_3G_util_site=texno_3G_util_site.merge(cluster,left_on='WBTS_name',right_on='Site_name',how='left')
	texno_3G_util_site['Site']=texno_3G_util_site['WBTS_name'].apply(lambda x: x[1:8])
	#texno_3G_util_site[['LIC_HSDPA_THR','LIC_HSUPA_THR','LIC_NUM_HSDPA_USERS']].fillna(1,inplace=True)
	texno_3G_util_site_grp = texno_3G_util_site.groupby(['Date','Site']).sum().reset_index().drop(columns=['Hour','Minute'])
	texno_3G_util_site_grp=texno_3G_util_site_grp.merge(cluster,left_on='Site',right_on='Site_name',how='outer')
	texno_3G_util_site_grp.loc[texno_3G_util_site_grp['Site'].isnull(),'Site']=texno_3G_util_site_grp.loc[texno_3G_util_site_grp['Site'].isnull(),'Site_name']

	texno_3G_util_lcg['Date']=pd.to_datetime(texno_3G_util_lcg['Date'],dayfirst=True)
	texno_3G_util_lcg['Day']=texno_3G_util_lcg['Date'].dt.date
	texno_3G_util_lcg['Minute']=texno_3G_util_lcg['Date'].dt.minute
	texno_3G_util_lcg['Hour']=texno_3G_util_lcg['Date'].dt.hour

	#texno_3G_util_lcg=texno_3G_util_lcg.merge(cluster,left_on='WBTS_name',right_on='Site_name',how='left')
	texno_3G_util_lcg['Site']=texno_3G_util_lcg['WBTS_name'].apply(lambda x: x[1:8])
	texno_3G_util_lcg_grp = texno_3G_util_lcg.groupby(['Date','Site']).sum().reset_index().drop(columns=['Hour','Minute'])
	texno_3G_util_lcg_grp=texno_3G_util_lcg_grp.merge(cluster,left_on='Site',right_on='Site_name',how='outer')
	texno_3G_util_lcg_grp.loc[texno_3G_util_lcg_grp['Site'].isnull(),'Site']=texno_3G_util_lcg_grp.loc[texno_3G_util_lcg_grp['Site'].isnull(),'Site_name']

	texno_3G_grp['Day']=texno_3G_grp['Date'].dt.date
	texno_3G_grp['Hour']=texno_3G_grp['Date'].dt.hour
	texno_3G_grp['Minute']=texno_3G_grp['Date'].dt.minute
	texno_3G_grp['Date']=texno_3G_grp['Date'].astype(str)
	texno_2G_grp['Date']=texno_2G_grp['Date'].astype(str)
	texno_4G_grp['Date']=texno_4G_grp['Date'].astype(str)
	texno_5G_grp['Date']=texno_5G_grp['Date'].astype(str)
	texno_3G_util_cell_grp['Date']=texno_3G_util_cell_grp['Date'].astype(str)
	texno_3G_util_site_grp['Date']=texno_3G_util_site_grp['Date'].astype(str)
	texno_3G_util_lcg_grp['Date']=texno_3G_util_lcg_grp['Date'].astype(str)

	texno_2G_grp.drop(columns='Site_name',inplace=True)
	texno_3G_grp.drop(columns='Site_name',inplace=True)
	texno_4G_grp.drop(columns='Site_name',inplace=True)
	texno_5G_grp.drop(columns='Site_name',inplace=True)
	texno_3G_util_cell_grp.drop(columns='Site_name',inplace=True)
	texno_3G_util_site_grp.drop(columns='Site_name',inplace=True)
	texno_3G_util_lcg_grp.drop(columns='Site_name',inplace=True)

	return texno_2G_grp, texno_3G_grp, texno_4G_grp, texno_3G_util_cell_grp, texno_3G_util_site_grp, texno_3G_util_lcg_grp,texno_5G_grp

texno_2G_grp, texno_3G_grp, texno_4G_grp, texno_3G_util_cell_grp, \
	texno_3G_util_site_grp, texno_3G_util_lcg_grp,texno_5G_grp = process(texno_2G,texno_3G,texno_4G,texno_3G_util_cell,texno_3G_util_site,texno_3G_util_lcg,texno_5G)

#t_two = session.read_pandas(texno_2G,table_name='Texnofest_2G_cell',keys=['Date','Site_name','Cell_name'])
#t_three = session.read_pandas(texno_3G,table_name='Texnofest_3G_cell',keys=['Date','Site_name','Cell_name'])
#t_four = session.read_pandas(texno_4G,table_name='Texnofest_4G_cell',keys=['Date','Site_name','Cell_name'])
#t_three_uc = session.read_pandas(texno_3G_util_cell,table_name='Texnofest_3G_util_cell',keys=['Date','WBTS_name','WCEL_name'])
#t_three_us = session.read_pandas(texno_3G_util_site,table_name='Texnofest_3G_util_site',keys=['Date','WBTS_name'])
#t_three_ul = session.read_pandas(texno_3G_util_lcg,table_name='Texnofest_3G_util_lcg',keys=['Date','WBTS_name'])
t_two_grp = session.read_pandas(texno_2G_grp,table_name='Texnofest_2G_grouped',keys=['Date','Cluster','Site'])
t_three_grp = session.read_pandas(texno_3G_grp,table_name='Texnofest_cluster',keys=['Date','Cluster','Site'])
t_four_grp = session.read_pandas(texno_4G_grp,table_name='Texnofest_4G_grouped',keys=['Date','Cluster','Site'])
t_three_uc_grp = session.read_pandas(texno_3G_util_cell_grp,table_name='Texnofest_3G_util_cell_grp',keys=['Date','Cluster','Site'])
t_three_us_grp = session.read_pandas(texno_3G_util_site_grp,table_name='Texnofest_3G_util_site_grp',keys=['Date','Cluster','Site'])
t_three_ul_grp = session.read_pandas(texno_3G_util_lcg_grp,table_name='Texnofest_3G_util_lcg_grp',keys=['Date','Cluster','Site'])
t_five_grp = session.read_pandas(texno_5G_grp,table_name='Texnofest_5G_grouped',keys=['Date','Cluster','Site'])

t_three_grp.join(t_two_grp, mapping={'Date':'Date','Cluster':'Cluster','Site':'Site'})
t_three_grp.join(t_four_grp, mapping={'Date':'Date','Cluster':'Cluster','Site':'Site'})
t_three_grp.join(t_three_uc_grp, mapping={'Date':'Date','Cluster':'Cluster','Site':'Site'})
t_three_grp.join(t_three_us_grp, mapping={'Date':'Date','Cluster':'Cluster','Site':'Site'})
t_three_grp.join(t_three_ul_grp, mapping={'Date':'Date','Cluster':'Cluster','Site':'Site'})
t_three_grp.join(t_five_grp, mapping={'Date':'Date','Cluster':'Cluster','Site':'Site'})

#cube2=session.create_cube(t_two)
#cube3=session.create_cube(t_three)
#cube4=session.create_cube(t_four)
#cube5=session.create_cube(t_three_uc)
#cube6=session.create_cube(t_three_us)
#cube7=session.create_cube(t_three_ul)
cube8=session.create_cube(t_three_grp)
t_three_grp['Hour'].default_value=10
t_three_grp['Minute'].default_value=10

add_measures()

from watchdog.observers.polling import PollingObserver
from watchdog.events import FileSystemEventHandler, FileCreatedEvent, FileModifiedEvent
#import glob


class AtotiWatcher(FileSystemEventHandler):
	def on_modified(self, event: FileModifiedEvent):
		try:
			print(event.src_path)
			#pd.DataFrame(os.listdir("/disk2/support_files/atoti/bi")).to_csv('/disk2/support_files/atoti/filesss.csv',index=False,mode='a')
			two_gt=pd.read_csv("/disk2/support_files/archive/twoG.csv")
			three_gt=pd.read_csv("/disk2/support_files/archive/threeG.csv")
			four_gt=pd.read_csv("/disk2/support_files/archive/fourG.csv")
			three_gt_cell=pd.read_csv('/disk2/support_files/archive/threeG_util_cell.csv')
			three_gt_site=pd.read_csv('/disk2/support_files/archive/threeG_util_site.csv')
			three_gt_lcg=pd.read_csv('/disk2/support_files/archive/threeG_util_lcg.csv')
			five_gt=pd.read_csv("/disk2/support_files/archive/fiveG.csv")
			
			
			texno_2G_grp, texno_3G_grp, texno_4G_grp, texno_3G_util_cell_grp, \
	texno_3G_util_site_grp, texno_3G_util_lcg_grp,texno_5G_grp = process(two_gt,three_gt,four_gt,three_gt_cell,three_gt_site,three_gt_lcg,five_gt)
			
			t_two_grp.load_pandas(texno_2G_grp)
			t_three_grp.load_pandas(texno_3G_grp)
			t_four_grp.load_pandas(texno_4G_grp)
			t_three_uc_grp.load_pandas(texno_3G_util_cell_grp)
			t_three_us_grp.load_pandas(texno_3G_util_site_grp)
			t_three_ul_grp.load_pandas(texno_3G_util_lcg_grp)
			t_five_grp.load_pandas(texno_5G_grp)
			


			#two_gt=pd.read_csv("/disk2/support_files/archive/twoG.csv")
			##two.load_csv("/disk2/support_files/atoti/bi/2G*.csv")
			#two_gt['Date']=pd.to_datetime(two_gt['Date'])
			#two_gt['Day']=two_gt['Date'].dt.date
			#two_gt['Minute']=two_gt['Date'].dt.minute
			#two_gt['Hour']=two_gt['Date'].dt.hour

			#t_two.load_pandas(two_gt)
						
			##os.remove("disk2/support/files/atoti/bi/2G*.csv")
			##three.load_csv("/disk2/support_files/atoti/bi/3G*.csv")
			#three_gt=pd.read_csv("/disk2/support_files/archive/threeG.csv")
			#three_gt['Date']=pd.to_datetime(three_gt['Date'])
			#three_gt['Day']=three_gt['Date'].dt.date
			#three_gt['Minute']=three_gt['Date'].dt.minute
			#three_gt['Hour']=three_gt['Date'].dt.hour
			#t_three.load_pandas(three_gt)
			##os.remove("disk2/support/files/atoti/bi/3G*.csv")
			##four.load_csv("/disk2/support_files/atoti/bi/4G*.csv")
			#four_gt=pd.read_csv("/disk2/support_files/archive/fourG.csv")
			#four_gt['Date']=pd.to_datetime(four_gt['Date'])
			#four_gt['Day']=four_gt['Date'].dt.date
			#four_gt['Month']=four_gt['Date'].dt.month
			#four_gt['Year']=four_gt['Date'].dt.year.astype(str)
			#t_four.load_pandas(four_gt)
			##os.remove("disk2/support/files/atoti/bi/4G*.csv")

		except Exception as error:
			print(error)
			1
		#	with open('/disk2/support_files/atoti/readme.txt', 'w') as f: [f.write(i) for i in error]
			#pd.DataFrame(list(error)).to_csv('/disk2/support_files/atoti/error.csv',index=False,mode='a')


observer = PollingObserver()
observer.schedule(AtotiWatcher(), "/disk2/support_files/archive/fiveG.csv")
observer.start()

try:
	while True:	
		time.sleep(10)
		print(observer.is_alive())
finally:
	observer.stop()
	observer.join()

#if __name__ == '__main__':
#    run()

