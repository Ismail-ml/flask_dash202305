import os
import pandas as pd
import glob,time
import dask.dataframe as dd
import numpy as np
import datetime

def run(path1,path2,tracker):
    print('Nokia 3G aggregation begin...')

    c1=time.time()

    nokia_3G_1={'WCEL name': 'object','CELL_UPDATE_ATT_CS_CALL (M1006C253)': 'float64',       'CELL_UPDATE_SUCC_CS_CALL (M1006C254)': 'float64',
           'D_D_REQ_D_D_ALLO_BGR (M1022C32)': 'float64',       'D_D_REQ_D_D_ALLO_INT (M1022C31)': 'float64',
           'D_D_REQ_D_D_ALLO_STRE (M1022C195)': 'float64',       'HS_D_REQ_D_D_ALLO_BGR_CELL (M1022C221)': 'float64',
           'HS_D_REQ_D_D_ALLO_INT_CELL (M1022C220)': 'float64',       'HS_D_REQ_D_D_ALLO_STR_CELL (M1022C222)': 'float64',
           'HS_D_REQ_HS_D_ALLO_BGR (M1022C26)': 'float64',       'HS_D_REQ_HS_D_ALLO_INT (M1022C25)': 'float64',
           'HS_D_REQ_HS_D_ALLO_STRE (M1022C192)': 'float64',       'HS_E_REQ_D_D_ALLO_BGR_CELL (M1022C218)': 'float64',
           'HS_E_REQ_D_D_ALLO_INT_CELL (M1022C217)': 'float64',       'HS_E_REQ_D_D_ALLO_STR_CELL (M1022C219)': 'float64',
           'HS_E_REQ_HS_D_ALLO_BGR (M1022C24)': 'float64',       'HS_E_REQ_HS_D_ALLO_BGR_CELL (M1022C215)': 'float64',
           'HS_E_REQ_HS_D_ALLO_INT (M1022C23)': 'float64',       'HS_E_REQ_HS_D_ALLO_INT_CELL (M1022C214)': 'float64',
           'HS_E_REQ_HS_D_ALLO_STRE (M1022C191)': 'float64',       'HS_E_REQ_HS_D_ALLO_STR_CELL (M1022C216)': 'float64',
           'HS_E_REQ_HS_E_ALLO_BGR (M1022C22)': 'float64',       'HS_E_REQ_HS_E_ALLO_INT (M1022C21)': 'float64',
           'HS_E_REQ_HS_E_ALLO_STRE (M1022C190)': 'float64',       'PS_ATT_DCH_DCH_BGR (M1022C8)': 'float64',
           'PS_ATT_DCH_DCH_INT (M1022C7)': 'float64',       'PS_ATT_DCH_DCH_STRE (M1022C183)': 'float64',
           'PS_ATT_HSDSCH_DCH_BGR (M1022C6)': 'float64',       'PS_ATT_HSDSCH_DCH_INT (M1022C5)': 'float64',
           'PS_ATT_HSDSCH_DCH_STRE (M1022C182)': 'float64',       'PS_ATT_HSDSCH_EDCH_BGR (M1022C4)': 'float64',
           'PS_ATT_HSDSCH_EDCH_INT (M1022C3)': 'float64',       'PS_ATT_HSDSCH_EDCH_STRE (M1022C181)': 'float64',
           'PS_REL_NORM_D_D_BGR (M1022C50)': 'float64',       'PS_REL_NORM_D_D_INT (M1022C49)': 'float64',
           'PS_REL_NORM_D_D_STRE (M1022C204)': 'float64',       'PS_REL_NORM_HS_D_BGR (M1022C48)': 'float64',
           'PS_REL_NORM_HS_D_INT (M1022C47)': 'float64',       'PS_REL_NORM_HS_D_STRE (M1022C203)': 'float64',
           'PS_REL_NORM_HS_E_BGR (M1022C46)': 'float64',       'PS_REL_NORM_HS_E_INT (M1022C45)': 'float64',
           'PS_REL_NORM_HS_E_STRE (M1022C202)': 'float64',       'PS_REL_OTH_FAIL_D_D_BGR (M1022C68)': 'float64',
           'PS_REL_OTH_FAIL_D_D_INT (M1022C67)': 'float64',       'PS_REL_OTH_FAIL_D_D_STRE (M1022C213)': 'float64',
           'PS_REL_OTH_FAIL_HS_D_BGR (M1022C66)': 'float64',       'PS_REL_OTH_FAIL_HS_D_INT (M1022C65)': 'float64',
           'PS_REL_OTH_FAIL_HS_D_STRE (M1022C212)': 'float64',       'PS_REL_OTH_FAIL_HS_E_BGR (M1022C64)': 'float64',
           'PS_REL_OTH_FAIL_HS_E_INT (M1022C63)': 'float64',       'PS_REL_OTH_FAIL_HS_E_STRE (M1022C211)': 'float64',
           'PS_REL_RL_FAIL_D_D_BGR (M1022C62)': 'float64',       'PS_REL_RL_FAIL_D_D_INT (M1022C61)': 'float64',
           'PS_REL_RL_FAIL_D_D_STRE (M1022C210)': 'float64',       'PS_REL_RL_FAIL_HS_D_BGR (M1022C60)': 'float64',
           'PS_REL_RL_FAIL_HS_D_INT (M1022C59)': 'float64',       'PS_REL_RL_FAIL_HS_D_STRE (M1022C209)': 'float64',
           'PS_REL_RL_FAIL_HS_E_BGR (M1022C58)': 'float64',       'PS_REL_RL_FAIL_HS_E_INT (M1022C57)': 'float64',
           'PS_REL_RL_FAIL_HS_E_STRE (M1022C208)': 'float64',       'PS_SWI_D_D_TO_HS_D_BGR (M1022C42)': 'float64',
           'PS_SWI_D_D_TO_HS_D_INT (M1022C41)': 'float64',       'PS_SWI_D_D_TO_HS_D_STRE (M1022C200)': 'float64',
           'PS_SWI_D_D_TO_HS_E_BGR (M1022C44)': 'float64',       'PS_SWI_D_D_TO_HS_E_INT (M1022C43)': 'float64',
           'PS_SWI_D_D_TO_HS_E_STRE (M1022C201)': 'float64',       'PS_SWI_HS_D_TO_D_D_BGR (M1022C38)': 'float64',
           'PS_SWI_HS_D_TO_D_D_INT (M1022C37)': 'float64',       'PS_SWI_HS_D_TO_D_D_STRE (M1022C198)': 'float64',
           'PS_SWI_HS_E_TO_D_D_BGR (M1022C36)': 'float64',       'PS_SWI_HS_E_TO_D_D_INT (M1022C35)': 'float64',
           'PS_SWI_HS_E_TO_D_D_STRE (M1022C197)': 'float64',       'PS_SWI_HS_E_TO_HS_D_BGR (M1022C34)': 'float64',
           'PS_SWI_HS_E_TO_HS_D_INT (M1022C33)': 'float64',       'PS_SWI_HS_E_TO_HS_D_STRE (M1022C196)': 'float64',
           'RAB_ACT_COMP_CS_VOICE_SAMECE (M1001C248)': 'float64',       'RAB_ACT_FAIL_CS_VOICE_TRANS (M1001C690)': 'float64',
           'RAB_ACT_FAIL_CS_VOICE_UE (M1001C392)': 'float64',       'RAB_ACT_REL_CS_VOICE_GANHO (M1001C650)': 'float64',
           'RAB_ACT_REL_CS_VOICE_HHO (M1001C644)': 'float64',       'RAB_ACT_REL_CS_VOICE_ISHO (M1001C647)': 'float64',
           'RRC_ACC_REL_EMERGENCY (M1001C562)': 'float64',       'RRC_ACC_REL_MO_CONV (M1001C553)': 'float64',
           'RRC_ACC_REL_MT_CONV (M1001C558)': 'float64',       'RRC_ATT_REP_EMERGENCY (M1001C582)': 'float64',
           'RRC_ATT_REP_MO_CONV (M1001C573)': 'float64',       'RRC_ATT_REP_MT_CONV (M1001C578)': 'float64',
           'RRC_CONN_STP_REJ_EMERG_CALL (M1001C617)': 'float64',       'emergency_call_atts (M1001C40)': 'float64',
           'emergency_call_fails (M1001C41)': 'float64',       'moc_conv_call_atts (M1001C22)': 'float64',
           'moc_conv_call_fails (M1001C23)': 'float64',       'mtc_conv_call_atts (M1001C32)': 'float64',
           'mtc_conv_call_fails (M1001C33)': 'float64',       'rab_acc_comp_cs_voice (M1001C115)': 'float64',
           'rab_act_comp_cs_voice (M1001C136)': 'float64',       'rab_act_fail_cs_voice_bts (M1001C147)': 'float64',
           'rab_act_fail_cs_voice_iu (M1001C145)': 'float64',       'rab_act_fail_cs_voice_iur (M1001C148)': 'float64',
           'rab_act_fail_cs_voice_radio (M1001C146)': 'float64',       'rab_act_fail_cs_voice_rnc (M1001C150)': 'float64',
           'rab_act_rel_cs_voice_p_emp (M1001C144)': 'float64',       'rab_act_rel_cs_voice_srnc (M1001C143)': 'float64',
           'rab_stp_att_cs_voice (M1001C66)': 'float64','AVAIL_WCELL_BLOCKED_BY_USER (M1000C179)': 'float64',
       'AVAIL_WCELL_EXISTS_IN_RNW_DB (M1000C180)': 'float64', 'AVAIL_WCELL_IN_WO_STATE (M1000C178)': 'float64'}

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
                df = dd.read_csv(filename,sep=';',dtype=nokia_3G_1)
                if len(df)<10: continue
                df[['RAB_HOLD_TIME_CS_CONV_64 (M1001C368)','RAB_HOLD_TIME_CS_STREAM_14_4 (M1001C370)',
                   'RAB_HOLD_TIME_CS_STREAM_57_6 (M1001C372)']]=df[['RAB_HOLD_TIME_CS_CONV_64 (M1001C368)','RAB_HOLD_TIME_CS_STREAM_14_4 (M1001C370)',
                   'RAB_HOLD_TIME_CS_STREAM_57_6 (M1001C372)']].fillna(0)
                df['cs_inter_freq_ho_num']=df['succ_intra_inter_hho_att_rt (M1008C59)']+df['succ_inter_hho_att_rt (M1008C63)']+df['succ_intra_intra_hho_att_rt (M1008C55)']
                df['cs_inter_freq_ho_den']=df['intra_intra_hho_att_rt (M1008C54)']+df['intra_inter_hho_att_rt (M1008C58)']+df['inter_hho_att_rt (M1008C62)']
                df['ps_inter_freq_ho_num']=df['succ_intra_intra_hho_att_nrt (M1008C103)']+df['succ_intra_inter_hho_att_nrt (M1008C107)']+df['succ_inter_hho_att_nrt (M1008C111)']
                df['ps_inter_freq_ho_den']=df['intra_intra_hho_att_nrt (M1008C102)']+df['intra_inter_hho_att_nrt (M1008C106)']+df['inter_hho_att_nrt (M1008C110)']
                df['cs_sho_ho_num']=df['succ_updates_on_sho_for_rt (M1007C15)']
                df['cs_sho_ho_den']=df['cell_add_req_on_sho_for_rt (M1007C10)']+df['cell_repl_req_on_sho_for_rt (M1007C12)']+df['cell_del_req_on_sho_for_rt (M1007C11)']
                df['ps_sho_ho_num']=df['succ_updates_on_sho_for_nrt (M1007C32)']
                df['ps_sho_ho_den']=df['cell_add_req_on_sho_for_nrt (M1007C27)']+df['cell_repl_req_on_sho_for_nrt (M1007C29)']+df['cell_del_req_on_sho_for_nrt (M1007C28)']
                df['cs_inter_rat_ho_num']=df['succ_is_hho_ul_dch_q_rt (M1010C19)']+df['succ_is_hho_ue_trx_pwr_rt (M1010C23)']+df['succ_is_hho_dl_dpch_pwr_rt (M1010C27)']+df['succ_is_hho_cpich_rscp_rt (M1010C31)']+df['succ_is_hho_cpich_ecno_rt (M1010C35)']+df['SUCC_IS_HHO_IM_IMS_RT (M1010C77)']+df['SUCC_IS_HHO_EMERG_CALL (M1010C98)']+df['SUCC_IS_HHO_LB_CAPA_DL_RT (M1010C198)']+df['SUCC_IS_HHO_LB_CAPA_UL_RT (M1010C197)']+df['SUCC_IS_HHO_LB_PRX_TOT_RT (M1010C149)']+df['SUCC_IS_HHO_LB_PTX_TOT_RT (M1010C150)']+df['SUCC_IS_HHO_LB_RES_LIM_RT (M1010C152)']+df['SUCC_IS_HHO_LB_RSVR_SC_RT (M1010C151)']+df['SUCC_IS_HHO_SB_RT (M1010C153)']+df['SUCC_IS_HHO_WPS_RT (M1010C186)']+df['SUCC_GANHO_AMR_RT (M1010C220)']+df['SUCC_ISHO_CELL_SHUTDOWN_RT (M1010C228)']+df['SUCC_ISHO_CELL_BLOCK_RT (M1010C236)']
                df['cs_inter_rat_ho_den']=df['is_hho_att_ul_dch_q_rt (M1010C18)']+df['is_hho_att_ue_trx_pwr_rt (M1010C22)']+df['is_hho_att_dpch_pwr_rt (M1010C26)']+df['is_hho_att_cpich_rscp_rt (M1010C30)']+df['is_hho_att_cpich_ecno_rt (M1010C34)']+df['IS_HHO_ATT_IM_IMS_RT (M1010C76)']+df['IS_HHO_ATT_EMERG_CALL (M1010C97)']+df['IS_HHO_ATT_LB_CAPA_DL_RT (M1010C196)']+df['IS_HHO_ATT_LB_CAPA_UL_RT (M1010C195)']+df['IS_HHO_ATT_LB_PRX_TOT_RT (M1010C137)']+df['IS_HHO_ATT_LB_PTX_TOT_RT (M1010C138)']+df['IS_HHO_ATT_LB_RES_LIM_RT (M1010C140)']+df['IS_HHO_ATT_LB_RSVR_SC_RT (M1010C139)']+df['IS_HHO_ATT_SB_RT (M1010C141)']+df['IS_HHO_ATT_UL_DCH_WPS_RT (M1010C185)']+df['ATT_GANHO_AMR_RT (M1010C219)']+df['ATT_ISHO_CELL_SHUTDOWN_RT (M1010C226)']+df['ATT_ISHO_CELL_BLOCK_RT (M1010C234)']-df['IS_HHO_ATT_2ND_BEST_CELL_RT (M1010C229)']-df['IS_HHO_ATT_3RD_BEST_CELL_RT (M1010C230)']
                df['hsdpa_sr_num']=df['HS_E_REQ_HS_E_ALLO_STRE (M1022C190)']+df['HS_E_REQ_HS_E_ALLO_INT (M1022C21)']+df['HS_E_REQ_HS_E_ALLO_BGR (M1022C22)']+df['HS_E_REQ_HS_D_ALLO_STRE (M1022C191)']+df['HS_E_REQ_HS_D_ALLO_INT (M1022C23)']+df['HS_E_REQ_HS_D_ALLO_BGR (M1022C24)']+df['HS_D_REQ_HS_D_ALLO_STRE (M1022C192)']+df['HS_D_REQ_HS_D_ALLO_INT (M1022C25)']+df['HS_D_REQ_HS_D_ALLO_BGR (M1022C26)']
                df['hsdpa_sr_den']=df['PS_ATT_HSDSCH_EDCH_STRE (M1022C181)']+df['PS_ATT_HSDSCH_EDCH_INT (M1022C3)']+df['PS_ATT_HSDSCH_EDCH_BGR (M1022C4)']+df['PS_ATT_HSDSCH_DCH_STRE (M1022C182)']+df['PS_ATT_HSDSCH_DCH_INT (M1022C5)']+df['PS_ATT_HSDSCH_DCH_BGR (M1022C6)']-df['HS_D_REQ_D_D_ALLO_STR_CELL (M1022C222)']-df['HS_D_REQ_D_D_ALLO_BGR_CELL (M1022C221)']-df['HS_D_REQ_D_D_ALLO_INT_CELL (M1022C220)']-df['HS_E_REQ_D_D_ALLO_STR_CELL (M1022C219)']-df['HS_E_REQ_D_D_ALLO_BGR_CELL (M1022C218)']-df['HS_E_REQ_D_D_ALLO_INT_CELL (M1022C217)']
                df['hsupa_sr_num']=df['HS_E_REQ_HS_E_ALLO_STRE (M1022C190)']+df['HS_E_REQ_HS_E_ALLO_INT (M1022C21)']+df['HS_E_REQ_HS_E_ALLO_BGR (M1022C22)']
                df['hsupa_sr_den']=df['PS_ATT_HSDSCH_EDCH_STRE (M1022C181)']+df['PS_ATT_HSDSCH_EDCH_INT (M1022C3)']+df['PS_ATT_HSDSCH_EDCH_BGR (M1022C4)']-df['HS_E_REQ_HS_D_ALLO_STR_CELL (M1022C216)']-df['HS_E_REQ_HS_D_ALLO_BGR_CELL (M1022C215)']-df['HS_E_REQ_HS_D_ALLO_INT_CELL (M1022C214)']-df['HS_E_REQ_D_D_ALLO_STR_CELL (M1022C219)']-df['HS_E_REQ_D_D_ALLO_BGR_CELL (M1022C218)']-df['HS_E_REQ_D_D_ALLO_INT_CELL (M1022C217)']
                df['hsdpa_dr_num']=df['PS_REL_RL_FAIL_HS_E_STRE (M1022C208)']+df['PS_REL_RL_FAIL_HS_E_INT (M1022C57)']+df['PS_REL_RL_FAIL_HS_E_BGR (M1022C58)']+df['PS_REL_RL_FAIL_HS_D_STRE (M1022C209)']+df['PS_REL_RL_FAIL_HS_D_INT (M1022C59)']+df['PS_REL_RL_FAIL_HS_D_BGR (M1022C60)']+df['PS_REL_OTH_FAIL_HS_E_STRE (M1022C211)']+df['PS_REL_OTH_FAIL_HS_E_INT (M1022C63)']+df['PS_REL_OTH_FAIL_HS_E_BGR (M1022C64)']+df['PS_REL_OTH_FAIL_HS_D_STRE (M1022C212)']+df['PS_REL_OTH_FAIL_HS_D_INT (M1022C65)']+df['PS_REL_OTH_FAIL_HS_D_BGR (M1022C66)']
                df['hsdpa_dr_den']=df['PS_REL_RL_FAIL_HS_E_STRE (M1022C208)']+df['PS_REL_RL_FAIL_HS_E_INT (M1022C57)']+df['PS_REL_RL_FAIL_HS_E_BGR (M1022C58)']+df['PS_REL_RL_FAIL_HS_D_STRE (M1022C209)']+df['PS_REL_RL_FAIL_HS_D_INT (M1022C59)']+df['PS_REL_RL_FAIL_HS_D_BGR (M1022C60)']+df['PS_REL_OTH_FAIL_HS_E_STRE (M1022C211)']+df['PS_REL_OTH_FAIL_HS_E_INT (M1022C63)']+df['PS_REL_OTH_FAIL_HS_E_BGR (M1022C64)']+df['PS_REL_OTH_FAIL_HS_D_STRE (M1022C212)']+df['PS_REL_OTH_FAIL_HS_D_INT (M1022C65)']+df['PS_REL_OTH_FAIL_HS_D_BGR (M1022C66)']+df['PS_REL_NORM_HS_E_STRE (M1022C202)']+df['PS_REL_NORM_HS_E_INT (M1022C45)']+df['PS_REL_NORM_HS_E_BGR (M1022C46)']+df['PS_REL_NORM_HS_D_STRE (M1022C203)']+df['PS_REL_NORM_HS_D_INT (M1022C47)']+df['PS_REL_NORM_HS_D_BGR (M1022C48)']+df['PS_SWI_HS_E_TO_D_D_STRE (M1022C197)']+df['PS_SWI_HS_E_TO_D_D_INT (M1022C35)']+df['PS_SWI_HS_E_TO_D_D_BGR (M1022C36)']+df['PS_SWI_HS_D_TO_D_D_STRE (M1022C198)']+df['PS_SWI_HS_D_TO_D_D_INT (M1022C37)']+df['PS_SWI_HS_D_TO_D_D_BGR (M1022C38)']
                df['hsupa_dr_num']=df['PS_REL_RL_FAIL_HS_E_STRE (M1022C208)']+df['PS_REL_RL_FAIL_HS_E_INT (M1022C57)']+df['PS_REL_RL_FAIL_HS_E_BGR (M1022C58)']+df['PS_REL_OTH_FAIL_HS_E_STRE (M1022C211)']+df['PS_REL_OTH_FAIL_HS_E_INT (M1022C63)']+df['PS_REL_OTH_FAIL_HS_E_BGR (M1022C64)']
                df['hsupa_dr_den']=df['PS_REL_RL_FAIL_HS_E_STRE (M1022C208)']+df['PS_REL_RL_FAIL_HS_E_INT (M1022C57)']+df['PS_REL_RL_FAIL_HS_E_BGR (M1022C58)']+df['PS_REL_OTH_FAIL_HS_E_STRE (M1022C211)']+df['PS_REL_OTH_FAIL_HS_E_INT (M1022C63)']+df['PS_REL_OTH_FAIL_HS_E_BGR (M1022C64)']+df['PS_REL_NORM_HS_E_STRE (M1022C202)']+df['PS_REL_NORM_HS_E_INT (M1022C45)']+df['PS_REL_NORM_HS_E_BGR (M1022C46)']+df['PS_SWI_HS_E_TO_D_D_STRE (M1022C197)']+df['PS_SWI_HS_E_TO_D_D_INT (M1022C35)']+df['PS_SWI_HS_E_TO_D_D_BGR (M1022C36)']+df['PS_SWI_HS_E_TO_HS_D_STRE (M1022C196)']+df['PS_SWI_HS_E_TO_HS_D_INT (M1022C33)']+df['PS_SWI_HS_E_TO_HS_D_BGR (M1022C34)']
                df['voice_sr_num1']=df['moc_conv_call_atts (M1001C22)']-df['moc_conv_call_fails (M1001C23)']+df['mtc_conv_call_atts (M1001C32)']-df['mtc_conv_call_fails (M1001C33)']+df['emergency_call_atts (M1001C40)']-df['emergency_call_fails (M1001C41)']-df['RRC_ACC_REL_EMERGENCY (M1001C562)']-df['RRC_ACC_REL_MO_CONV (M1001C553)']-df['RRC_ACC_REL_MT_CONV (M1001C558)']+df['CELL_UPDATE_SUCC_CS_CALL (M1006C254)']
                df['voice_sr_den1']=df['moc_conv_call_atts (M1001C22)']+df['mtc_conv_call_atts (M1001C32)']+df['emergency_call_atts (M1001C40)']-df['RRC_ATT_REP_MO_CONV (M1001C573)']-df['RRC_ATT_REP_MT_CONV (M1001C578)']-df['RRC_ATT_REP_EMERGENCY (M1001C582)']-df['RRC_ACC_REL_EMERGENCY (M1001C562)']-df['RRC_ACC_REL_MO_CONV (M1001C553)']-df['RRC_ACC_REL_MT_CONV (M1001C558)']-df['RRC_CONN_STP_REJ_EMERG_CALL (M1001C617)']+df['CELL_UPDATE_ATT_CS_CALL (M1006C253)']
                df['voice_sr_num2']=df['rab_acc_comp_cs_voice (M1001C115)']
                df['voice_sr_den2']=df['rab_stp_att_cs_voice (M1001C66)']
                df['voice_dr_num']=df['rab_act_rel_cs_voice_p_emp (M1001C144)']+df['rab_act_fail_cs_voice_iu (M1001C145)']+df['rab_act_fail_cs_voice_radio (M1001C146)']+df['rab_act_fail_cs_voice_bts (M1001C147)']+df['rab_act_fail_cs_voice_iur (M1001C148)']+df['rab_act_fail_cs_voice_rnc (M1001C150)']+df['RAB_ACT_FAIL_CS_VOICE_UE (M1001C392)']+df['RAB_ACT_FAIL_CS_VOICE_TRANS (M1001C690)']
                df['voice_dr_den']=df['rab_act_comp_cs_voice (M1001C136)']+df['rab_act_rel_cs_voice_srnc (M1001C143)']+df['rab_act_rel_cs_voice_p_emp (M1001C144)']+df['RAB_ACT_REL_CS_VOICE_HHO (M1001C644)']+df['RAB_ACT_REL_CS_VOICE_ISHO (M1001C647)']+df['RAB_ACT_REL_CS_VOICE_GANHO (M1001C650)']+df['rab_act_fail_cs_voice_iu (M1001C145)']+df['rab_act_fail_cs_voice_radio (M1001C146)']+df['rab_act_fail_cs_voice_bts (M1001C147)']+df['rab_act_fail_cs_voice_iur (M1001C148)']+df['rab_act_fail_cs_voice_rnc (M1001C150)']+df['RAB_ACT_FAIL_CS_VOICE_UE (M1001C392)']+df['RAB_ACT_FAIL_CS_VOICE_TRANS (M1001C690)']
                df['cell_avail_num']=5*df['AVAIL_WCELL_IN_WO_STATE (M1000C178)']
                df['cell_avail_den']=5*df['AVAIL_WCELL_EXISTS_IN_RNW_DB (M1000C180)']
                df['cell_avail_blck_den']=5*df['AVAIL_WCELL_BLOCKED_BY_USER (M1000C179)']
                df['cell_avail_blck_num']=0
                df['cs_traf']=(df['avg_rab_hld_tm_cs_voice (M1001C199)']+(df['RAB_HOLD_TIME_CS_CONV_64 (M1001C368)']*64+df['RAB_HOLD_TIME_CS_STREAM_14_4 (M1001C370)']*14.4+df['RAB_HOLD_TIME_CS_STREAM_57_6 (M1001C372)']*57.6)/12.2)/(60*100*60)
                df['ps_traf']=(df['HS_DSCH_DATA_VOL (M1023C8)']+df['RT_HS_DSCH_DL_STREA_DATA (M1023C25)']+df['NRT_EDCH_UL_DATA_VOL (M1023C10)']+df['RT_E_DCH_UL_STREA_DATA (M1023C23)']+df['RT_PS_DCH_DL_DATA_VOL (M1023C5)']+df['NRT_DCH_DL_DATA_VOL (M1023C7)']+df['RT_PS_DCH_UL_DATA_VOL (M1023C4)']+df['NRT_DCH_UL_DATA_VOL (M1023C6)']+df['NRT_DCH_HSDPA_UL_DATA_VOL (M1023C9)']+df['RT_DCH_HSDPA_UL_STREA_DATA (M1023C24)'])/(1024*1024)
                df['hsdpa_thrp_num']=(df['RECEIVED_HS_MACD_BITS (M5000C126)']-df['DISCARDED_HS_MACD_BITS (M5000C127)']+(df['MC_HSDPA_ORIG_DATA_PRI (M5002C128)']+df['MC_HSDPA_ORIG_DATA_SEC (M5002C129)'])*8)*500
                df['hsdpa_thrp_den']=df['HSDPA_BUFF_WITH_DATA_PER_TTI (M5000C85)']
                df['r99_sr_num']=df['D_D_REQ_D_D_ALLO_STRE (M1022C195)']+df['D_D_REQ_D_D_ALLO_INT (M1022C31)']+df['D_D_REQ_D_D_ALLO_BGR (M1022C32)']
                df['r99_sr_den']=df['PS_ATT_DCH_DCH_STRE (M1022C183)']+df['PS_ATT_DCH_DCH_INT (M1022C7)']+df['PS_ATT_DCH_DCH_BGR (M1022C8)']
                df['r99_dr_num']=df['PS_REL_RL_FAIL_D_D_STRE (M1022C210)']+df['PS_REL_RL_FAIL_D_D_INT (M1022C61)']+df['PS_REL_RL_FAIL_D_D_BGR (M1022C62)']+df['PS_REL_OTH_FAIL_D_D_INT (M1022C67)']+df['PS_REL_OTH_FAIL_D_D_STRE (M1022C213)']+df['PS_REL_OTH_FAIL_D_D_BGR (M1022C68)']
                df['r99_dr_den']=df['PS_REL_RL_FAIL_D_D_STRE (M1022C210)']+df['PS_REL_RL_FAIL_D_D_INT (M1022C61)']+df['PS_REL_RL_FAIL_D_D_BGR (M1022C62)']+df['PS_REL_OTH_FAIL_D_D_STRE (M1022C213)']+df['PS_REL_OTH_FAIL_D_D_INT (M1022C67)']+df['PS_REL_OTH_FAIL_D_D_BGR (M1022C68)']+df['PS_REL_NORM_D_D_STRE (M1022C204)']+df['PS_REL_NORM_D_D_BGR (M1022C50)']+df['PS_REL_NORM_D_D_INT (M1022C49)']+df['PS_SWI_D_D_TO_HS_E_STRE (M1022C201)']+df['PS_SWI_D_D_TO_HS_E_INT (M1022C43)']+df['PS_SWI_D_D_TO_HS_E_BGR (M1022C44)']+df['PS_SWI_D_D_TO_HS_D_STRE (M1022C200)']+df['PS_SWI_D_D_TO_HS_D_INT (M1022C41)']+df['PS_SWI_D_D_TO_HS_D_BGR (M1022C42)']

                df=df[['WCEL name','WBTS name','PERIOD_START_TIME','RNC name','cs_inter_freq_ho_num','cs_inter_freq_ho_den',
                       'ps_inter_freq_ho_num','ps_inter_freq_ho_den','cs_sho_ho_num','cs_sho_ho_den','ps_sho_ho_num','ps_sho_ho_den',
                       'cs_inter_rat_ho_num','cs_inter_rat_ho_den','hsdpa_sr_num','hsdpa_sr_den','hsupa_sr_num','hsupa_sr_den',
                       'hsdpa_dr_num','hsdpa_dr_den','hsupa_dr_num','hsupa_dr_den','voice_sr_num1','voice_sr_den1',
                       'voice_sr_num2','voice_sr_den2','voice_dr_num','voice_dr_den','cell_avail_num','cell_avail_den',
                       'cell_avail_blck_den','cell_avail_blck_num','cs_traf','ps_traf','hsdpa_thrp_num','hsdpa_thrp_den','r99_sr_num','r99_sr_den',
                       'r99_dr_num','r99_dr_den']]
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
                df = dd.read_csv(filename,sep=';',dtype={'WCEL name': 'object','RAB_STP_ATT_PS_BACKG (M1001C72)': 'float64',
           'RRC_ATT_REP_MT_HIGH_PR_SIGN (M1001C590)': 'float64',       'rab_acc_comp_ps_backg (M1001C121)': 'float64',
           'rab_acc_comp_ps_inter (M1001C120)': 'float64',       'rab_stp_att_ps_inter (M1001C71)': 'float64','RRC_ACC_REL_INTERACTIVE (M1001C560)': 'float64',
       'RRC_ACC_REL_MO_BACKGROUND (M1001C556)': 'float64',       'RRC_ACC_REL_MO_HIGH_PR_SIGN (M1001C567)': 'float64',
       'RRC_ACC_REL_MO_INTERACTIVE (M1001C555)': 'float64',       'RRC_ACC_REL_MT_BACKGROUND (M1001C561)': 'float64',
       'RRC_ACC_REL_MT_HIGH_PR_SIGN (M1001C570)': 'float64',       'RRC_ATT_REP_INTERACTIVE (M1001C580)': 'float64',
       'RRC_ATT_REP_MO_BACKGROUND (M1001C576)': 'float64',       'RRC_ATT_REP_MO_HIGH_PR_SIGN (M1001C587)': 'float64',
       'RRC_ATT_REP_MO_INTERACTIVE (M1001C575)': 'float64',       'RRC_ATT_REP_MT_BACKGROUND (M1001C581)': 'float64'})
                if len(df) < 10: continue
                df['ps_ac_sr_num1']=df['moc_inter_call_atts (M1001C26)']-df['moc_inter_call_fails (M1001C27)']+df['moc_backg_call_atts (M1001C28)']-df['moc_backg_call_fails (M1001C29)']+df['mtc_inter_call_atts (M1001C36)']-df['mtc_inter_call_fails (M1001C37)']+df['mtc_backg_call_atts (M1001C38)']-df['mtc_backg_call_fails (M1001C39)']+df['mtc_high_prior_sign_atts (M1001C52)']-df['mtc_high_prior_sign_fails (M1001C53)']+df['moc_high_prior_sign_atts (M1001C50)']-df['moc_high_prior_sign_fails (M1001C51)']-df['RRC_ACC_REL_INTERACTIVE (M1001C560)']-df['RRC_ACC_REL_MO_BACKGROUND (M1001C556)']-df['RRC_ACC_REL_MO_HIGH_PR_SIGN (M1001C567)']-df['RRC_ACC_REL_MO_INTERACTIVE (M1001C555)']-df['RRC_ACC_REL_MT_BACKGROUND (M1001C561)']-df['RRC_ACC_REL_MT_HIGH_PR_SIGN (M1001C570)']
                df['ps_ac_sr_den1']=df['moc_inter_call_atts (M1001C26)']+df['moc_backg_call_atts (M1001C28)']+df['moc_high_prior_sign_atts (M1001C50)']+df['mtc_inter_call_atts (M1001C36)']+df['mtc_backg_call_atts (M1001C38)']+df['mtc_high_prior_sign_atts (M1001C52)']-df['RRC_ATT_REP_INTERACTIVE (M1001C580)']-df['RRC_ATT_REP_MO_INTERACTIVE (M1001C575)']-df['RRC_ATT_REP_MO_HIGH_PR_SIGN (M1001C587)']-df['RRC_ATT_REP_MO_BACKGROUND (M1001C576)']-df['RRC_ATT_REP_MT_BACKGROUND (M1001C581)']-df['RRC_ATT_REP_MT_HIGH_PR_SIGN (M1001C590)']-df['RRC_ACC_REL_INTERACTIVE (M1001C560)']-df['RRC_ACC_REL_MO_BACKGROUND (M1001C556)']-df['RRC_ACC_REL_MO_HIGH_PR_SIGN (M1001C567)']-df['RRC_ACC_REL_MO_INTERACTIVE (M1001C555)']-df['RRC_ACC_REL_MT_BACKGROUND (M1001C561)']-df['RRC_ACC_REL_MT_HIGH_PR_SIGN (M1001C570)']
                df['ps_ac_sr_num2']=df['rab_acc_comp_ps_inter (M1001C120)']+df['rab_acc_comp_ps_backg (M1001C121)']
                df['ps_ac_sr_den2']=df['rab_stp_att_ps_inter (M1001C71)']+df['RAB_STP_ATT_PS_BACKG (M1001C72)']

                df=df[['WCEL name','WBTS name','RNC name','PERIOD_START_TIME','ps_ac_sr_num1','ps_ac_sr_den1',
                       'ps_ac_sr_num2','ps_ac_sr_den2']]
                li.append(df)
            nsn2 = dd.concat(li, axis=0, ignore_index=True)
        
        #print('len of nsn:',len(nsn))
        an.append(nsn2)
    print(time.time()-c1,'append2 finish')
    nsn2 = dd.concat(an, axis=0, ignore_index=True,sort=False)
    print(time.time()-c1,'concat2 finish')
    nsn2=nsn2.compute()
    print(time.time()-c1,'compute2 finish')


    nsn_agr=pd.merge(nsn,nsn2,on=['WCEL name','WBTS name','RNC name','PERIOD_START_TIME'],how='left')    
    nsn_agr['Date']=pd.to_datetime(nsn_agr['PERIOD_START_TIME'], format='%m.%d.%Y %H:%M:%S')
    #nsn_agr.drop(labels='PERIOD_START_TIME',axis=1,inplace=True)
    nsn_agr['Vendor']='Nokia'

    nsn_agr['WBTS name']=nsn_agr['WBTS name'].astype(str)
    nsn_agr['lookup']=nsn_agr['WBTS name'].apply(lambda x: x[1:])
    #tracker=pd.read_excel(r'\\file-server\AZERCONNECT_LLC_OLD\Corporate Folder\CTO\Technology trackers\RNP\Azerconnect_RNP_tracker.xlsx',skiprows=[0])
    nsn_agr=pd.merge(nsn_agr,tracker[['SITE_ID','Economical Region']],left_on='lookup',right_on='SITE_ID',how='left')
    nsn_agr.rename(columns={'WCEL name':'Cell_name','RNC name':'RNC_name','WBTS name':'Site_name',
                           'Economical Region':'Region'},inplace=True)
    print(time.time()-c1,'end of merging')
    nsn_agr=nsn_agr[['Date','Vendor','RNC_name','Site_name','Cell_name','Region','voice_sr_num1','voice_sr_den1',
                       'voice_sr_num2','voice_sr_den2','voice_dr_num','voice_dr_den','hsdpa_sr_num','hsdpa_sr_den','hsupa_sr_num','hsupa_sr_den',
                       'hsdpa_dr_num','hsdpa_dr_den','hsupa_dr_num','hsupa_dr_den','cell_avail_num','cell_avail_den',
                       'cell_avail_blck_den','cell_avail_blck_num','cs_traf','ps_traf','hsdpa_thrp_num','hsdpa_thrp_den','r99_sr_num','r99_sr_den',
                       'r99_dr_num','r99_dr_den','cs_inter_freq_ho_num','cs_inter_freq_ho_den',
                       'ps_inter_freq_ho_num','ps_inter_freq_ho_den','cs_sho_ho_num','cs_sho_ho_den','ps_sho_ho_num','ps_sho_ho_den',
                       'cs_inter_rat_ho_num','cs_inter_rat_ho_den','ps_ac_sr_num1','ps_ac_sr_den1','ps_ac_sr_num2','ps_ac_sr_den2']]
    nsn_agr.iloc[:,6:]=nsn_agr.iloc[:,6:].astype(np.float64)
    nsn_agr.drop_duplicates(keep='first',inplace=True)
    print(time.time()-c1,'new table created')
    nsn_rnc=nsn_agr.groupby(['Date','RNC_name','Vendor','Region']).sum()
    nsn_rnc.reset_index(inplace=True)

    #print(nsn_agr['Date'].unique())
    print(time.time()-c1,'time to save')
    #file_name = datetime.datetime.strftime(nsn_rnc['Date'].iloc[0], "%B_%Y") it was working
    #file_name2 = datetime.datetime.strftime(nsn_rnc['Date'].iloc[0], "%Y-%m-%d") it was working
    #print('file name is ', file_name, ' and file name2 is ', file_name2)
    for i in nsn_rnc['Date'].unique():
        file_name = datetime.datetime.strftime(datetime.datetime.utcfromtimestamp(i.astype(datetime.datetime)/1e9), "%B_%Y")
        file_name2 = datetime.datetime.strftime(datetime.datetime.utcfromtimestamp(i.astype(datetime.datetime)/1e9), "%Y-%m-%d")

        nsn_agr.loc[nsn_agr['Date']==i].to_hdf(r'/disk2/support_files/archive/ran/' + file_name2 + '.h5', '/threeG', append=True,
                       format='table', data_columns=['Date', 'RNC_name', 'Site_name', 'Cell_name', 'Vendor', 'Region'],
                       complevel=5,
                       min_itemsize={'RNC_name': 10, 'Site_name': 20, 'Cell_name': 20, 'Vendor': 10, 'Region': 15})
        nsn_rnc.loc[nsn_rnc['Date']==i].to_hdf(r'/disk2/support_files/archive/ran/' + file_name2 + '.h5', '/threeG/bsc', append=True,
                       format='table', data_columns=['Date', 'RNC_name', 'Vendor', 'Region'], complevel=5,
                       min_itemsize={'RNC_name': 10, 'Vendor': 10, 'Region': 15})
    #nsn_agr.to_hdf(r'/disk2/support_files/archive/'+file_name+'.h5','/threeG',append=True,
    #           format='table',data_columns=['Date','RNC_name','Site_name','Cell_name','Vendor','Region'],complevel=5,
    #                min_itemsize={'RNC_name':10,'Site_name':20,'Cell_name':20,'Vendor':10,'Region':15})
    #nsn_rnc.to_hdf(r'/disk2/support_files/archive/'+file_name+'.h5','/threeG/bsc',append=True,
    #           format='table',data_columns=['Date','RNC_name','Vendor','Region'],complevel=5,
    #                min_itemsize={'RNC_name':10,'Vendor':10,'Region':15})
    nsn_rnc.to_hdf(r'/disk2/support_files/archive/combined_bsc.h5', '/threeG',append=True,
                   format='table', data_columns=['Date', 'RNC_name', 'Vendor', 'Region'], complevel=5,
                   min_itemsize={'RNC_name': 10, 'Vendor': 10, 'Region': 15})

    
    print(time.time()-c1,'end of all')
