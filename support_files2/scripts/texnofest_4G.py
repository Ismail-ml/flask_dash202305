import pandas as pd
import glob, time, os, re
#import dask.dataframe as dd
import concurrent.futures as cf
import numpy as np
import datetime

def run(path1,tracker):
    try:
        print('Huawei 4G aggregation begin...')
        c1 = time.time()
        path1='/home/ismayil/flask_dash/data/huawei'
        path2='/home/ismayil/flask_dash/data/huawei'
        hw=pd.DataFrame()
        def proccess_excel(diddd):
            try:
                #existing_files = pd.read_csv(os.path.join(path1, 'files.txt'), sep=" ", header=None)
                all_files=glob.glob(path1 + "/*crystal_1*.csv")
                for filename in all_files:
                    #if os.path.basename(filename) in existing_files: continue
                    df = pd.read_csv(filename,skiprows=[1])
                    df.rename(columns={'1526728322':'csfb_sr_num','1526728321':'csfb_sr_den','1526726659':'rrc_sr_num',
                                    '1526726658':'rrc_sr_den','1526727544':'rab_sr_num','1526727545':'rab_sr_den',
                                    '1526726991':'irat_ho_num','1526726990':'irat_ho_den'},inplace=True)
                    df['cell_avail_den']=3600
                    df['cell_avail_num']=df['cell_avail_den']-df['1526727209']-df['1526727210']
                    df['cell_avail_blck_num']=df['1526727210']
                    df['cell_avail_blck_den']=0
                    df['dcr_num']=df['1526727546']
                    df['dcr_den']=df['1526727547']+df['1526727546']
                    df['dl_thrp_num_1']=df['1526729005']
                    df['dl_thrp_den']=df['1526729015']*1024
                    df['ul_thrp_num_1']=df['1526729049']
                    df['ul_thrp_den']=df['1526729050']*1024
                    df['intra_freq_ho_num']=df['1526726997']+df['1526727003']
                    df['intra_freq_ho_den']=df['1526727002']+df['1526726996']
                    df['volte_sr_num'] = df['1526726669']
                    df['volte_sr_den'] = df['1526726668']
                    df['volte_dr_num'] = df['1526726686']
                    df['volte_dr_den'] = (df['1526726686']+df['1526726687'])
                    df['volte_dl_ps_traf'] = df['1526726803']/8/1024/1024
                    df['volte_ul_ps_traf'] = df['1526726776']/8/1024/1024
                    df['volte_cs_traf'] = df['1526735162']*0.1/60/60
                    df['volte_srvcc_e2w_num'] = df['1526728402']
                    df['volte_srvcc_e2w_den'] = df['1526728401']
                    df['Voice_VQI_DL_Accept_Times'] = df['1526728418']
                    df['Voice_VQI_DL_Bad_Times'] = df['1526728420']
                    df['Voice_VQI_DL_Excellent_Times'] = df['1526728416']
                    df['Voice_VQI_DL_Good_Times'] = df['1526728417']
                    df['Voice_VQI_DL_Poor_Times'] = df['1526728419']
                    df['Voice_VQI_DL_TotalValue'] = df['1526741682']
                    df['Voice_VQI_UL_Accept_Times'] = df['1526728413']
                    df['Voice_VQI_UL_Bad_Times'] = df['1526728415']
                    df['Voice_VQI_UL_Excellent_Times'] = df['1526728411']
                    df['Voice_VQI_UL_Good_Times'] = df['1526728412']
                    df['Voice_VQI_UL_Poor_Times'] = df['1526728414']
                    df['Voice_VQI_UL_TotalValue'] = df['1526741681']
                    df['Voice_DL_Silent_Num'] = df['1526732893']
                    df['Voice_UL_Silent_Num'] = df['1526732892']


                    df=df[['Object Name','Result Time','cell_avail_num',
                    'cell_avail_den','cell_avail_blck_num','cell_avail_blck_den','csfb_sr_num','csfb_sr_den','rrc_sr_num',
                    'rrc_sr_den','rab_sr_num','rab_sr_den',
                    'dcr_num','dcr_den','dl_thrp_num_1','dl_thrp_den','ul_thrp_num_1','ul_thrp_den','intra_freq_ho_num',
                            'intra_freq_ho_den','irat_ho_num','irat_ho_den',
                        'volte_sr_num','volte_sr_den','volte_dr_num','volte_dr_den','volte_dl_ps_traf','volte_ul_ps_traf','volte_cs_traf',
                        'volte_srvcc_e2w_num','volte_srvcc_e2w_den','Voice_VQI_DL_Accept_Times','Voice_VQI_DL_Bad_Times','Voice_VQI_DL_Excellent_Times',
                        'Voice_VQI_DL_Good_Times','Voice_VQI_DL_Poor_Times','Voice_VQI_DL_TotalValue','Voice_VQI_UL_Accept_Times',
                        'Voice_VQI_UL_Bad_Times','Voice_VQI_UL_Excellent_Times','Voice_VQI_UL_Good_Times','Voice_VQI_UL_Poor_Times',
                        'Voice_VQI_UL_TotalValue','Voice_DL_Silent_Num','Voice_UL_Silent_Num']]
                    df['Date']=pd.to_datetime(df['Result Time'], format='%Y-%m-%d %H:%M')
                    df['Cell_name']=df['Object Name'].apply(lambda x: x[x.find('=',x.find('Cell Name'))+1:x.find(',',x.find('Cell Name'))])
                    df['Site_name']=df['Cell_name'].apply(lambda x: x[:9])
                    li.append(df)
            except Exception as u:
                print(u)
                1        
        h=[]
        [h.append(r) for r, d, folder in os.walk(path1)]
        li = [] 

        #with cf.ThreadPoolExecutor() as executor:
        #    executor.map(proccess_excel,h)

        proccess_excel(path1)

        hw = pd.concat(li, axis=0, ignore_index=True,sort=False)
        print(time.time()-c1,'part1 finish')


        hw2=pd.DataFrame()
        def proccess_excel2(diddd):
            try:
                #existing_files = pd.read_csv(os.path.join(path2, 'files.txt'), sep=" ", header=None)
                all_files=glob.glob(path1 + "/*crystal_2*.csv")
                for filename in all_files:
                    #if os.path.basename(filename) in existing_files: continue
                    df = pd.read_csv(filename,skiprows=[1])
                    df['dl_ps_traf']=df['1526728261']/(8*1024*1024)
                    df['ul_ps_traf']=df['1526728259']/(8*1024*1024)
                    df['rtwp']=df['1526728298']
                    df['dl_prb_num'] = df['1526726740']
                    df['dl_prb_den'] = df['1526728433']
                    df['ul_prb_num'] = df['1526726737']
                    df['ul_prb_den'] = df['1526728434']
                    df.rename(columns={'1526728976':'Max_active_user','1526730601':'Max_active_user_dl_qci1',
                    '1526730611':'Max_active_user_ul_qci1', '1526730878':'RRC_user_license',
                    '1526726717':'Rab_fail_NoReply', '1526728276':'Rab_fail_MME','1526728277':'Rab_fail_TNL',
                    '1526728278':'Rab_fail_RNL', '1526728279':'Rab_fail_NoRadioRes','1526728280':'Rab_fail_SecurModeFail',
                    '1526727379':'Max_connected_user'},inplace=True)
                    df=df[['Object Name','Result Time','dl_ps_traf',
                    'ul_ps_traf','1526728261','1526728259','rtwp','dl_prb_num','dl_prb_den','ul_prb_num','ul_prb_den',
                    'Max_active_user','Max_active_user_dl_qci1','Max_active_user_ul_qci1','RRC_user_license',
                    'Rab_fail_NoReply','Rab_fail_MME','Rab_fail_TNL','Rab_fail_RNL','Rab_fail_NoRadioRes','Rab_fail_SecurModeFail','Max_connected_user']]
                    df['Date']=pd.to_datetime(df['Result Time'], format='%Y-%m-%d %H:%M')
                    df['Cell_name']=df['Object Name'].apply(lambda x: x[x.find('=',x.find('Cell Name'))+1:x.find(',',x.find('Cell Name'))])
                    li.append(df)
            except Exception as u:
                print(u)
                1
        h=[]
        [h.append(r) for r, d, folder in os.walk(path2)]
        li = [] 

        #with cf.ThreadPoolExecutor() as executor:
        #    executor.map(proccess_excel2,h)
        proccess_excel2(path1)

        hw2 = pd.concat(li, axis=0, ignore_index=True,sort=False)
        print(time.time()-c1,'part2 finish')


        hw_agr=pd.merge(hw,hw2,on=['Cell_name','Date'],how='left')
        print(time.time()-c1)

        hw_agr['ul_thrp_num']=(hw_agr['1526728259']-hw_agr['ul_thrp_num_1'])*1000
        hw_agr['dl_thrp_num']=(hw_agr['1526728261']-hw_agr['dl_thrp_num_1'])*1000



        #hw_agr_finish.drop(labels=['Object Name','Result Time'],axis=1,inplace=True)

        print(time.time()-c1,'whole finish')

        hw_agr['Vendor']='Huawei'
        hw_agr['lookup']=hw_agr['Site_name'].apply(lambda x: x[1:8])
        #tracker=pd.read_excel(r'\\file-server\AZERCONNECT_LLC_OLD\Corporate Folder\CTO\Technology trackers\RNP\Azerconnect_RNP_tracker.xlsx',skiprows=[0])
        hw_agr=pd.merge(hw_agr,tracker[['SITE_ID','Economical Region']],left_on='lookup',right_on='SITE_ID',how='left')
        hw_agr.rename(columns={'Economical Region':'Region'},inplace=True)
        print(time.time()-c1,'end of merging')
        hw_agr_finish=hw_agr[['Date','Vendor','Site_name','Cell_name','Region','cell_avail_num','cell_avail_den',
                    'cell_avail_blck_num','cell_avail_blck_den','csfb_sr_num','csfb_sr_den','rrc_sr_num','rrc_sr_den','rab_sr_num',
                    'rab_sr_den','dcr_num','dcr_den','dl_ps_traf','ul_ps_traf','dl_thrp_num','dl_thrp_den',
                    'ul_thrp_num','ul_thrp_den','intra_freq_ho_num','intra_freq_ho_den','irat_ho_num','irat_ho_den',
                    'volte_sr_num','volte_sr_den','volte_dr_num','volte_dr_den','volte_dl_ps_traf','volte_ul_ps_traf','volte_cs_traf','rtwp',
                    'volte_srvcc_e2w_num','volte_srvcc_e2w_den','Voice_VQI_DL_Accept_Times','Voice_VQI_DL_Bad_Times','Voice_VQI_DL_Excellent_Times',
                    'Voice_VQI_DL_Good_Times','Voice_VQI_DL_Poor_Times','Voice_VQI_DL_TotalValue','Voice_VQI_UL_Accept_Times',
                    'Voice_VQI_UL_Bad_Times','Voice_VQI_UL_Excellent_Times','Voice_VQI_UL_Good_Times','Voice_VQI_UL_Poor_Times',
                    'Voice_VQI_UL_TotalValue','Voice_DL_Silent_Num','Voice_UL_Silent_Num',
                    'dl_prb_num','dl_prb_den','ul_prb_num','ul_prb_den',
                'Max_active_user','Max_active_user_dl_qci1','Max_active_user_ul_qci1','RRC_user_license',
                'Rab_fail_NoReply','Rab_fail_MME','Rab_fail_TNL','Rab_fail_RNL','Rab_fail_NoRadioRes','Rab_fail_SecurModeFail','Max_connected_user']]
        keys = ['cell_avail_num', 'cell_avail_den',
                'cell_avail_blck_num', 'cell_avail_blck_den', 'csfb_sr_num', 'csfb_sr_den', 'rrc_sr_num', 'rrc_sr_den',
                'rab_sr_num',
                'rab_sr_den', 'dcr_num', 'dcr_den', 'dl_ps_traf', 'ul_ps_traf', 'dl_thrp_num', 'dl_thrp_den',
                'ul_thrp_num', 'ul_thrp_den', 'intra_freq_ho_num', 'intra_freq_ho_den', 'irat_ho_num', 'irat_ho_den',
                'volte_sr_num', 'volte_sr_den', 'volte_dr_num', 'volte_dr_den', 'volte_dl_ps_traf', 'volte_ul_ps_traf',
                'volte_cs_traf','volte_srvcc_e2w_num','volte_srvcc_e2w_den','Voice_VQI_DL_Accept_Times','Voice_VQI_DL_Bad_Times','Voice_VQI_DL_Excellent_Times',
                    'Voice_VQI_DL_Good_Times','Voice_VQI_DL_Poor_Times','Voice_VQI_DL_TotalValue','Voice_VQI_UL_Accept_Times',
                    'Voice_VQI_UL_Bad_Times','Voice_VQI_UL_Excellent_Times','Voice_VQI_UL_Good_Times','Voice_VQI_UL_Poor_Times',
                    'Voice_VQI_UL_TotalValue','Voice_DL_Silent_Num','Voice_UL_Silent_Num',
                    'dl_prb_num','dl_prb_den','ul_prb_num','ul_prb_den',
                'Max_active_user','Max_active_user_dl_qci1','Max_active_user_ul_qci1','RRC_user_license',
                'Rab_fail_NoReply','Rab_fail_MME','Rab_fail_TNL','Rab_fail_RNL','Rab_fail_NoRadioRes','Rab_fail_SecurModeFail','Max_connected_user']
        agg_dict = dict.fromkeys(keys, 'sum')
        agg_dict['rtwp'] = 'mean'
        hw_agr_finish.iloc[:,5:]=hw_agr_finish.iloc[:,5:].astype(np.float64)
        hw_agr_finish.drop_duplicates(keep='first',inplace=True)
        print(time.time()-c1,'new table created')
        hw_rnc=hw_agr_finish.groupby(['Date','Vendor','Region']).agg(agg_dict)
        hw_rnc.reset_index(inplace=True)
        print(time.time()-c1,'time to save')

        if os.path.exists('/disk2/support_files/archive/fourG_bsc.csv'):
            existing=pd.read_csv('/disk2/support_files/archive/fourG_bsc.csv')

        if os.path.exists('/disk2/support_files/archive/fourG.csv'):
            hw_agr=hw_agr[~hw_agr['Date'].isin(existing['Date'].unique())]
            hw_agr_finish.to_csv('/disk2/support_files/archive/fourG.csv',header=False,index=False,mode='a')
        else:
            hw_agr_finish.to_csv('/disk2/support_files/archive/fourG.csv',index=False,mode='a')
        
        if os.path.exists('/disk2/support_files/archive/fourG_bsc.csv'):
            hw_rnc=hw_rnc[~hw_rnc['Date'].isin(existing['Date'].unique())]
            hw_rnc.to_csv('/disk2/support_files/archive/fourG_bsc.csv',header=False,index=False,mode='a')
        else:
            hw_rnc.to_csv('/disk2/support_files/archive/fourG_bsc.csv',index=False,mode='a')

        print(time.time()-c1)
    except Exception as e:
        print(e)

