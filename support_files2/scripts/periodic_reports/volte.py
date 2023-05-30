import pandas as pd
import glob
import os,datetime,zipfile,sys
import subprocess as sp
os.chdir('/disk2/support_files/archive/core')
v,u,c=[],[],[]
yesterday = datetime.date.today() - datetime.timedelta(3)
files = pd.date_range(start=yesterday,periods=3, freq='24H').strftime('core_new_%Y-%m-%d.h5').tolist()
folder = pd.date_range(start=yesterday,periods=3, freq='24H').strftime('%Y/%B/%-d.%m.%Y').tolist()
folder2 = pd.date_range(start=yesterday,periods=3, freq='24H').strftime('%Y/%B/%-d.%m.%Y').tolist()
files2 = pd.date_range(start=yesterday,periods=3, freq='24H').strftime('%Y_%m_%d-21_14_00').tolist()
files4 = pd.date_range(start=yesterday,periods=3, freq='24H').strftime('_60_%Y%m%d2000').tolist()
main ='/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/RAN QA/Daily/Raw_counters/Nokia/common/counters/'
main2 ='/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/RAN QA/Daily/Raw_counters/Core/'

for num,i in enumerate(files):
     try:
          to_filter=i[9:-3]+' 20:00'
          try:
               df_sc=pd.read_csv(glob.glob(os.path.join(main2,folder2[num])+'/*'+files4[num]+'*qar_scscf_sess*.csv')[0],skiprows=[1])
               df_srvcc=pd.read_csv(glob.glob(os.path.join(main2,folder2[num])+'/*'+files4[num]+'*qar_2g3g_esrvcc*.csv')[0],skiprows=[1])
               df_ims=pd.read_csv(glob.glob(os.path.join(main2,folder2[num])+'/*'+files4[num]+'*qar_volte_ims_USNs*.csv')[0],skiprows=[1])
          except:
               print('no file in server')
               print(os.path.join(main,folder[num])+'/*'+files2[num]+'*qar_PAG-LU-Subs_per_LAI*.csv')
               try:
                    script='sshpass -p "qarftp" scp qarftp@10.240.104.155:Huawei/*'+files2[num]+'*qar_scscf_sess*.csv /home/ismayil/flask_dash/data/core'
                    sp.run(script,shell=True)
                    script='sshpass -p "qarftp" scp qarftp@10.240.104.155:Huawei/*'+files2[num]+'*qar_2g3g_esrvcc*.csv /home/ismayil/flask_dash/data/core'
                    sp.run(script,shell=True)
                    script='sshpass -p "qarftp" scp qarftp@10.240.104.155:Huawei/*'+files2[num]+'*qar_volte_ims_USNs*.csv /home/ismayil/flask_dash/data/core'
                    sp.run(script,shell=True)
                    df_sc=pd.read_csv(glob.glob('/home/ismayil/flask_dash/data/core/*'+files4[num]+'*qar_scscf_sess*.csv')[0],skiprows=[1])
                    df_srvcc=pd.read_csv(glob.glob('/home/ismayil/flask_dash/data/core/*'+files4[num]+'*qar_2g3g_esrvcc*.csv')[0],skiprows=[1])
                    df_ims=pd.read_csv(glob.glob('/home/ismayil/flask_dash/data/core/*'+files4[num]+'*qar_volte_ims_USNs*.csv')[0],skiprows=[1])
                    sp.run('rm /home/ismayil/flask_dash/data/core/*.csv',shell=True)
               except:
                    print('no file in qarftp')
                    from_file=pd.DataFrame()
                    1
          df_sc['Date']=pd.to_datetime(df_sc['Result Time'],format='%Y-%m-%d %H:%M')
          df_srvcc['Date']=pd.to_datetime(df_srvcc['Result Time'],format='%Y-%m-%d %H:%M')
          df_ims['Date']=pd.to_datetime(df_ims['Result Time'],format='%Y-%m-%d %H:%M')

          df=pd.read_hdf(i,'abcf_basics')
          df.drop_duplicates(inplace=True)
          df1=df.groupby('Date').sum().reset_index()
          df2=pd.DataFrame(df1.sum()).T
          df1=df1[df1['Date'].dt.hour==20]
          df1['a']=df1['Date'].dt.date
          df2['a']=df1['a'].values
          df1=df1.merge(df2,on='a',how='outer',suffixes=('','_y'))
          df1['mo_call_comp']=round(df1['MO Connected']/df1['MO Attempts']*100,2)
          df1['mt_call_comp']=round(df1['MT Connected']/df1['MT Attempts']*100,2)
          df1['mo_call_answer']=round(df1['MO Answered']/df1['MO Attempts']*100,2)
          df1['mt_call_answer']=round(df1['MT Answered']/df1['MT Attempts']*100,2)
          df1['mo_call_interrupt']=round(df1['MO Interruptions']/df1['MO Connected']*100,2)
          df1['mt_call_interrupt']=round(df1['MT Interruptions']/df1['MT Connected']*100,2)
          abcf=df1[['a','MO Attempts','MO Connected','MT Attempts','MT Connected','MO Answered','MT Answered','MO Interruptions','MT Interruptions',
               'MO Attempts_y','MO Connected_y','MT Attempts_y','MT Connected_y','MO Answered_y','MT Answered_y',
               'Connected ABCF Traf_erl','Answered ABCF Traf_erl','Connected ABCF Traf_erl_y','Answered ABCF Traf_erl_y',
               'mo_call_comp','mt_call_comp','mo_call_answer','mt_call_answer','mo_call_interrupt','mt_call_interrupt']]

          df=pd.read_hdf(i,'ats_basics')
          df.drop_duplicates(inplace=True)
          df1=df.groupby('Date').sum().reset_index()
          df2=df1[df1['Date'].dt.hour==20]
          df1=df1[df1['Date'].dt.hour==20]
          df1['a']=df1['Date'].dt.date
          df2['a']=df1['a'].values
          df1=df1.merge(df2,on='a',how='outer',suffixes=('','_y'))
          ats=df1[['a','Mean duration of connected MO session_ms','Mean duration of connected MT session_ms','BHCA per Subscriber',
               'Mean duration of MO seized sessions_sec','Mean duration of MT seized sessions_sec','Peak num of ATS simultaneous sessions','ATS VoLTE registered subs_y']]

          df=pd.read_hdf(i,'/scsf')
          df.drop_duplicates(inplace=True)
          df1=df.groupby('Date').sum().reset_index()
          df1=df1[df1['Date'].dt.hour==20]
          df1['init_regis']=round(df1['Initial register success']/df1['Initial register attempt']*100,2)
          df1['re_regis']=round(df1['Re-registration success']/df1['Re-registration attempt']*100,2)
          df1['de_regis']=round(df1['Deregistration success']/df1['Deregistration attempt']*100,2)
          df1[['b','c','d','e','f','g','h','i','j','k','l','m']]=df_sc.loc[df_sc['Date'].dt.hour==20,['Date','335664440','335664436','335664432','335664441','335664437',
                                                                      '335664433','335664438','335664434','335664430','335664439','335664435','335664431']].\
                                                                           groupby('Date').sum().values
          df1['notify_sr']=round(df1['Notify Messages for Reg num']/df1['Notify Messages for Reg den']*100,2)
          if (df1['Publish message'].sum()<1):
               df1['publish_sr']=None
          else:
               df1['publish_sr']=round(df1['Publish success']/df1['Publish message']*100,2)

          if (df1['Refer message'].sum()<1):
               df1['refer_sr']=None
          else:
               df1['refer_sr']=round(df1['Refer success']/df1['Refer message']*100,2)

          df1['subscribe_regis']=round(df1['Subscribe Requests for Registration num']/df1['Subscribe Requests for Registration den']*100,2)
          df1['third_party_de_regis']=round(df1['Third_party deregistration success']/df1['Third_party deregistration attempt']*100,2)
          df1['third_party_regis']=round(df1['Third_party registration success']/(df1['Third_party registration success']+df1['Third_party registration unsuccess'])*100,2)
          df1['auth_sr']=round(df1['Authentication success']/df1['Authentication attempt']*100,2)
          df1['mo_dcr']=round(df1['MO Call Drop']/df1['MO Answered']*100,2)
          df1['mt_dcr']=round(df1['MT Call Drop']/df1['MT Answered']*100,2)
          df1['a']=df1['Date'].dt.date
          scsf=df1[['a','Initial register attempt','Initial register success','Re-registration attempt','Re-registration success','Deregistration attempt','Deregistration success',
               'init_regis','re_regis','de_regis','b','c','d','e','f','g','Publish message','Publish success','Refer message','Refer success','h','i','j','k','l','m',
               'Third_party deregistration attempt','Third_party deregistration success','Third_party registration success','Third_party registration unsuccess',
               'Authentication attempt','Authentication success','MO Answered','MO Call Drop','MT Answered','MT Call Drop',
               'notify_sr','publish_sr','refer_sr','subscribe_regis','third_party_de_regis','third_party_regis','auth_sr','mo_dcr','mt_dcr']]

          df_srvcc=df_srvcc.loc[df_srvcc['Date'].dt.hour==20].groupby('Date').sum().reset_index()
          df_srvcc['a']=df_srvcc['Date'].dt.date
          if df_srvcc[['84164965','84164967']].sum(axis=1).values==0:
               df_srvcc.loc[:,'2g_srvcc']=None
          else: 
               df_srvcc.loc[:,'2g_srvcc']=round(df_srvcc[['84175689','84176435','84176698']].sum(axis=1)/df_srvcc[['84164965','84164967']].sum(axis=1)*100,2)

          if df_srvcc[['84164975','84164977']].sum(axis=1).values==0:
               df_srvcc.loc[:,'3g_srvcc']=None
          else: 
               df_srvcc.loc[:,'3g_srvcc']=round(df_srvcc[['84175691','84176438','84176700']].sum(axis=1)/df_srvcc[['84164975','84164977']].sum(axis=1)*100,2)

          srvcc=df_srvcc[['a','84175689','84176698','84176435','84164965','84164967','84175691','84176700','84176438','84164975','84164977','2g_srvcc','3g_srvcc']]
          
          df=pd.read_hdf(i,'/ims_usn')
          df.drop_duplicates(inplace=True)
          df1=df[df['Site']!='vUSN'].groupby('Date').sum().reset_index()
          df1=df1[df1['Date'].dt.hour==20]
          df1[['b','c','d','e','f','g','h','i','j','k','l','m']]=df_ims.loc[(df_ims['Date'].dt.hour==20) & (~df_ims['Object Name'].str.contains('vUSN')),\
                                                                 ['Date','117499820','117499821','117499822','117499823',
                                                                 '117499812','117499813','117499816','117499817','117499814','117499815','117499818','117499819']].\
                                                                      groupby('Date').sum().values
          df1['bearer_act_sr']=round(df1['Voice bearer success over S11']/df1['Voice bearer request over S11']*100,2)
          df1['voice_bearer_act_sr']=round(df1['Voice bearer success']/df1['Voice bearer request']*100,2)
          df1['pdn_con_sr']=round(df1['PDN Connect success']/df1['PDN Connect request']*100,2)
          df1['video_dr']=round((df1['Video bearer deactivation']-df1[['Video bearer deactivation trigger by enodeb','Video bearer deactivation trigger by ue',
                         'Video bearer deactivation trigger by sgw_pgw','Video bearer deactivation trigger erab update failure']].sum(axis=1))/df1['Video bearer success']*100,2)
          df1['voice_dr']=round((df1['Voice bearer deactivation']-df1[['Voice bearer deactivation trigger by enodeb','Voice bearer deactivation trigger by ue',
               'Voice bearer deactivation trigger by sgw_pgw','Voice bearer deactivation trigger erab update failure']].sum(axis=1))/df1['Voice bearer success']*100,2)
          if df1['Inter S1 Handover den'].sum()==0:df1['inter_mme_voice_hosr']=None
          else:df1['inter_mme_voice_hosr']=round(df1['Inter S1 Handover num']/df1['Inter S1 Handover den']*100,2)


          if df1['Intra S1 Handover num'].sum()==0:df1['intra_mme_voice_hosr']=None
          else:df1['intra_mme_voice_hosr']=round((df1['Intra S1 Handover num']+df1['X2 Handover num'])/(df1['Intra S1 Handover den']+df1['X2 Handover den'])*100,2)

          mo_mt=pd.read_hdf(i,'mo_mt_ccr')
          mo_mt.drop_duplicates(inplace=True)
          mo_mt=mo_mt.loc[mo_mt['Date'].dt.hour==20].groupby('Direction').sum()
          cem=pd.read_hdf(i,'cem_cst')
          cem.drop_duplicates(inplace=True)

          df1['volte_mo_cst']=round(ats['Mean duration of connected MO session_ms']/1000,2).values
          df1['volte_mt_cst']=round(ats['Mean duration of connected MT session_ms']/1000,2).values
          df1['volte_cst']=round(ats[['Mean duration of connected MO session_ms','Mean duration of connected MT session_ms']].mean(axis=1)/1000,2).values
          df1['2G_mo_cst']=mo_mt.loc['MO','TwoG Call Setup time']
          df1['2G_mo_call_comp']=mo_mt.loc['MO','TwoG Call Completion']
          df1['3G_mo_cst']=mo_mt.loc['MO','ThreeG Call Setup time']
          df1['3G_mo_call_comp']=mo_mt.loc['MO','ThreeG Call Completion']
          df1['cs_cst']=df1[['2G_mo_cst','3G_mo_cst']].sum(axis=1)/df1[['2G_mo_call_comp','3G_mo_call_comp']].sum(axis=1)
          #df1['n']=0
          df1['n']=round(cem.loc[cem['Date'].dt.hour==20,'V2V_MO_Connection_Delay']/1000,2).values
          df1['2G_3G_traf']=pd.read_hdf(i,'ab_vtraff').drop_duplicates().sum()['Inc_out and HO seizure traffic_erl']
          df1['volte_share']=round(abcf['Connected ABCF Traf_erl_y'].values/(abcf['Connected ABCF Traf_erl_y'].values+df1['2G_3G_traf'])*100,2)
          df1['a']=df1['Date'].dt.date
          ims=df1[['a','Voice bearer request over S11','Voice bearer success over S11','Voice bearer request','Voice bearer success','Maximum number of PDN connections',
               'PDN Connect request','PDN Connect success','Video bearer deactivation','Video bearer deactivation trigger by ue',
               'Video bearer deactivation trigger by enodeb','Video bearer deactivation trigger by sgw_pgw','Video bearer deactivation trigger erab update failure',
               'Video bearer success','Voice bearer deactivation','Voice bearer deactivation trigger by ue','Voice bearer deactivation trigger by enodeb',
               'Voice bearer deactivation trigger by sgw_pgw','Voice bearer deactivation trigger erab update failure','b','c','d','e','f','g','h','i','j','k','l','m',
               'bearer_act_sr','voice_bearer_act_sr','pdn_con_sr','video_dr','voice_dr','inter_mme_voice_hosr','intra_mme_voice_hosr','volte_mo_cst','volte_mt_cst','volte_cst',
               '2G_mo_cst','2G_mo_call_comp','3G_mo_cst','3G_mo_call_comp','cs_cst','n','2G_3G_traf','volte_share']]
          
          ################### VUSN part #######################
          df1=df[df['Site']=='vUSN'].groupby('Date').sum().reset_index()
          df1=df1[df1['Date'].dt.hour==20]
          df1[['b','c','d','e','f','g','h','i','j','k','l','m']]=df_ims.loc[(df_ims['Date'].dt.hour==20) & (df_ims['Object Name'].str.contains('vUSN')),\
                                                                 ['Date','117499820','117499821','117499822','117499823',
                                                                 '117499812','117499813','117499816','117499817','117499814','117499815','117499818','117499819']].\
                                                                      groupby('Date').sum().values
          df1['bearer_act_sr']=round(df1['Voice bearer success over S11']/df1['Voice bearer request over S11']*100,2)
          df1['voice_bearer_act_sr']=round(df1['Voice bearer success']/df1['Voice bearer request']*100,2)
          df1['pdn_con_sr']=round(df1['PDN Connect success']/df1['PDN Connect request']*100,2)
          df1['video_dr']=round((df1['Video bearer deactivation']-df1[['Video bearer deactivation trigger by enodeb','Video bearer deactivation trigger by ue',
                         'Video bearer deactivation trigger by sgw_pgw','Video bearer deactivation trigger erab update failure']].sum(axis=1))/df1['Video bearer success']*100,2)
          df1['voice_dr']=round((df1['Voice bearer deactivation']-df1[['Voice bearer deactivation trigger by enodeb','Voice bearer deactivation trigger by ue',
               'Voice bearer deactivation trigger by sgw_pgw','Voice bearer deactivation trigger erab update failure']].sum(axis=1))/df1['Voice bearer success']*100,2)
          if df1['Inter S1 Handover den'].sum()==0:df1['inter_mme_voice_hosr']=None
          else:df1['inter_mme_voice_hosr']=round(df1['Inter S1 Handover num']/df1['Inter S1 Handover den']*100,2)


          if df1['Intra S1 Handover num'].sum()==0:df1['intra_mme_voice_hosr']=None
          else:df1['intra_mme_voice_hosr']=round((df1['Intra S1 Handover num']+df1['X2 Handover num'])/(df1['Intra S1 Handover den']+df1['X2 Handover den'])*100,2)
          df1['a']=df1['Date'].dt.date
          v.append(df1[['a','Voice bearer request over S11','Voice bearer success over S11','Voice bearer request','Voice bearer success','Maximum number of PDN connections',
               'PDN Connect request','PDN Connect success','Video bearer deactivation','Video bearer deactivation trigger by ue',
               'Video bearer deactivation trigger by enodeb','Video bearer deactivation trigger by sgw_pgw','Video bearer deactivation trigger erab update failure',
               'Video bearer success','Voice bearer deactivation','Voice bearer deactivation trigger by ue','Voice bearer deactivation trigger by enodeb',
               'Voice bearer deactivation trigger by sgw_pgw','Voice bearer deactivation trigger erab update failure','b','c','d','e','f','g','h','i','j','k','l','m',
               'bearer_act_sr','voice_bearer_act_sr','pdn_con_sr','video_dr','voice_dr','inter_mme_voice_hosr','intra_mme_voice_hosr']])
          ##########################################################
          
          total=abcf.merge(ats,on='a',how='outer')
          total=total.merge(scsf,on='a',how='outer')
          total=total.merge(srvcc,on='a',how='outer')
          total=total.merge(ims,on='a',how='outer')
          u.append(total)
     except Exception as e:
          exc_type, exc_obj, exc_tb = sys.exc_info()
          fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
          print(exc_type, fname, exc_tb.tb_lineno)
          print(e)
          print(df_ims)
          continue

pd.concat(u).to_csv('/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/Core QA/scheduled_reports/volte_usn_'+files2[-1][:10]+'.csv',index=False)
pd.concat(v).to_csv('/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/Core QA/scheduled_reports/volte_vusn_'+files2[-1][:10]+'.csv',index=False)
