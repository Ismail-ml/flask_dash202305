import pandas as pd
import glob
import os,datetime,zipfile
import subprocess as sp
os.chdir('/disk2/support_files/archive/core')
v,u,c=[],[],[]
yesterday = datetime.date.today() - datetime.timedelta(3)
files = pd.date_range(start=yesterday,periods=3, freq='24H').strftime('core_new_%Y-%m-%d.h5').tolist()
folder = pd.date_range(start=yesterday,periods=3, freq='24H').strftime('%Y/%B/%-d.%m.%Y').tolist()
folder2 = pd.date_range(start=yesterday,periods=3, freq='24H').strftime('%Y/%B/%-d.%m.%Y').tolist()
files2 = pd.date_range(start=yesterday,periods=3, freq='24H').strftime('%Y_%m_%d-23_14_00').tolist()
files4 = pd.date_range(start=yesterday,periods=3, freq='24H').strftime('_60_%Y%m%d2200').tolist()
main ='/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/RAN QA/Daily/Raw_counters/Nokia/common/counters/'
main2 ='/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/RAN QA/Daily/Raw_counters/Core/'
for num,i in enumerate(files):
    try:
        try:
            df=pd.read_csv(glob.glob(os.path.join(main2,folder2[num])+'/*'+files4[num]+'*qar_USNs_KPIs*.csv')[0],skiprows=[1])
            df_l=pd.read_csv(glob.glob(os.path.join(main2,folder2[num])+'/*'+files4[num]+'*qar_USNs_LTE_KPIs*.csv')[0],skiprows=[1])
            df=df.merge(df_l.drop(columns=['Granularity Period','Reliability']),on=['Result Time','Object Name'])
            df_user=pd.read_csv(glob.glob(os.path.join(main2,folder2[num])+'/*'+files4[num]+'*qar_USN_attach_users*.csv')[0],skiprows=[1])
            df=df.merge(df_user.drop(columns=['Granularity Period','Reliability']),on=['Result Time','Object Name'])
        except:
            print('no file in server')
            print(os.path.join(main,folder[num])+'/*'+files2[num]+'*qar_PAG-LU-Subs_per_LAI*.csv')
            try:
                script='sshpass -p "qarftp" scp qarftp@10.240.104.155:Huawei/*'+files2[num]+'*qar_USNs_KPIs*.csv /home/ismayil/flask_dash/data/core'
                sp.run(script,shell=True)
                script='sshpass -p "qarftp" scp qarftp@10.240.104.155:Huawei/*'+files2[num]+'*qar_USNs_LTE_KPIs*.csv /home/ismayil/flask_dash/data/core'
                sp.run(script,shell=True)
                script='sshpass -p "qarftp" scp qarftp@10.240.104.155:Huawei/*'+files2[num]+'*qar_USN_attach_users*.csv /home/ismayil/flask_dash/data/core'
                sp.run(script,shell=True)
                df=pd.read_csv(glob.glob('/home/ismayil/flask_dash/data/core/*'+files4[num]+'*qar_USNs_KPIs*.csv')[0],skiprows=[1])
                df_l=pd.read_csv(glob.glob('/home/ismayil/flask_dash/data/core/*'+files4[num]+'*qar_USNs_LTE_KPIs*.csv')[0],skiprows=[1])
                df=df.merge(df_l.drop(columns=['Granularity Period','Reliability']),on=['Result Time','Object Name'])
                df_user=pd.read_csv(glob.glob('/home/ismayil/flask_dash/data/core/*'+files4[num]+'*qar_USN_attach_users*.csv')[0],skiprows=[1])
                df=df.merge(df_user.drop(columns=['Granularity Period','Reliability']),on=['Result Time','Object Name'])
                sp.run('rm /home/ismayil/flask_dash/data/core/*.csv',shell=True)
            except:
                print('no file in qarftp')
                from_file=pd.DataFrame()
                1
        df2=df
        for site in ['vUSN','USN9810']:
            df=df2[df2['Object Name'].str.contains(site)].groupby('Result Time').sum().reset_index()
            print(df.columns)
            df['s1_sr']=round(df['117495953']/df['117495952']*100,2)
            df['s1_pag']=round(df['117491813']/df['117491812']*100,2)
            df['intra_mme']=round(df[['117491328','117491313','117491358','117491343']].sum(axis=1)/df[['117491327','117491312','117491357','117491342']].sum(axis=1)*100,2)
            df['inter_mme']=round(df[['117491428','117491413']].sum(axis=1)/df[['117491427','117491412']].sum(axis=1)*100,2)
            df['x2']=round(df[['117491515','117491513']].sum(axis=1)/df[['117491514','117491512']].sum(axis=1)*100,2)
            df['s1_intra']=round(df[['117491519','117491517']].sum(axis=1)/df[['117491518','117491516']].sum(axis=1)*100,2)
            df['s1_inter']=round(df[['117491615','117491613']].sum(axis=1)/df[['117491614','117491612']].sum(axis=1)*100,2)
            df['auth']=round(df['117491713']/df['117491712']*100,2)
            df['comb_attach']=round((df['117491129']+df['117491128'])/df['117491127']*100,2)
            df['Date']=pd.to_datetime(df['Result Time']).dt.date
            vusn=df[['Date','117495952','117495953','117491812','117491813','117491327','117491312','117491357','117491342','117491328','117491313','117491358','117491343',
                '117491427','117491412','117491428','117491413','117491514','117491512','117491515','117491513','117491518','117491516','117491519','117491517',
                '117491614','117491612','117491615','117491613','117491712','117491713','117491127','117491128','117491129','117491130',
                's1_sr','s1_pag','intra_mme','inter_mme','x2','s1_intra','s1_inter','auth','comb_attach']]
            if site=='vUSN': v.append(vusn)
            else: u.append(vusn)
    except Exception as e:
        print('error from usn ',e)
        1
for num,i in enumerate(files):
    try:
        to_filter=i[9:-3]+' 22:00'
        try:
            try:
                files3 = zipfile.ZipFile(glob.glob(os.path.join(main,folder[num])+'/*Core_KPIs*'+files2[num]+'*.zip')[0], 'r')
                df=pd.read_csv(files3.open(files3.namelist()[2]), sep=';')
            except:
                print('no file in server')
                print(os.path.join(main,folder[num])+'/*Core_KPIs*'+files2[num]+'.zip')
                try:
                    script='sshpass -p "qarftp" scp qarftp@10.240.104.155:Nokia/*Core_KPIs*'+files2[num]+'*.zip /home/ismayil/flask_dash/data/nokia/pool'
                    sp.run(script,shell=True)
                    files3 = zipfile.ZipFile(glob.glob('/home/ismayil/flask_dash/data/nokia/pool/*Core_KPIs*'+files2[num]+'*.zip')[0], 'r')
                    df=pd.read_csv(files3.open(files3.namelist()[2]), sep=';')
                    sp.run('rm /home/ismayil/flask_dash/data/core/*.csv',shell=True)
                except:
                    print('no file in qarftp')
                    from_file=pd.DataFrame()
                    1
            df['Date']=pd.to_datetime(df['PERIOD_START_TIME'],format='%m.%d.%Y %H:%M:%S')
            df=df.groupby('Date').sum().reset_index()
            df['s1_sr']=round(df['ACT_DFLT_EPS_BRR_SUCC (M103C194)']/df['ACT_DFLT_EPS_BRR_ATT (M103C193)']*100,2)
            df['s1_pag']=round((df['ATT_PAGE (M122C019)']-df[['PAGE_FAIL_NON_SYS_REL (M122C024)','PAGE_FAIL_SYS_REL (M122C025)','PAGE_FAIL_TO (M122C026)']].sum(axis=1))/\
                            df['ATT_PAGE (M122C019)']*100,2)
            df['intra_mme']=round(df['SUCC_TAU (M118C278)']/df['ATT_TAU (M118C255)']*100,2)
            df['inter_mme']=round(df['TAU_INTER_MME_SUCC (M118C248)']/df['TAU_INTER_MME_ATT (M118C246)']*100,2)
            df['x2']=round(df[['HO_SUCC_PATH_SW_NEW_SGW (M114C096)','HO_SUCC_PATH_SW_SAME_SGW (M114C104)']].sum(axis=1)/\
                        df[['ATT_HO_PATH_SW_NEW_SGW (M114C092)','ATT_HO_PATH_SW_SAME_SGW (M114C101)']].sum(axis=1)*100,2)
            df['s1_intra']=round(df[['S1_HO_SUCC_SAME_MME_NEW_SGW (M114C073)','HO_SUCC_HO_REQ_NO_RELOC (M114C100)']].sum(axis=1)/\
                                df[['ATT_S1_HO_SAME_MME_NEW_SGW (M114C070)','ATT_HO_REQ_NO_RELOC (M114C097)']].sum(axis=1)*100,2)
            df['auth']=round(df['SUCC_AUTH_REQ_UE (M101C021)']/df['ATT_AUTH_REQ_UE (M101C013)']*100,2)
            df['comb_attach']=round(df['SUCC_NON_EPS_ATCH (M118C321)']/df['ATT_NON_EPS_ATCH (M118C299)']*100,2)
            df['Date']=df['Date'].dt.date
            cmm=df[['Date','s1_sr','ACT_DFLT_EPS_BRR_ATT (M103C193)','ACT_DFLT_EPS_BRR_SUCC (M103C194)','s1_pag','ATT_PAGE (M122C019)','PAGE_FAIL_NON_SYS_REL (M122C024)',
                'PAGE_FAIL_SYS_REL (M122C025)','PAGE_FAIL_TO (M122C026)','intra_mme','ATT_TAU (M118C255)','SUCC_TAU (M118C278)','inter_mme','TAU_INTER_MME_ATT (M118C246)',
                'TAU_INTER_MME_SUCC (M118C248)',
                'x2','ATT_HO_PATH_SW_NEW_SGW (M114C092)','ATT_HO_PATH_SW_SAME_SGW (M114C101)','HO_SUCC_PATH_SW_NEW_SGW (M114C096)','HO_SUCC_PATH_SW_SAME_SGW (M114C104)',
                's1_intra','ATT_S1_HO_SAME_MME_NEW_SGW (M114C070)','ATT_HO_REQ_NO_RELOC (M114C097)','S1_HO_SUCC_SAME_MME_NEW_SGW (M114C073)','HO_SUCC_HO_REQ_NO_RELOC (M114C100)',
                'auth','ATT_AUTH_REQ_UE (M101C013)','SUCC_AUTH_REQ_UE (M101C021)','comb_attach','ATT_NON_EPS_ATCH (M118C299)','SUCC_NON_EPS_ATCH (M118C321)']]
            c.append(cmm)
        except Exception as e:
            print('error from cmm ',e)
            1
    except Exception as e:
        print(e)
        continue
pd.concat(v).to_csv('/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/Core QA/scheduled_reports/lte_ps_vusn_kpis_'+files4[-1][4:-4]+'.csv',index=False)
pd.concat(u).to_csv('/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/Core QA/scheduled_reports/lte_ps_usn_kpis_'+files4[-1][4:-4]+'.csv',index=False)
pd.concat(c).to_csv('/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/Core QA/scheduled_reports/lte_ps_cmm_kpis_'+files2[-1][:10]+'.csv',index=False)

