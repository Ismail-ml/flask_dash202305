import pandas as pd
import glob,sys
import os,datetime
import subprocess as sp
os.chdir('/disk2/support_files/archive/core')
h=[]
h2=[]
yesterday = datetime.date.today() - datetime.timedelta(3)
files = pd.date_range(start=yesterday,periods=3, freq='24H').strftime('core_new_%Y-%m-%d.h5').tolist()
folder = pd.date_range(start=yesterday,periods=3, freq='24H').strftime('%Y/%B/%d.%m.%Y').tolist()
files2 = pd.date_range(start=yesterday,periods=3, freq='24H').strftime('_60_%Y%m%d1900').tolist()
main ='/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/RAN QA/Daily/Raw_counters/Core/'
for num,i in enumerate(files):
      try:
            to_filter=i[9:-3]+' 20:00'
            df=pd.read_hdf(i,'pag_per_lac',where='Date=to_filter')
            df.drop_duplicates(inplace=True)
            df2=pd.read_hdf(i,'auth_sms_vlr',where='Date=to_filter')
            df2.drop_duplicates(inplace=True)
            per_lac=df.groupby('Date').sum().reset_index()
            vlr=df2.groupby('Date').sum().reset_index()
            subs=per_lac.merge(vlr,on='Date',how='outer')
            subs['Local VLR Attach subs']=subs['VLR IMSI attached Subscribers']-subs['VLR IMSI Attached Inter Roam Subs']-subs['VLR IMSI Attached National Roam Subs']
            subs['Local 2G subs']=subs['VLR 2G LAI Subs']-subs['International Roam 2G LAI Subs']-subs['National Roam 2G LAI Subs']
            subs['Local 3G subs']=subs['VLR 3G LAI Subs']-subs['International Roam 3G LAI Subs']-subs['National Roam 3G LAI Subs']
            subs['day']=subs['Date'].dt.date
            subs=subs[['day','VLR Total Subscribers','VLR 2G LAI Subs','VLR 3G LAI Subs','VLR IMSI attached Subscribers','IMSI Attached 2G LAI Subs','IMSI Attached 3G LAI Subs',
                  'VLR Local Subs','Local VLR Attach subs','Local 2G subs','Local 3G subs','VLR International Roam Subs','International Roam 2G LAI Subs',
                  'International Roam 3G LAI Subs','VLR IMSI Attached Inter Roam Subs','IMSI Attached International Roam 2G LAI Subs',
                  'IMSI Attached International Roam 3G LAI Subs','National Roam 2G LAI Subs','National Roam 3G LAI Subs',
                  'VLR National Roam Subs','VLR IMSI Attached National Roam Subs','IMSI Attached National Roam 2G LAI Subs','IMSI Attached National Roam 3G LAI Subs']]
            subs['a']=''
            #
            df=pd.read_hdf(i,'traf')
            df.drop_duplicates(inplace=True)
            a=df[~df['Site'].str.contains('UP')].groupby(['Date','MNO']).sum().reset_index()
            a['eps_bears']=a['Max_simult_act_PGW_EPS_bearer']+a['Max_simult_act_S_PGW_EPS_bearer']
            a['2G_3G_bear']=a['Max_active_utran_bearers']+a['Max_active_geran_bearers']
            a['total_eps']=a['Max_simult_act_PGW_EPS_bearer']+a['Max_simult_act_S_PGW_EPS_bearer']+a['Max_simult_act_PDP_context']+\
                              a['Max_active_utran_bearers']+a['Max_active_geran_bearers']+a['Max_active_eutran_bearers']
            #a['total_bear']=a['Max_active_utran_bearers']+a['Max_active_geran_bearers']+a['Max_active_eutran_bearers']
            a=a.groupby('MNO').max().reset_index()
            a['day']=df['Date'].dt.date.values[0]
            a2=a[a['MNO']=='Azerfon'].merge(a[a['MNO']=='Bakcell'],on='day',how='outer')
            a2=a2[['day','Max_simult_act_PDP_context_y','eps_bears_y','Max_simult_act_PDP_context_x','eps_bears_x','2G_3G_bear_y','Max_active_eutran_bearers_y',
                  '2G_3G_bear_x','Max_active_eutran_bearers_x','total_eps_y','total_eps_x']]
            #
            total=subs.merge(a2,on='day',how='outer')
            total[['b','c','d','e','f','g','gu']]=''
            df=pd.read_hdf(i,'usn_pdp_plmn')
            df.drop_duplicates(inplace=True)
            df['day']=df['Date'].dt.date
            df['total_attach']=df['Gb mode max attached user']+df['Iu mode max attached user']+df['S1 mode max attached user']
            df['total_pdp_context']=df['Gb mode max user with pdp activated context']+df['Iu mode max user with pdp activated context']+df['S1 mode max PDN connection num']
            user_t=df.copy()
            df2=df.groupby(['Date','Mobile_country_code','Mobile_network_code']).sum().reset_index()
            df3=df2[df2['Mobile_country_code']=='400'].groupby(['Mobile_country_code','Mobile_network_code']).max().reset_index()
            df3['day']=df['day'].values[0]
            df4=df[df['Mobile_country_code']!='400'].groupby('Date').sum().max().reset_index().T
            df4.columns=df4.iloc[0,:]
            df4=df4.iloc[-1:]
            df4['day']=df['day'].values[0]
            df5=df3.loc[df3['Mobile_network_code']=='02',df3.columns[0:]].merge(df3.loc[df3['Mobile_network_code']=='04',df3.columns[0:]],on='day',how='outer')
            n=df5.merge(df4,on='day',how='outer')
            user=n[['day','Gb mode max attached user_y','Gb mode max attached user_x','Gb mode max attached user',
                  'Iu mode max attached user_y','Iu mode max attached user_x','Iu mode max attached user',
                  'S1 mode max attached user_y','S1 mode max attached user_x','S1 mode max attached user',
                  'total_attach_y','total_attach_x','total_attach']]
            user[['total_2g_attach','total_3g_attach','total_4g_attach']]=df.groupby('Date').sum()[['Gb mode max attached user','Iu mode max attached user',\
                                                                                                'S1 mode max attached user']].max().values
            pdp=n[['day','Gb mode max user with pdp activated context_y','Gb mode max user with pdp activated context_x','Gb mode max user with pdp activated context',
                        'Iu mode max user with pdp activated context_y','Iu mode max user with pdp activated context_x','Iu mode max user with pdp activated context',
                        'S1 mode max PDN connection num_y','S1 mode max PDN connection num_x','S1 mode max PDN connection num']]
            pdp[['total_2g_pdp','total_3g_pdp','total_4g_pdp']]=df.groupby('Date').sum()[['Gb mode max user with pdp activated context','Iu mode max user with pdp activated context',\
                                                                                          'S1 mode max PDN connection num']].max().values
            pdp[['total_pdp_context_y','total_pdp_context_x','total_pdp_context']]=n[['total_pdp_context_y','total_pdp_context_x','total_pdp_context']]
            pdp['network_pdp']=df.groupby('Date').sum()[['total_pdp_context']].max().values
            total=total.merge(user,on='day',how='outer')
            total[['h','i','j','k','l','m','n','o']]=''
            total=total.merge(pdp,on='day',how='outer')

            df = pd.read_hdf(i, 'pag_per_lac',where='Date=to_filter')
            df.drop_duplicates(inplace=True)
            df=df.groupby('Date').sum().reset_index()
            #total['SG']=df['SGs Associated Subs'].values
            h.append(total)

            df=pd.read_hdf(i,'usn_kpi')
            df=df[['Date','Site','mode','Max_home_subs','Max_visiting_subs','Max attach user']]
            df.drop_duplicates(inplace=True)
            a=df[df['Site'].str.contains('@')].groupby(['Date','mode']).sum().reset_index()
            b=a.groupby('mode',as_index=False).max()[['mode','Max attach user']].T
            b=b.iloc[1:]
            b['BKC'],b['visiting']=zip(*a[a['mode']=='Gb mode'].groupby('mode').max()[['Max_home_subs','Max_visiting_subs']].values)
            b['inroamer']=b[['BKC','visiting']].sum(axis=1)*0.55/100
            b.insert(5,'AZF',b['visiting']-b['inroamer'])
            b['inroam_2g']=b.loc[:,0]/100
            b['inroam_3g']=b.loc[:,1]*0.45/100
            b['a']=None
            b['azrc_2G_data_user']=b.loc[:,0]-b['inroam_2g']+user[['Gb mode max attached user_y','Gb mode max attached user_x']].sum(axis=1).values
            b['azrc_3G_data_user']=b.loc[:,1]-b['inroam_3g']+user[['Iu mode max attached user_y','Iu mode max attached user_x']].sum(axis=1).values
            db=user_t[(user_t['Mobile_country_code']=='400') & (user_t['Mobile_network_code']=='02')].groupby('Date').sum()
            da=user_t[(user_t['Mobile_country_code']=='400') & (user_t['Mobile_network_code']=='04')].groupby('Date').sum()
            dc=user_t[(user_t['Mobile_country_code']!='400')].groupby('Date').sum()
            new_mode=df[(df['mode']=='Gb mode') & (df['Site'].str.contains('@'))].groupby('Date',as_index=False).sum()
            new_mode['lte_bkc_subs']=new_mode.merge(db,on='Date',how='left')['S1 mode max attached user'].values
            new_mode['lte_azf_subs']=new_mode.merge(da,on='Date',how='left')['S1 mode max attached user'].values
            new_mode['inroamer']=new_mode[['Max_home_subs','Max_visiting_subs']].sum(axis=1)*0.55/100
            new_mode['AZF']=new_mode['Max_visiting_subs']-new_mode['inroamer']
            b['lte_azf']=round(max(new_mode['AZF']+new_mode['lte_azf_subs']),0)
            b['lte_bkc']=round(max(new_mode['Max_home_subs']+new_mode['lte_bkc_subs']),0)
            b.insert(len(b.columns)-2,'lte_azrc',b['lte_azf']+b['lte_bkc'])
            new_mode['lte_inroam_subs']=new_mode.merge(dc,on='Date',how='left')['S1 mode max attached user'].values
            new_mode['tot_2G']=new_mode.merge(df[df['mode']=='Gb mode'].groupby('Date',as_index=False).sum(),on='Date',how='left',\
                              suffixes={'','_x'})['Max attach user_x'].values+new_mode.merge(user_t.groupby('Date',as_index=False).sum(),on='Date',how='left',\
                              )['Gb mode max attached user'].values
            new_mode['nn']=new_mode.merge(user_t.groupby('Date',as_index=False).sum(),on='Date',how='left',suffixes={'','_x'})['S1 mode max attached user'].values
            new_mode['nn2']=new_mode.merge(user_t.groupby('Date',as_index=False).sum(),on='Date',how='left',suffixes={'','_x'})['Gb mode max attached user'].values
            new_mode['nn3']=new_mode.merge(user_t.groupby('Date',as_index=False).sum(),on='Date',how='left',suffixes={'','_x'})['Iu mode max attached user'].values
            b['lte_inroam']=max(new_mode['inroamer']+new_mode['lte_inroam_subs'])
            b['tot_2G']=round(max(new_mode.merge(df[(df['mode']=='Gb mode') & (df['Site'].str.contains('@'))].groupby('Date',as_index=False).sum(),\
                        on='Date',how='left',suffixes={'','_x'})['Max attach user_x'].values+new_mode.merge(user_t.groupby('Date',as_index=False).sum(),\
                        on='Date',how='left',suffixes={'','_x'})['Gb mode max attached user'].values),0)
            b['tot_3G']=round(max(new_mode.merge(df[(df['mode']=='Iu mode') & (df['Site'].str.contains('@'))].groupby('Date',as_index=False).sum(),\
                        on='Date',how='left',suffixes={'','_x'})['Max attach user_x'].values+new_mode.merge(user_t.groupby('Date').sum(),\
                        on='Date',how='left',suffixes={'','_x'})['Iu mode max attached user'].values),0)
            b['lte_total']=round(max(new_mode[['inroamer','lte_inroam_subs','AZF','lte_azf_subs','Max_home_subs','lte_bkc_subs']].sum(axis=1)),0)
            b['sg']=per_lac['SGs Associated Subs'].values
            b['totalll']=max(df[(df['Site'].str.contains('@')) & (df['mode']=='Gb mode')].groupby('Date').sum()['Max attach user'].values+\
                        df[(df['Site'].str.contains('@')) & (df['mode']=='Iu mode')].groupby('Date').sum()['Max attach user'].values+\
                              new_mode[['Max_home_subs','AZF','inroamer','nn','nn2','nn3']].sum(axis=1).values)
            b.insert(0,'Date',df['Date'].dt.date.iloc[0])

            h2.append(b)
      except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            print(e)
            print(i)
            continue
pd.concat(h).to_csv('/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/Core QA/scheduled_reports/users_'+files2[-1][4:-4]+'.csv',index=False)
pd.concat(h2).to_csv('/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/Core QA/scheduled_reports/users_cmm_'+files2[-1][4:-4]+'.csv',index=False)
#pd.concat(h2).to_csv('/home/ismayil/Documents/cs_kpis2.csv',index=False)
