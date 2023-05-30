import pandas as pd
import glob
import os,datetime,zipfile
import subprocess as sp
os.chdir('/disk2/support_files/archive/core')
v,u,c=[],[],[]
yesterday = datetime.date.today() - datetime.timedelta(3)
files = pd.date_range(start=yesterday,periods=3, freq='24H').strftime('core_new_%Y-%m-%d.h5').tolist()
folder = pd.date_range(start=yesterday,periods=3, freq='24H').strftime('%Y/%B/%-d.%m.%Y').tolist()
files2 = pd.date_range(start=yesterday,periods=3, freq='24H').strftime('%Y_%m_%d-23_14_00').tolist()
main ='/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/RAN QA/Daily/Raw_counters/Nokia/common/counters/'
for num,i in enumerate(files):
    try:
        to_filter=i[9:-3]+' 22:00'
        try:
            df=pd.read_hdf(i,'usn_kpi',where='Date=to_filter')
            df.drop_duplicates(inplace=True)
            vusn=df[df['Site']=='vUSN']
            vusn['attach_sr']=round(vusn['Data accept']/vusn['Data attach']*100,2)
            vusn['inter_sgsn sr']=round(vusn['Inter_SGSN_MME RAU success']/vusn['Inter_SGSN_MME RAU request']*100,2)
            vusn['intra_sgsn sr']=round(vusn['Intra_SGSN_MME RAU success']/vusn['Intra_SGSN_MME RAU request']*100,2)
            vusn['paging sr']=round(100-vusn['packet paging none_response']/vusn['packet paging request']*100,2)
            vusn['init pdp sr']=round(vusn['MS init PDP_bear context act suc']/vusn['MS init PDP_bear context act']*100,2)
            a=vusn.loc[vusn['mode']=='Gb mode',['Date','Data attach','Data accept','Inter_SGSN_MME RAU request','Inter_SGSN_MME RAU success','Intra_SGSN_MME RAU request',
                    'Intra_SGSN_MME RAU success','packet paging request','packet paging none_response','MS init PDP_bear context act','MS init PDP_bear context act suc']].\
                merge(vusn.loc[vusn['mode']=='Iu mode',['Date','Data attach','Data accept','Inter_SGSN_MME RAU request','Inter_SGSN_MME RAU success',
                    'Intra_SGSN_MME RAU request','Intra_SGSN_MME RAU success','packet paging request','packet paging none_response','MS init PDP_bear context act',
                    'MS init PDP_bear context act suc']],on='Date',how='outer')
            a['gb_sim_req'],a['gb_usim_req']=zip(*vusn.loc[vusn['mode']=='Gb mode',['SIM Auth request','USIM Auth request']].values)
            a['iu_sim_req'],a['iu_usim_req']=zip(*vusn.loc[vusn['mode']=='Iu mode',['SIM Auth request','USIM Auth request']].values)
            a['gb_sim_suc'],a['gb_usim_suc']=zip(*vusn.loc[vusn['mode']=='Gb mode',['SIM Auth success','USIM Auth success']].values)
            a['iu_sim_suc'],a['iu_usim_suc']=zip(*vusn.loc[vusn['mode']=='Iu mode',['SIM Auth success','USIM Auth success']].values)
            a['gb_attach_sr'],a['gb_inter_sgsn sr'],a['gb_intra_sgsn sr'],a['gb_paging sr'],a['gb_init pdp sr']= \
                zip(*vusn.loc[vusn['mode']=='Gb mode',['attach_sr','inter_sgsn sr','intra_sgsn sr','paging sr','init pdp sr']].values)
            a['iu_attach_sr'],a['iu_inter_sgsn sr'],a['iu_intra_sgsn sr'],a['iu_paging sr'],a['iu_init pdp sr']= \
                zip(*vusn.loc[vusn['mode']=='Iu mode',['attach_sr','inter_sgsn sr','intra_sgsn sr','paging sr','init pdp sr']].values)
            a['auth sr']=round((a['gb_sim_suc']+a['gb_usim_suc']+a['iu_sim_suc']+a['iu_usim_suc'])/(a['gb_sim_req']+a['gb_usim_req']+a['iu_sim_req']+a['iu_usim_req'])*100,2)
            vusn=a
            vusn['Date']=vusn['Date'].dt.date
            v.append(vusn)
        except Exception as e:
            print('error from vusn ',e)
            1
        try:
            df=pd.read_hdf(i,'usn_kpi',where='Date=to_filter')
            df3=pd.read_hdf(i,'ugw_pdp',where='Date=to_filter')
            df.drop_duplicates(inplace=True)
            df3.drop_duplicates(inplace=True)
            df2=df3[df3['Site'].str.contains('UGW')].groupby('Date').sum().reset_index()
            df2['pdp sr']=round(df2['PDP success']/df2['PDP request']*100,2)
            usn=df[df['Site'].str.contains('USN9810')].groupby(['Date','mode']).sum().reset_index()
            usn['attach_sr']=round(usn['Data accept']/usn['Data attach']*100,2)
            usn['inter_sgsn sr']=round(usn['Inter_SGSN_MME RAU success']/usn['Inter_SGSN_MME RAU request']*100,2)
            usn['intra_sgsn sr']=round(usn['Intra_SGSN_MME RAU success']/usn['Intra_SGSN_MME RAU request']*100,2)
            usn['paging sr']=round(100-usn['packet paging none_response']/usn['packet paging request']*100,2)
            usn['init pdp sr']=round(usn['MS init PDP_bear context act suc']/usn['MS init PDP_bear context act']*100,2)
            a=usn.loc[usn['mode']=='Gb mode',['Date','Data attach','Data accept','Inter_SGSN_MME RAU request','Inter_SGSN_MME RAU success','Intra_SGSN_MME RAU request','Intra_SGSN_MME RAU success',
                'packet paging request','packet paging none_response','MS init PDP_bear context act','MS init PDP_bear context act suc']].\
                merge(usn.loc[usn['mode']=='Iu mode',['Date','Data attach','Data accept','Inter_SGSN_MME RAU request','Inter_SGSN_MME RAU success','Intra_SGSN_MME RAU request','Intra_SGSN_MME RAU success',
                'packet paging request','packet paging none_response','MS init PDP_bear context act','MS init PDP_bear context act suc']],on='Date',how='outer')
            a.insert(1,'PDP success',df2['PDP success'].values)
            a.insert(1,'PDP request',df2['PDP request'].values)
            a['gb_sim_req'],a['gb_usim_req']=zip(*usn.loc[usn['mode']=='Gb mode',['SIM Auth request','USIM Auth request']].values)
            a['iu_sim_req'],a['iu_usim_req']=zip(*usn.loc[usn['mode']=='Iu mode',['SIM Auth request','USIM Auth request']].values)
            a['gb_sim_suc'],a['gb_usim_suc']=zip(*usn.loc[usn['mode']=='Gb mode',['SIM Auth success','USIM Auth success']].values)
            a['iu_sim_suc'],a['iu_usim_suc']=zip(*usn.loc[usn['mode']=='Iu mode',['SIM Auth success','USIM Auth success']].values)
            a['pdp sr']=df2['pdp sr'].values
            a['gb_attach_sr'],a['gb_inter_sgsn sr'],a['gb_intra_sgsn sr'],a['gb_paging sr'],a['gb_init pdp sr']= \
                zip(*usn.loc[usn['mode']=='Gb mode',['attach_sr','inter_sgsn sr','intra_sgsn sr','paging sr','init pdp sr']].values)
            a['iu_attach_sr'],a['iu_inter_sgsn sr'],a['iu_intra_sgsn sr'],a['iu_paging sr'],a['iu_init pdp sr']= \
                zip(*usn.loc[usn['mode']=='Iu mode',['attach_sr','inter_sgsn sr','intra_sgsn sr','paging sr','init pdp sr']].values)
            a['auth sr']=round((a['gb_sim_suc']+a['gb_usim_suc']+a['iu_sim_suc']+a['iu_usim_suc'])/(a['gb_sim_req']+a['gb_usim_req']+a['iu_sim_req']+a['iu_usim_req'])*100,2)
            a['total_auth_request']=a['gb_sim_req']+a['gb_usim_req']+a['iu_sim_req']+a['iu_usim_req']
            usn=a
            usn['Date']=usn['Date'].dt.date
            u.append(usn)
        except Exception as e:
            print('error from usn ',e)
            1
        try:
            df=pd.read_hdf(i,'usn_kpi',where='Date=to_filter')
            df.drop_duplicates(inplace=True)
            df3=pd.read_hdf(i,'ugw_pdp',where='Date=to_filter')
            df3.drop_duplicates(inplace=True)
            cmm=df[df['Site'].str.contains('@')].groupby(['Date','mode']).sum().reset_index()
            df2=df3[~df3['Site'].str.contains('UGW')].groupby('Date').sum().reset_index()
            df2['pdp sr']=round(df2['PDP success']/df2['PDP request']*100,2)
            cmm['attach_sr']=round(cmm['Data accept']/cmm['Data attach']*100,2)
            cmm['inter_sgsn sr']=round(cmm['Inter_SGSN_MME RAU success']/cmm['Inter_SGSN_MME RAU request']*100,2)
            cmm['intra_sgsn sr']=round(cmm['Intra_SGSN_MME RAU success']/cmm['Intra_SGSN_MME RAU request']*100,2)
            cmm['paging sr']=round(100-cmm['packet paging none_response']/cmm['packet paging request']*100,2)
            cmm['init pdp sr']=round(cmm['MS init PDP_bear context act suc']/cmm['MS init PDP_bear context act']*100,2)
            cmm['auth sr']=round(cmm['Total Auth success']/cmm['Total Auth request']*100,2)
            a=cmm.loc[cmm['mode']=='Gb mode',['Date','attach_sr','Data attach','Data accept','inter_sgsn sr','Inter_SGSN_MME RAU request','Inter_SGSN_MME RAU success',
                'intra_sgsn sr','Intra_SGSN_MME RAU request','paging sr','packet paging request','packet paging none_response','init pdp sr','MS init PDP_bear context act','MS init PDP_bear context act suc']].\
                merge(cmm.loc[cmm['mode']=='Iu mode',['Date','attach_sr','Data attach','Data accept','inter_sgsn sr','Inter_SGSN_MME RAU request','Inter_SGSN_MME RAU success',
                'intra_sgsn sr','Intra_SGSN_MME RAU request','paging sr','packet paging request','packet paging none_response','init pdp sr','MS init PDP_bear context act','MS init PDP_bear context act suc']],\
                    on='Date',how='outer')
            a.insert(1,'PDP success',df2['PDP success'].values)
            a.insert(1,'PDP request',df2['PDP request'].values)
            a.insert(1,'PDP SR',df2['pdp sr'].values)
            a['gb_auth_sr'],a['gb_sim_req'],a['gb_usim_req']=zip(*cmm.loc[cmm['mode']=='Gb mode',['auth sr','Total Auth request','Total Auth success']].values)
            a['a']=0 # to add from_file
            a['iu_auth_sr'],a['iu_sim_req'],a['iu_usim_req']=zip(*cmm.loc[cmm['mode']=='Iu mode',['auth sr','Total Auth request','Total Auth success']].values)
            a['a2']=0 # to add from_file
            try:
                files3 = zipfile.ZipFile(glob.glob(os.path.join(main,folder[num])+'/*Core_KPIs*'+files2[num]+'*.zip')[0], 'r')
                from_file=pd.read_csv(files3.open(files3.namelist()[2]), sep=';')
            except:
                print('no file in server')
                print(os.path.join(main,folder[num])+'/*Core_KPIs*'+files2[num]+'.zip')
                try:
                    script='sshpass -p "qarftp" scp qarftp@10.240.104.155:Nokia/*Core_KPIs*'+files2[num]+'*.zip /home/ismayil/flask_dash/data/nokia/pool'
                    sp.run(script,shell=True)
                    files3 = zipfile.ZipFile(glob.glob('/home/ismayil/flask_dash/data/nokia/pool/*Core_KPIs*'+files2[num]+'*.zip')[0], 'r')
                    from_file=pd.read_csv(files3.open(files3.namelist()[2]), sep=';')
                    sp.run('rm /home/ismayil/flask_dash/data/core/*.csv',shell=True)
                except:
                    print('no file in qarftp')
                    from_file=pd.DataFrame()
                    1
            if len(from_file)>0:
                from_file=from_file.groupby('PERIOD_START_TIME').sum().reset_index()
                a['gb_usim_req'],a['a'],a['iu_usim_req'],a['a2'] =zip(*from_file.loc[:,['GSM_SUCC_MM_AUTH (M22C001)','GSM_SUCC_SM_AUTH (M22C002)',\
                                                                'UMTS_SUCC_MM_AUTH (M23C001)','UMTS_SUCC_SM_AUTH (M23C002)']].values)
                print('counter are put from zip file')
            a['Date']=a['Date'].dt.date
            c.append(a)
        except Exception as e:
            print('error from cmm ',e)
            1
    except Exception as e:
        print(e)
        continue
pd.concat(v).to_csv('/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/Core QA/scheduled_reports/2G_3G_ps_vusn_kpis_'+files2[-1][:10]+'.csv',index=False)
pd.concat(u).to_csv('/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/Core QA/scheduled_reports/2G_3G_ps_usn_kpis_'+files2[-1][:10]+'.csv',index=False)
pd.concat(c).to_csv('/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/Core QA/scheduled_reports/2G_3G_ps_cmm_kpis_'+files2[-1][:10]+'.csv',index=False)
