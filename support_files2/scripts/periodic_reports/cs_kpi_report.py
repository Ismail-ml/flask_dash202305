import pandas as pd
import glob,sys
import os,datetime
import subprocess as sp
os.chdir('/disk2/support_files/archive/core')
h=[]
h2=[]
yesterday = datetime.date.today() - datetime.timedelta(3)
files = pd.date_range(start=yesterday,periods=3, freq='24H').strftime('core_new_%Y-%m-%d.h5').tolist()
folder = pd.date_range(start=yesterday,periods=3, freq='24H').strftime('%Y/%B/%-d.%m.%Y').tolist()
files2 = pd.date_range(start=yesterday,periods=3, freq='24H').strftime('_60_%Y%m%d2000').tolist()
main ='/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/RAN QA/Daily/Raw_counters/Core/'
for num,i in enumerate(files):
    try:
        to_filter=i[9:-3]+' 20:00'
        df=pd.read_hdf(i,'mo_mt_ccr',where='Date=to_filter')
        df.drop_duplicates(inplace=True)
        df=df.groupby(['Date','Direction']).sum().reset_index()
        df['2G_call_comp_sr']=round(df['TwoG Call Completion']/df['TwoG Call Attempt']*100,2)
        df['3G_call_comp_sr']=round(df['ThreeG Call Completion']/df['ThreeG Call Attempt']*100,2)
        df['call_comp_sr']=round((df['TwoG Call Completion']+df['ThreeG Call Completion'])/(df['TwoG Call Attempt']+df['ThreeG Call Attempt'])*100,2)
        df2=df[['Date','Direction','2G_call_comp_sr','TwoG Call Attempt','TwoG Call Completion','3G_call_comp_sr','ThreeG Call Attempt','ThreeG Call Completion','call_comp_sr']]
        #pd.concat([df2[df2['Direction']=='MO'].iloc[:,2:],df2[df2['Direction']=='MT'].iloc[:,2:]])
        call_comp=df2[df2['Direction']=='MO'].merge(df2[df2['Direction']=='MT'],on='Date',how='outer').drop(columns=['Direction_x','Direction_y'])
        df['2G_cst']=round(df['TwoG Call Setup time']/df['TwoG Call Completion'],2)
        df['3G_cst']=round(df['ThreeG Call Setup time']/df['ThreeG Call Completion'],2)
        df['cst']=round((df['TwoG Call Setup time']+df['ThreeG Call Setup time'])/(df['TwoG Call Completion']+df['ThreeG Call Completion']),2)
        cst=df.loc[df['Direction']=='MO',['Date','cst','2G_cst','3G_cst','TwoG Call Setup time','TwoG Call Completion','ThreeG Call Setup time','ThreeG Call Completion']]
        df=pd.read_hdf(i,'pag_per_lac',where='Date=to_filter')
        df.drop_duplicates(inplace=True)
        #print(glob.glob(os.path.join(main,folder[num])+'/*'+files2[num]+'*qar_PAG-LU-Subs_per_LAI*.csv')[0])
        try:
            from_file=pd.read_csv(glob.glob(os.path.join(main,folder[num])+'/*'+files2[num]+'*qar_PAG-LU-Subs_per_LAI*.csv')[0],skiprows=[1])
        except:
            print('no file in server')
            print(os.path.join(main,folder[num])+'/*'+files2[num]+'*qar_PAG-LU-Subs_per_LAI*.csv')
            try:
                script='sshpass -p "qarftp" scp qarftp@10.240.104.155:Huawei/*'+files2[num]+'*qar_PAG-LU-Subs_per_LAI*.csv /home/ismayil/flask_dash/data/core'
                sp.run(script,shell=True)
                from_file=pd.read_csv(glob.glob('/home/ismayil/flask_dash/data/core/*'+files2[num]+'*qar_PAG-LU-Subs_per_LAI*.csv')[0],skiprows=[1])
                sp.run('rm /home/ismayil/flask_dash/data/core/*.csv',shell=True)
            except:
                print('no file in qarftp')
                from_file=pd.DataFrame()
                1
        df2=df[df['LAC']!=1505].groupby('Date').sum().reset_index()
        df2['pag_sr']=round((df2['ThreeG_CS Paging SR num']+df2['TwoG_CS Paging SR num'])/(df2['ThreeG_CS Paging SR den']+df2['TwoG_CS Paging SR den'])*100,2)
        df2['2G_pag_sr']=round((df2['TwoG_CS Paging SR num'])/(df2['TwoG_CS Paging SR den'])*100,2)
        df2['3G_pag_sr']=round((df2['ThreeG_CS Paging SR num'])/(df2['ThreeG_CS Paging SR den'])*100,2)
        df2['2G_pag_s']=0
        df2['3G_pag_s']=0
        if len(from_file)>0:
            from_file['LAC']=from_file['Object Name'].apply(lambda x: int(str(x[-4:]),16))
            from_file2=from_file[from_file['LAC']!=1505].groupby('Result Time').sum().reset_index()
            df2['TwoG_CS Paging SR num']=from_file2['84152186'].values
            df2['2G_pag_s']=from_file2['84152188'].values
            df2['ThreeG_CS Paging SR num']=from_file2['84152190'].values
            df2['3G_pag_s']=from_file2['84152192'].values
        df3=df2[['Date','pag_sr','2G_pag_sr','3G_pag_sr','TwoG_CS Paging SR den','TwoG_CS Paging SR num','2G_pag_s','ThreeG_CS Paging SR den','ThreeG_CS Paging SR num','3G_pag_s']]
        df2=df[df['LAC']==1505].groupby('Date').sum().reset_index().copy()
        df2['pag_sr']=round((df2['ThreeG_CS Paging SR num']+df2['TwoG_CS Paging SR num'])/(df2['ThreeG_CS Paging SR den'])*100,2)
        df2['pag_num']=(df2['ThreeG_CS Paging SR num']+df2['TwoG_CS Paging SR num'])
        df2['2G_3G_pag_s']=0
        if len(from_file)>0:
            from_file['LAC']=from_file['Object Name'].apply(lambda x: int(str(x[-4:]),16))
            from_file=from_file[from_file['LAC']==1505].groupby('Result Time').sum().reset_index()
            df2['pag_num']=from_file['84152186'] + from_file['84152190']
            df2['2G_3G_pag_s']=from_file['84152188'].values+from_file['84152192'].values
        df2=df2[['Date','pag_sr','ThreeG_CS Paging SR den','pag_num','2G_3G_pag_s']]
        df3=df3.merge(df2,on='Date',how='outer')
        df3['total_pag_att']=df3['TwoG_CS Paging SR den']+df3['ThreeG_CS Paging SR den_x']+df3['ThreeG_CS Paging SR den_y']
        
        pag=df3
        df=df.groupby('Date').sum().reset_index()
        df['vlr_loc_sr']=round(df['VLR Location Update Success']/df['VLR Location Update Requests']*100,2)
        df['roam_loc_sr']=round(df['Roaming Location Update Success']/df['Roaming Location Update Requests']*100,2)
        df['loc_sr']=round((df['VLR Location Update Success']+df['Roaming Location Update Success'])/(df['VLR Location Update Requests']+df['Roaming Location Update Requests'])*100,2)
        vlr=df[['Date','loc_sr','vlr_loc_sr','roam_loc_sr','VLR Location Update Requests','Roaming Location Update Requests','VLR Location Update Success','Roaming Location Update Success']]
        total=call_comp.merge(pag,on='Date',how='outer')
        df=pd.read_hdf(i,'auth_sms_vlr',where='Date=to_filter')
        df.drop_duplicates(inplace=True)
        df=df.groupby('Date').sum().reset_index()
        df['mo_sms_sr']=round(df['SMS MO Success']/df['SMS MO Attempt']*100,2)
        df['mt_sms_sr']=round(df['SMS MT Success']/df['SMS MT Attempt']*100,2)
        df['auth_sr']=round(df['Auth_success']/df['Auth_requests']*100,2)
        sms=df[['Date','mo_sms_sr','SMS MO Attempt','SMS MO Success','mt_sms_sr','SMS MT Attempt','SMS MT Success']]
        auth=df[['Date','auth_sr','Auth_requests','Auth_success']]
        total=total.merge(sms,on='Date',how='outer')
        total=total.merge(vlr,on='Date',how='outer')
        df=pd.read_hdf(i,'ho_intramsc',where='Date=to_filter')
        df.drop_duplicates(inplace=True)
        df2=df.groupby('Date').sum().reset_index()
        df2['sr']=round(df2['Intra_MSC_HO_success']/df2['Intra_MSC_HO_request']*100,2)
        df2=df2[['Date','sr','Intra_MSC_HO_request','Intra_MSC_HO_success']]
        df3=df.groupby(['Date','HO_type']).sum().reset_index()
        df3['sr']=round(df3['Intra_MSC_HO_success']/df3['Intra_MSC_HO_request']*100,2)
        df3=df3[['Date','HO_type','sr','Intra_MSC_HO_request','Intra_MSC_HO_success']]
        df3=df3.query('HO_type=="ho2g"').merge(df3.query('HO_type=="ho2gto3g"')[['Date','sr','Intra_MSC_HO_request','Intra_MSC_HO_success']],on='Date',how='outer').\
            merge(df3.query('HO_type=="ho3gto2g"')[['Date','sr','Intra_MSC_HO_request','Intra_MSC_HO_success']],on='Date',how='outer').\
            merge(df3.query('HO_type=="ho3g"')[['Date','sr','Intra_MSC_HO_request','Intra_MSC_HO_success']],on='Date',how='outer')
        ho=df2.merge(df3,on='Date',how='outer').drop(columns='HO_type')
        total=total.merge(ho,on='Date',how='outer')
        total[['a','b','c','d','e','f']]=0
        total=total.merge(auth,on='Date',how='outer')
        total=total.merge(cst,on='Date',how='outer')
        df=pd.read_hdf(i,'csfb_pag',where='Date=to_filter')
        df.drop_duplicates(inplace=True)
        df=df.groupby(['Date']).sum().reset_index()
        df['csfb_pag_sr']=round(df['Paging response']/(df['CSFB First paging request']+df['CSFB Second paging request']+df['CSFB Third paging request'])*100,2)
        df['csfb_1st_pag_sr']=round(df['Paging response']/df['CSFB First paging request']*100,2)
        csfb_pag=df[['Date','csfb_pag_sr','csfb_1st_pag_sr','CSFB First paging request', 'CSFB Second paging request','CSFB Third paging request', 'Paging response']]
        total=total.merge(csfb_pag,on='Date',how='outer')
        total['Date']=total['Date'].dt.date
        if len(total)>0:
            h.append(total)
        a=pd.read_hdf(i,'mo_mt_ccr',where='Date=to_filter')
        a.drop_duplicates(inplace=True)
        a['total_calls']=a[['TwoG Call Attempt','ThreeG Call Attempt']].sum(axis=1)
        a['total_erlang']=a[['TwoG Seized Traffic_Erl','ThreeG Seized Traffic_Erl']].sum(axis=1)
        b=a.T
        b.columns=b.loc['Site',:].values+b.loc['Direction',:].values
        b=b[['MSSBHQH01MO','MSSBTCH02MO','MSSGANH03MO','MSSBHQH01MT','MSSBTCH02MT','MSSGANH03MT']]
        c=pd.DataFrame(pd.concat([b.loc['TwoG Call Attempt',['MSSBHQH01MO','MSSBTCH02MO','MSSGANH03MO']],
                                b.loc['TwoG Seized Traffic_Erl',['MSSBHQH01MO','MSSBTCH02MO','MSSGANH03MO']],
                                b.loc['ThreeG Call Attempt',['MSSBHQH01MO','MSSBTCH02MO','MSSGANH03MO']],
                                b.loc['ThreeG Seized Traffic_Erl',['MSSBHQH01MO','MSSBTCH02MO','MSSGANH03MO']],
                                b.loc['total_calls',['MSSBHQH01MO','MSSBTCH02MO','MSSGANH03MO']],
                                b.loc['total_erlang',['MSSBHQH01MO','MSSBTCH02MO','MSSGANH03MO']],
                                a.groupby('Direction').sum().T.loc[['total_calls','total_erlang'],'MO'],pd.DataFrame(index=['n']),
                                b.loc['TwoG Call Attempt',['MSSBHQH01MT','MSSBTCH02MT','MSSGANH03MT']],
                                b.loc['TwoG Seized Traffic_Erl',['MSSBHQH01MT','MSSBTCH02MT','MSSGANH03MT']],
                                b.loc['ThreeG Call Attempt',['MSSBHQH01MT','MSSBTCH02MT','MSSGANH03MT']],
                                b.loc['ThreeG Seized Traffic_Erl',['MSSBHQH01MT','MSSBTCH02MT','MSSGANH03MT']],
                                b.loc['total_calls',['MSSBHQH01MT','MSSBTCH02MT','MSSGANH03MT']],
                                b.loc['total_erlang',['MSSBHQH01MT','MSSBTCH02MT','MSSGANH03MT']],
                                a.groupby('Direction').sum().T.loc[['total_calls','total_erlang'],'MT']])).T
        print(a.reset_index()['Date'].dt.date[0])
        c.insert(0,'Date',a.reset_index()['Date'].dt.date[0])
        h2.append(c)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        print(e)
        continue
pd.concat(h).to_csv('/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/Core QA/scheduled_reports/cs_kpis_'+files2[-1][4:-4]+'.csv',index=False)
pd.concat(h2).to_csv('/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/Core QA/scheduled_reports/cs_kpis2_'+files2[-1][4:-4]+'.csv',index=False)