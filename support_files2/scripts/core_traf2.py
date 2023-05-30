import pandas as pd
import datetime
import os

#existing_files = pd.read_csv(os.path.join('/home/ismayil/flask_dash/data/core', 'files.txt'), sep=" ", header=None)


#apn_mapping=pd.read_csv(r'/home/ismayil/flask_dash/support_files/apn_mapping.csv')
apn_mapping=pd.read_excel('/mnt/raw_counters/Corporate Folder/CTO/Technology Governance and Central Support/Core QA/Mapping for schedule/mapping.xlsx',sheet_name='APN')

lu_sr,pag,cst,pdp,s1,traf=([] for i in range(6))
to_save=[]
files={'1_abcf_sess':[[],0],'1_ats_basic':[[],0],'2_scscf':[[],0],'3_scscf':[[],0],'4-2g3g_esrvcc':[[],0],'5_volte_ims':[[],0]}
for file in os.listdir('/home/ismayil/flask_dash/data/core'):
    try:
        if '_qar' in file: continue
        if ('LU_SR' in file) or ('Pag_SR' in file) or ('C_setup' in file) or ('usn_kpis' in file) or ('LTE_rep' in file) or ('UGW DT' in file) or ('vUGW_DT' in file):
            df=pd.read_csv(os.path.join('/home/ismayil/flask_dash/data/core',file),skiprows=[1])
            if ('_UGW DT' in file) and (('bearers' not in file) or ('AZF' not in file)):
                df['2G/3G UL']=df['134706896'] #KB
                df['2G/3G DL']=df['134706897']
                df['4G UL']=df['138413069']+df['142608004']
                df['4G DL']=df['138413071']+df['142608006']
                df['Date']=pd.to_datetime(df['Result Time'],format='%Y-%m-%d %H:%M')
                df['CMG_name']=df['Object Name'].map(lambda x: x.split('/')[0])
                df['APN']=df['Object Name'].map(lambda x: x.split('/')[1])
                df=df[['Date','APN','CMG_name','4G DL','4G UL','2G/3G DL','2G/3G UL']]
                filter_apn=df['APN'].str.contains('APN=')
                df.loc[filter_apn,'APN']=df.loc[filter_apn,'APN'].map(lambda x: x[x.find('APN=')+4:])
                df=df.merge(apn_mapping,how='left', left_on='APN', right_on='APN ID')
                df=df[['Date','MNO','APN','CMG_name','2G/3G DL','2G/3G UL','4G DL','4G UL']]
                df[['2G/3G DL','2G/3G UL','4G DL','4G UL']]=df[['2G/3G DL','2G/3G UL','4G DL','4G UL']].astype('float')
                traf.append(df)                
            elif ('vUGW_DT' in file) and (('bearers' not in file) or ('AZF' not in file)):
                df['2G/3G UL']=df['1911621400']  
                df['2G/3G DL']=df['1911621401']  
                df['4G UL']=df['1911620086']+df['1911621989']
                df['4G DL']=df['1911620088']+df['1911621991']
                df['Date']=pd.to_datetime(df['Result Time'],format='%Y-%m-%d %H:%M')
                df['CMG_name']=df['Object Name'].map(lambda x: x.split('/')[0])
                df['APN']=df['Object Name'].map(lambda x: x.split('/')[1])
                df=df[['Date','APN','CMG_name','4G DL','4G UL','2G/3G DL','2G/3G UL']]
                filter_apn=df['APN'].str.contains('APN=')
                df.loc[filter_apn,'APN']=df.loc[filter_apn,'APN'].map(lambda x: x[x.find('APN=')+4:])
                df=df.merge(apn_mapping,how='left', left_on='APN', right_on='APN ID')
                df=df[['Date','MNO','APN','CMG_name','2G/3G DL','2G/3G UL','4G DL','4G UL']]
                df[['2G/3G DL','2G/3G UL','4G DL','4G UL']]=df[['2G/3G DL','2G/3G UL','4G DL','4G UL']].astype('float')
                traf.append(df)
            else:
                if ('usn_kpis' in file) or ('LTE_rep' in file):
                    df_g=df  
                else:
                    df_g=df.groupby('Result Time').sum()
                    df_g.reset_index(inplace=True)
                df_g['Date']=pd.to_datetime(df_g['Result Time'])
            
            if 'LU_SR' in file:
                #df_g['LU_SR_VLR']=df_g['84151990']/df_g['84151989']*100
                #df_g['LU_SR_Roaming']=df_g['84151992']/df_g['84151991']*100
                df_gg=df_g[['Date','84151989','84151990','84151991','84151992']]
                lu_sr.append(df_gg)
            elif 'Pag_SR' in file:
                #df_g['2G_PAG_SR']=(df_g['84152186']+df_g['84152188'])/df_g['84152185']*100
                #df_g['3G_PAG_SR']=(df_g['84152192']+df_g['84152190'])/df_g['84152189']*100
                df_gg=df_g[['Date','84152186','84152188','84152185','84152192','84152190','84152189']]
                pag.append(df_gg)
            elif 'C_setup' in file:
                #df_g['2G_CST']=df_g['84148248']/df_g['84148235']
                #df_g['3G_CST']=df_g['84148278']/df_g['84148265']
                df_gg=df_g[['Date','84148248','84148235','84148278','84148265']]
                cst.append(df_gg)
            elif 'usn_kpis' in file:
                #df_g['2G_Attach_SR']=df_g['117454514']/df_g['117454513']*100
                #df_g['3G_Attach_SR']=df_g['117456614']/df_g['117456613']*100
                #df_g['2G_PDP_Ctx_SR']=df_g['117458514']/df_g['117458513']*100
                #df_g['3G_PDP_Ctx_SR']=df_g['117459414']/df_g['117459413']*100
                df_g.rename(columns={'117454514':'TwoG_attach_num','117454513':'TwoG_attach_den','117456614':'ThreeG_attach_num','117456613':'ThreeG_attach_den',
                            '117458514':'TwoG_pdp_num','117458513':'TwoG_pdp_den','117459414':'ThreeG_pdp_num','117459413':'ThreeG_pdp_den','Object Name':'Site'},inplace=True)
                df_g['Site']=df_g['Site'].apply(lambda x: x.split('/')[0])
                df_gg=df_g[['Date','Site','TwoG_attach_num','TwoG_attach_den','ThreeG_attach_num','ThreeG_attach_den',
                            'TwoG_pdp_num','TwoG_pdp_den','ThreeG_pdp_num','ThreeG_pdp_den']]
                df_gg[['TwoG_attach_num','TwoG_attach_den','ThreeG_attach_num','ThreeG_attach_den',
                            'TwoG_pdp_num','TwoG_pdp_den','ThreeG_pdp_num','ThreeG_pdp_den']]=df_gg[['TwoG_attach_num','TwoG_attach_den','ThreeG_attach_num','ThreeG_attach_den',
                            'TwoG_pdp_num','TwoG_pdp_den','ThreeG_pdp_num','ThreeG_pdp_den']].astype(float)
                pdp.append(df_gg)
            elif 'LTE_rep' in file:
                #df_g['Bearer_Setup_SR']=df_g['117495953']/df_g['117495952']*100
                df_g.rename(columns={'117495953':'bearer_setup_num','117495952':'bearer_setup_den','Object Name':'Site'},inplace=True)
                df_g['Site']=df_g['Site'].apply(lambda x: x.split('/')[0])
                df_gg=df_g[['Date','Site','bearer_setup_num','bearer_setup_den']]
                df_gg[['bearer_setup_num','bearer_setup_den']]=df_gg[['bearer_setup_num','bearer_setup_den']].astype(float)
                s1.append(df_gg)
        else:
            if "1_abcf_sess" in file:
                files['1_abcf_sess'][0].append(pd.read_csv(os.path.join('/home/ismayil/flask_dash/data/core',file),skiprows=[1]))
            elif '1_ats_basic' in file:
                files['1_ats_basic'][0].append(pd.read_csv(os.path.join('/home/ismayil/flask_dash/data/core',file),skiprows=[1]))
            elif '2_scscf' in file:
                files['2_scscf'][0].append(pd.read_csv(os.path.join('/home/ismayil/flask_dash/data/core',file),skiprows=[1]))
            elif '3_scscf' in file:
                files['3_scscf'][0].append(pd.read_csv(os.path.join('/home/ismayil/flask_dash/data/core',file),skiprows=[1]))
            elif '4-2g3g_esrvcc' in file:
                files['4-2g3g_esrvcc'][0].append(pd.read_csv(os.path.join('/home/ismayil/flask_dash/data/core',file),skiprows=[1]))
            elif '5_volte_ims' in file:
                files['5_volte_ims'][0].append(pd.read_csv(os.path.join('/home/ismayil/flask_dash/data/core',file),skiprows=[1]))
        to_save.append(file)
    except Exception as e:
        print(e)
        continue


for i in ['lu_sr','pag','cst','pdp','s1','traf']:
    try:
        if len(i)>0:
            concated= pd.concat(eval(i))
            for j in concated['Date'].unique():
                try:
                    file_name = datetime.datetime.strftime(datetime.datetime.utcfromtimestamp(j.astype(datetime.datetime) / 1e9),"%B_%Y")
                    file_name2 = datetime.datetime.strftime(datetime.datetime.utcfromtimestamp(j.astype(datetime.datetime) / 1e9),"%Y-%m-%d")
                    if i=='traf':
                        concated.loc[concated['Date']==j].to_hdf('/disk2/support_files/archive/core/core_'+file_name2+'.h5',i,append=True,
                            format='table', data_columns=['Date', 'APN', 'MNO', 'CMG_name'], complevel=5,
                            min_itemsize={'APN': 100, 'MNO': 20, 'CMG_name': 100})
                    elif i in ['pdp','s1']:
                        concated.loc[concated['Date']==j].to_hdf('/disk2/support_files/archive/core/core_'+file_name2+'.h5',i,append=True,
                            format='table', data_columns=['Date','Site'], min_itemsize={'Site': 100}, complevel=5)
                    else:
                        concated.loc[concated['Date']==j].to_hdf('/disk2/support_files/archive/core/core_'+file_name2+'.h5',i,append=True,
                            format='table', data_columns=['Date'], complevel=5)
                except Exception as u:
                    print(u, ' exception raised')
                    continue
    except Exception as e: 
        print(e)
        print('error from tradditional part')
        continue
print(' pdp part finished.')
##pd.DataFrame(os.listdir(os.path.join('/home/ismayil/flask_dash/data', 'core'))).to_csv(os.path.join('/home/ismayil/flask_dash/data', 'files.csv'), mode='a', header=None, index=False)

#print(files)

for f in files.keys():
    try:
        df=pd.concat(files[f][0])
        df.columns=pd.Series(df.columns).apply(lambda x: str(x)).values
        df=df.groupby("Result Time",as_index=False).sum()
        if f=='1_abcf_sess':
            df["Total Daily Traffic,Erl"]=df["1912981109"]
            df["Total daily calls"]=df["1912981071"]+df["1912981074"]
            df["MO Call Completion rate"]=df["1912981071"]/df["1912981070"]*100
            df["MT Call Completion rate"]=df["1912981074"]/df["1912981073"]*100
            df["Call Interrupt rate"]=(df["1912981143"]+df["1912981144"])/(df["1912981071"]+df["1912981074"])*100
            df.rename(columns={'Result Time':'Date'},inplace=True)
            files[f][1]=5
        elif f=='1_ats_basic':
            df["Call setup time"]=(df["478154861"]+df["478154862"])/2/1000
            df.rename(columns={'Result Time':'Date'},inplace=True)
            files[f][1]=1
        elif f=='2_scscf':
            df["Initial Registration SR"]=df["335672722"]/df["335672721"]*100
            df["Re-Registration SR"]=df["335672725"]/df["335672724"]*100
            df.rename(columns={'Result Time':'Date'},inplace=True)
            files[f][1]=2
        elif f=='3_scscf':
            df["Authentication SR"]=df["335672444"]/df["335672443"]*100
            df.rename(columns={'Result Time':'Date'},inplace=True)
            files[f][1]=1
        elif f=='4-2g3g_esrvcc':
            df["LTE to UMTS eSRVCC Handover SR"]=(df["84175691"]+df["84176700"]+df["84176438"])/(df["84164975"]+df["84164977"])*100
            df.rename(columns={'Result Time':'Date'},inplace=True)
            files[f][1]=1
        elif f=='5_volte_ims':
            df["PDN connect activation SR"]=df["117499713"]/df["117499712"]*100
            df["Voice Bearer Handover SR"]=(df["117499818"]+df["117499819"]+df["117499814"]+df["117499815"])/(df["117499816"]+df["117499817"]+df["117499812"]+df["117499813"])*100
            df.rename(columns={'Result Time':'Date'},inplace=True)
            files[f][1]=2
        df['Date']=pd.to_datetime(df['Date'])
    #    #if os.path.exists(f+'.csv'):
    #    #    df[['Date',*df.columns[-files[f][1]:]]].to_csv(f+'.csv',index=False,mode='a',header=False)
    #    #else:
    #    #    df[['Date',*df.columns[-files[f][1]:]]].to_csv(f+'.csv',index=False,mode='a')
        df['Date']=pd.to_datetime(df['Date'])
        for j in df['Date'].unique():
            file_name = datetime.datetime.strftime(datetime.datetime.utcfromtimestamp(j.astype(datetime.datetime) / 1e9),"%B_%Y")
            file_name2 = datetime.datetime.strftime(datetime.datetime.utcfromtimestamp(j.astype(datetime.datetime) / 1e9),"%Y-%m-%d")
            df.loc[df['Date']==j].to_hdf('/disk2/support_files/archive/core/core_'+file_name2+'.h5',f,append=True,
                    format='table', data_columns=['Date'], complevel=5)
    except Exception as e:
        print(e)
        print('error from volte part')
        continue
#pd.DataFrame(os.listdir(os.path.join('/home/ismayil/flask_dash/data', 'core'))).to_csv(os.path.join('/home/ismayil/flask_dash/data', 'files.csv'), mode='a', header=None, index=False)
pd.DataFrame(to_save).to_csv(os.path.join('/home/ismayil/flask_dash/data', 'files.csv'), mode='a', header=None, index=False)
#try:
#    import utilization_hourly
#    utilization_hourly.run()
#except Exception as e:
#    print(e)
#    1
#try:     
#    import cem
#    cem.run()
#except Exception as e:
#    print(e)
#    1
try:
    import core_traf_cmg as cm
    cm.run('dasd')
except Exception as e:
    print(e,' error from nokia core')
    1

#try:
#    import core_inputs_for_bi
#    core_inputs_for_bi.run()
#except Exception as ll: 
#    print(ll,' from core inputs')
#    1
