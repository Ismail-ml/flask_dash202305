import pandas as pd
import glob
import os
os.chdir('/disk2/support_files/archive/core')
lac_mapping=pd.read_excel('/home/ismayil/flask_dash/support_files/core_mapping.xlsx',sheet_name='2&3G LAC Region')
tac_mapping=pd.read_excel('/home/ismayil/flask_dash/support_files/core_mapping.xlsx',sheet_name='TAC Region')
#print(glob.glob(r'core_new*.h5'))
for i in glob.glob(r'core_new*.h5'):
    try:
        df=pd.HDFStore(i,'r')
        b=df.select('/abcf_basics',where='Date>="2023-05-05 10:00"').drop_duplicates()
        c=df.select('/ats_basics',where='Date>="2023-05-05 10:00"').drop_duplicates()
        d=df.select('/auth_sms_vlr',where='Date>="2023-05-05 10:00"').drop_duplicates()
        e=df.select('/csfb_pag',where='Date>="2023-05-05 10:00"').drop_duplicates()
        g=df.select('ho_intramsc',where='Date>="2023-05-05 10:00"').drop_duplicates()
        f=df.select('/mo_mt_ccr',where='Date>="2023-05-05 10:00"').drop_duplicates()
        h=df.select('/scsf',where='Date>="2023-05-05 10:00"').drop_duplicates()
        i=df.select('/srvcc',where='Date>="2023-05-05 10:00"').drop_duplicates()
        j=df.select('/traf',where='Date>="2023-05-05 10:00"').drop_duplicates()
        j=j.groupby(['Date','Site','MNO']).sum().reset_index()
        k=df.select('/usn_kpi',where='Date>="2023-05-05 10:00"').drop_duplicates()
        o=df.select('/usn_pdp_plmn',where='Date>="2023-05-05 10:00"').drop_duplicates()
        p=df.select('/ugw_pdp',where='Date>="2023-05-05 10:00"').drop_duplicates()
        q=df.select('/hss_subs',where='Date>="2023-05-05 10:00"').drop_duplicates()
        r=df.select('/ims_usn',where='Date>="2023-05-05 10:00"').drop_duplicates()
        s=df.select('/pag_per_lac',where='Date>="2023-05-05 10:00"').drop_duplicates()
        t=df.select('/ps_pag',where='Date>="2023-05-05 10:00"').drop_duplicates()
        #u=
        #v=
        #ww=
        #aa=
        #ab=
        #ac=
        #ad=
        if len(b)<1: 
            df.close()
            continue
        if 'Granularity Period' in b.columns: b.drop(columns='Granularity Period',inplace=True)
        #b=b.groupby(['Date','Site']).sum().reset_index()
        b['Site']=b['Site'].str[:6]
        #print(b)
        #c=c.groupby(['Date','Site']).sum().reset_index()
        c['Site']=c['Site'].str[:6]
        #print(c)
        b=b.merge(c,on=['Date','Site'],how='outer',suffixes=['','_ats'])
        del c
        #d=d.groupby(['Date','Site']).sum().reset_index()
        b=b.merge(d,on=['Date','Site'],how='outer',suffixes=['','_auth'])
        b=b.merge(q,on=['Date','Site'],how='outer',suffixes=['','hss_subs'])
        del q
        b=b.merge(r,on=['Date','Site'],how='outer',suffixes=['','ims'])
        del r
        b['HO_type']='All'
        b=b.merge(g,on=['Date','Site','HO_type'],how='outer',suffixes=['','_intramsc'])
        del d
        b['Entity_name']='All'
        b=b.merge(e,on=['Date','Site','Entity_name'],how='outer',suffixes=['','_csfb_pag'])
        del e
        b['Direction']='All'
        b=b.merge(f,on=['Date','Site','Direction'],how='outer',suffixes=['','_ccr'])
        del f
        b[['Type','Identifier']]='All'
        h['Site']=h['Site'].str[:6]
        b=b.merge(h,on=['Date','Site','Type','Identifier'],how='outer',suffixes=['','_scsf'])
        del h
        i.rename(columns={'Peer_entity':'Entity_name'},inplace=True)
        b=b.merge(i,on=['Date','Site','Entity_name'],how='outer',suffixes=['','_srvcc'])
        del i
        #b['gpname_id']='All'
        p=p.groupby(['Date','Site']).sum().reset_index()
        b=b.merge(p,on=['Date','Site'],how='outer',suffixes=['','_ugw_pdp'])
        del p
        b['mode']='All'
        k=k.groupby(['Date','Site','mode']).sum().reset_index()
        b=b.merge(k,on=['Date','Site','mode'],how='outer',suffixes=['','_usn_kpi'])
        del k
        b['MNO']='All'
        o2=o[(o['Mobile_country_code']=='400') & (o['Mobile_network_code']=='02')].groupby(['Date','Site']).sum().reset_index()
        o2['MNO']='Bakcell'
        o3=o[(o['Mobile_country_code']=='400') & (o['Mobile_network_code']=='04')].groupby(['Date','Site']).sum().reset_index()
        o3['MNO']='Azerfon'
        o4=o[o['Mobile_country_code']!='400'].groupby(['Date','Site']).sum().reset_index()
        o4['MNO']='International_MNO'
        o=pd.concat([o2,o3,o4])
        b=b.merge(o,on=['Date','Site','MNO'],how='outer',suffixes=['','_usn_plmn'])
        del o
        del o2
        del o3
        del o4
        #b['APN']='AZRC'
        b=b.merge(j,on=['Date','Site','MNO'],how='outer',suffixes=['','_traf'])
        del j
        s['LAC']=s['LAC'].astype(int)
        lac_mapping['LAC']=lac_mapping['LAC'].astype(int)
        tac_mapping['TAC']=tac_mapping['TAC'].astype(int)
        s=s.merge(lac_mapping[['LAC','Region']],on='LAC',how='left')
        s=s.drop(columns='LAC').groupby(['Date','Site','Region','BSC_RNC']).sum().reset_index()
        b[['BSC_RNC','Region']]='All'
        #s['LAC']=s['LAC'].astype(str)
        b=b.merge(s,on=['Date','Site','Region','BSC_RNC'],how='outer',suffixes=['','_cs_pag'])
        del s
        #b['RAC']='All'
        #t[['LAC','RAC']]=t[['LAC','RAC']].astype(str)
        t['LAC']=t['LAC'].astype(int)
        t=t.merge(lac_mapping[['LAC','Region']],on='LAC',how='left')
        t2=t.loc[t['Region'].isnull()]
        t2=t2.merge(tac_mapping[['TAC','Region']],left_on='LAC',right_on='TAC',how='left')
        t2.drop(columns=['Region_x','TAC','LAC','RAC'],inplace=True)
        t2.rename(columns={'Region_y':'Region'},inplace=True)
        t.drop(columns=['LAC','RAC'],inplace=True)
        t=pd.concat([t[t['Region'].notnull()],t2])
        t=t.groupby(['Date','Site','Region','mode']).sum().reset_index()
        b=b.merge(t,on=['Date','Site','mode','Region'],how='outer',suffixes=['','_ps_pag'])
        del t
        del t2
        #l=list(b.columns)
        #[l.remove(i) for i in ['Date','Site','HO_type','Entity_name','Direction','Type','Identifier','mode','MNO','APN','BSC_RNC','Region']]
        #b[l]=b[l].astype(float)
        #print(b.columns)
        b['Date']=pd.to_datetime(b['Date'])
        b.drop_duplicates(inplace=True)
        if os.path.isfile('/home/ismayil/Documents/core_ready.csv'):
            b.to_csv('/home/ismayil/Documents/core_ready.csv',mode='a',index=False,header=False)
        else:
            b.to_csv('/home/ismayil/Documents/core_ready.csv',mode='a',index=False)
        df.close()
    except Exception as e:
        df.close()
        print(e)
        continue