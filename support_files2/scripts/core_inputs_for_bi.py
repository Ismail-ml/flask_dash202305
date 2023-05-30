import pandas as pd
import os
import datetime
import numpy as np

def run():
    os.chdir('/disk2/support_files/archive/core')
    latest_file=max(os.listdir(),key=os.path.getctime)
    time_to_filter=datetime.datetime.strftime(datetime.datetime.now()+datetime.timedelta(hours=-1),'%Y-%m-%d %H:00')
    df=pd.HDFStore('/disk2/support_files/archive/core/'+latest_file,'r')
    try:
        b=df.select('1_abcf_sess',where='Date=time_to_filter')
        c=df.select('2_scscf',where='Date=time_to_filter')
        d=df.select('3_scscf',where='Date=time_to_filter')
        e=df.select('4-2g3g_esrvcc',where='Date=time_to_filter')
        f=df.select('5_volte_ims',where='Date=time_to_filter')
        h=df.select('cem_cst',where='Date=time_to_filter')
        i=df.select('cst',where='Date=time_to_filter')
        j=df.select('lu_sr',where='Date=time_to_filter')
        k=df.select('pag',where='Date=time_to_filter')
        if 'Granularity Period' in b.columns: b.drop(columns='Granularity Period',inplace=True)
        b=b.merge(c,on='Date',how='left')
        b=b.merge(d,on='Date',how='left')
        if 'Granularity Period' in b.columns: b.drop(columns='Granularity Period',inplace=True)
        b=b.merge(e,on='Date',how='left')
        if 'Granularity Period' in b.columns: b.drop(columns='Granularity Period',inplace=True)
        b=b.merge(f,on='Date',how='left')
        if 'Granularity Period' in b.columns: b.drop(columns='Granularity Period',inplace=True)
        b=b.merge(h,on='Date',how='left')
        if 'Granularity Period' in b.columns: b.drop(columns='Granularity Period',inplace=True)
        b=b.merge(i,on='Date',how='left')
        if 'Granularity Period' in b.columns: b.drop(columns='Granularity Period',inplace=True)
        b=b.merge(j,on='Date',how='left')
        if 'Granularity Period' in b.columns: b.drop(columns='Granularity Period',inplace=True)
        b=b.merge(k,on='Date',how='left')
        if 'Granularity Period' in b.columns: b.drop(columns='Granularity Period',inplace=True)
        b.insert(2,'Site','AZRC')
        l=df.select('pdp',where='Date=time_to_filter')
        m=df.select('s1',where='Date=time_to_filter')
        b=b.merge(l,on=['Date','Site'],how='outer')
        if 'Granularity Period' in b.columns: b.drop(columns='Granularity Period',inplace=True)
        b=b.merge(m,on=['Date','Site'],how='outer')
        if 'Granularity Period' in b.columns: b.drop(columns='Granularity Period',inplace=True)
        b.insert(2,'MNO','AZRC')
        n=df.select('traf',where='Date=time_to_filter')
        n=n.groupby(['Date','MNO','CMG_name'],as_index=False).sum()
        n.rename(columns={'CMG_name':'Site'},inplace=True)
        n2=n.groupby(['Date','Site'],as_index=False).sum()
        n2.insert(1,'MNO','AZRC')
        n=pd.concat([n,n2])
        b=b.merge(n,on=['Date','Site','MNO'],how='outer')
        if 'Granularity Period' in b.columns: b.drop(columns='Granularity Period',inplace=True)
        df.close()
    except:
        df.close()

    df_core=b
    df_core.columns=[i.replace('/','_') for i in df_core.columns]
    df_core.columns=[i.replace(',','_') for i in df_core.columns]
    df_core.fillna(np.NaN,inplace=True)
    df_core['Date']=pd.to_datetime(df_core['Date'])
    df_core['Day']=df_core['Date'].dt.date
    df_core['Month']=df_core['Date'].dt.month
    df_core['Year']=df_core['Date'].dt.year.astype(str)
    df_core['Hour']=df_core['Date'].dt.hour
    df_core.to_csv('/disk2/support_files/atoti/bi/core_inputs.csv',index=False)
    print('Core inputs saved successfully')
