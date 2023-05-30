import pandas as pd
import glob
import os,datetime,sys
import subprocess as sp
from send_notification import send_mail
from datetime import datetime as dt
os.chdir('/disk2/support_files/archive/core')
vb,vd,vh,dd,dh=[[] for i in range(5)]
yesterday = datetime.date.today() - datetime.timedelta(3)
files = pd.date_range(start=yesterday,periods=3, freq='24H').strftime('core_new_%Y-%m-%d.h5').tolist()
folder = pd.date_range(start=yesterday,periods=3, freq='24H').strftime('%Y/%B/%-d.%m.%Y').tolist()
files2 = pd.date_range(end=datetime.date.today(), periods=4, freq='24H').strftime('core_new_%Y-%m-%d.h5').tolist()
main ='/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/RAN QA/Daily/Raw_counters/Core/'
for num,i in enumerate(files):
    try:
        df=pd.read_hdf(i,'ab_vtraff')
        df.drop_duplicates(inplace=True)
        filt=(df['Office'].str.contains('BSC')) & (df['Office'].str.contains('H'))
        df.loc[filt,'BSC']='HBSC'+df.loc[filt,'Office'].apply(lambda x: x[-2:])
        filt=(df['Office'].str.contains('RNC')) & (df['Office'].str.contains('H'))
        df.loc[filt,'BSC']='HRNC'+df.loc[filt,'Office'].apply(lambda x: x[-2:])
        df2=pd.read_hdf(i,'abcf_basics')
        df2.drop_duplicates(inplace=True)
        a=pd.DataFrame(df.groupby('Office').sum()['Inc_out and HO seizure traffic_erl'])
        a=a[a['Inc_out and HO seizure traffic_erl']>0].T
        a.reset_index(drop=True)
        a.insert(1,'Date',df['Date'].dt.date.iloc[0])
        a[['Date','BSCGANH01','BSCGANH02','BSCAGSH03','BSCSHKH04', 'BSCZAGH05','BSCKHAH06','BSCLANH07','BSCGANH08','BSCQUBH09','BSCSVNH10', 'BSCSVNH11','BSCKURH12', 
            'MCBSCCS3N12','MCBSCBTCN16','ASBSCBTCN21', 'ASBSCCS3N22','RNCGANH01','RNCLANH02', 'RNCSHKH03','RNCKHAH04', 'RNCQUBH05','RNCSVNH07','IPQRBHRNC08','MCRNCBTCN08',
            'MCRNCBTCN09','ASRNCBTCN21']]
        b=df.pivot_table(index='Date',columns='Office',values='Inc_out and HO seizure traffic_erl',aggfunc='sum').reset_index()
        b['day']=b['Date'].dt.date
        b['hour']=b['Date'].dt.hour.astype(str)+':00'
        b=b[['day','hour','BSCGANH01','BSCGANH02','BSCAGSH03','BSCSHKH04', 'BSCZAGH05','BSCKHAH06','BSCLANH07','BSCGANH08','BSCQUBH09','BSCSVNH10', 'BSCSVNH11','BSCKURH12', 
            'MCBSCCS3N12','MCBSCBTCN16','ASBSCBTCN21', 'ASBSCCS3N22','RNCGANH01','RNCLANH02', 'RNCSHKH03','RNCKHAH04', 'RNCQUBH05','RNCSVNH07','IPQRBHRNC08','MCRNCBTCN08',
            'MCRNCBTCN09','ASRNCBTCN21']]
        b['a']=None
        b['2g_3g']=b.drop(columns=['day','hour','a']).sum(axis=1)
        b['volte']=df2.groupby('Date').sum()['Connected ABCF Traf_erl'].values
        b['tot']=b['2g_3g']+b['volte']
        b.loc[b['hour']=='23:00','bh']=b.groupby('day').max()['tot'].values
        vh.append(b)

        c=pd.DataFrame(b.groupby('day').max().reset_index())
        c.insert(1,'Date',df['Date'].dt.date.iloc[0])
        c=c[['Date','BSCGANH01','BSCGANH02','BSCAGSH03','BSCSHKH04', 'BSCZAGH05','BSCKHAH06','BSCLANH07','BSCGANH08','BSCQUBH09','BSCSVNH10', 'BSCSVNH11','BSCKURH12', 
            'MCBSCCS3N12','MCBSCBTCN16','ASBSCBTCN21', 'ASBSCCS3N22','RNCGANH01','RNCLANH02', 'RNCSHKH03','RNCKHAH04', 'RNCQUBH05','RNCSVNH07','IPQRBHRNC08','MCRNCBTCN08',
            'MCRNCBTCN09','ASRNCBTCN21']]
        c['a']=None
        c['volte']=df2.groupby('Date').sum().max()['Connected ABCF Traf_erl']
        c['2g_3g']=c.drop(columns=['Date','volte','a']).sum(axis=1)
        vb.append(c)

        d=pd.DataFrame(b.groupby('day').sum().reset_index())
        d.insert(1,'Date',df['Date'].dt.date.iloc[0])
        d=d[['Date','BSCGANH01','BSCGANH02','BSCAGSH03','BSCSHKH04', 'BSCZAGH05','BSCKHAH06','BSCLANH07','BSCGANH08','BSCQUBH09','BSCSVNH10', 'BSCSVNH11','BSCKURH12', 
            'MCBSCCS3N12','MCBSCBTCN16','ASBSCBTCN21', 'ASBSCCS3N22','RNCGANH01','RNCLANH02', 'RNCSHKH03','RNCKHAH04', 'RNCQUBH05','RNCSVNH07','IPQRBHRNC08','MCRNCBTCN08',
            'MCRNCBTCN09','ASRNCBTCN21']]
        #df=pd.read_hdf('core_new_2023-02-17.h5','abcf_basics')
        d['a']=None
        d['2g']=d[d.columns[d.columns.str.contains('BSC')]].sum().sum()
        d['3g']=d[d.columns[d.columns.str.contains('RNC')]].sum().sum()
        d['2g_3g']=d['2g']+d['3g']
        d['volte']=df2.groupby('Date').sum().sum()['Connected ABCF Traf_erl']
        d['tot']=d['2g_3g']+d['volte']
        df3=df.groupby('Date').sum().reset_index()
        df3['volte']=df2.groupby('Date').sum()['Connected ABCF Traf_erl'].values
        df3['tot']=df3['volte']+df3['Inc_out and HO seizure traffic_erl']
        d['tot_bh']=df3.max()['tot']
        vd.append(d)
    except Exception as uu:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        print(uu)
        continue
traf=[]
for num,i in enumerate(files2):
    try:
        df=pd.read_hdf(i,'traf')
        df.drop_duplicates(inplace=True)
        if len(df.loc[(df['MNO'].isnull()) & (df[['2G/3G DL', '2G/3G UL', '4G DL', '4G UL']].sum(axis=1)>0)])>0:
            traf.append(df.loc[(df['MNO'].isnull()) & (df[['2G/3G DL', '2G/3G UL', '4G DL', '4G UL']].sum(axis=1)>0),['APN','MNO','2G/3G DL','2G/3G UL','4G DL','4G UL']].drop_duplicates())
        a=df.groupby(['MNO','Date']).sum()[['2G/3G DL', '2G/3G UL', '4G DL', '4G UL']].reset_index()
        a[['2G/3G DL', '2G/3G UL', '4G DL', '4G UL']]/=1024
        b=a.round(0).sort_values(by=['MNO','Date'])
        b.insert(4,'2G_3G total',b['2G/3G DL']+b['2G/3G UL'])
        b['4G total']=b['4G DL']+b['4G UL']
        b['DL total']=b['4G DL']+b['2G/3G DL']
        b['UL total']=b['4G UL']+b['2G/3G UL']
        b['total']=b['DL total']+b['UL total']
        cols=[i+'_thrp' for i in b.columns[2:]]
        b[cols]=b.loc[:,b.columns[2:]]*8/3600
        b.loc[(b['MNO']=='Azerfon') & (b['Date'].dt.hour==23),['netw_thrp','ul_thrp','lte_ul_thrp','2G_3G_ul_thrp']]=b[b['MNO']=='Azerfon'][['total_thrp','UL total_thrp','4G UL_thrp','2G/3G UL_thrp']].max().values
        b.loc[(b['MNO']=='Bakcell') & (b['Date'].dt.hour==23),['netw_thrp','ul_thrp','lte_ul_thrp','2G_3G_ul_thrp']]=b[b['MNO']=='Bakcell'][['total_thrp','UL total_thrp','4G UL_thrp','2G/3G UL_thrp']].max().values
        b.loc[b['MNO']=='Bakcell',['azf_2g_3g_thrp','azf_lte_thrp','azf_total_thrp']]=b.loc[b['MNO']=='Azerfon',['2G_3G total_thrp','4G total_thrp','total_thrp']].values
        b.loc[b['MNO']=='Bakcell','Azrc_total_thrp']=b.loc[b['MNO']=='Bakcell',['total_thrp','azf_total_thrp']].sum(axis=1).values
        b.loc[(b['MNO']=='Bakcell') & (b['Date'].dt.hour==23),'Azrc_max_thrp']=b[b['MNO']=='Bakcell'][['Azrc_total_thrp']].max().values
        #b.round(0).to_csv('data_traf.csv',index=False)
        dh.append(b)

        if len(df['Date'].unique())<24: continue
        b.insert(1,'Day',b['Date'].dt.date)
        c=b[['MNO','Day','2G/3G DL', '2G/3G UL', '2G_3G total', '4G DL', '4G UL','4G total', 'DL total', 'UL total', 'total']].groupby(['MNO','Day']).sum()/1024
        c.reset_index(inplace=True)
        c.loc[:,['2G/3G DL_thrp','2G/3G UL_thrp', '2G_3G total_thrp', '4G DL_thrp', '4G UL_thrp','4G total_thrp', 'DL total_thrp', 'UL total_thrp', 'total_thrp']]=\
            b.loc[b['total_thrp'].isin(b['netw_thrp'].values),['2G/3G DL_thrp','2G/3G UL_thrp', '2G_3G total_thrp', '4G DL_thrp', '4G UL_thrp','4G total_thrp', 
                                                                'DL total_thrp', 'UL total_thrp', 'total_thrp']].values
        c[['ul_thrp', 'lte_ul_thrp', '2G_3G_ul_thrp']]=b.loc[b['Date'].dt.hour==23,['ul_thrp', 'lte_ul_thrp', '2G_3G_ul_thrp']].values
        c['a']=None
        c.insert(1,'b',None)
        c.insert(1,'c',None)
        d=pd.read_hdf(i,'mobile_cacti')
        d.drop_duplicates(inplace=True)
        d['tot']=d['DL']+d['UL']
        #print(d.loc[(d['Site'].str.contains('BKC')) & (d['Date'].dt.day==int(i[-5:-3]))])
        c.loc[c['MNO']=='Bakcell','isp']=d.loc[(d['Site'].str.contains('BKC')) & (d['Date'].dt.day==int(i[-5:-3]))].groupby('Date').sum().max()['tot']/1000/1000
        c.loc[c['MNO']=='Azerfon','isp']=d.loc[(d['Site'].str.contains('AZF')) & (d['Date'].dt.day==int(i[-5:-3]))].groupby('Date').sum().max()['tot']/1000/1000
        c=c.round(0)
        c['delta']=round((c['total_thrp']-c['isp'])/c['total_thrp']*100,2)
        dd.append(c)
        print(i,' done')
    except Exception as uu:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        print(uu,' from ',i)
        #print(i)
        continue
if len(traf)>0:
    if dt.now().hour==9:
        send_mail(['ismayilm@azerconnect.az','zahidh@azerconnect.az','yanab@azerconnect.az'],'New APN','There is no MNO mapping for below APNs',\
                    pd.concat(traf).drop_duplicates().groupby('APN').sum().reset_index(),datetime.datetime.strftime(df['Date'].dt.date.iloc[0],'%d.%m.%Y'),False)
try:
    pd.concat(vb).to_csv('/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/Core QA/scheduled_reports/network_cs_traf_bh_'+files2[-1][9:-3]+'.csv',index=False)
except: 1
try:
    pd.concat(vh).to_csv('/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/Core QA/scheduled_reports/network_cs_traf_hourly_'+files2[-1][9:-3]+'.csv',index=False)
except: 1
try:
    pd.concat(vd).to_csv('/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/Core QA/scheduled_reports/network_cs_traf_dt_'+files2[-1][9:-3]+'.csv',index=False)
except: 1
try:
    a=pd.concat(dh).drop(columns='Day').sort_values(by=['MNO','Date'])
    a.to_csv('/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/Core QA/scheduled_reports/data_traf_hourly_'+files2[-1][9:-3]+'.csv',index=False)
except: 1
try:
    b=pd.concat(dd).sort_values(by=['MNO','Day'])
    b.to_csv('/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/Core QA/scheduled_reports/data_traf_dt_'+files2[-1][9:-3]+'.csv',index=False)
except Exception as e: 
    print(e)
    print(pd.concat(dd))
    1