import pandas as pd
import datetime
import os
gun=datetime.datetime.strftime(datetime.date.today()- datetime.timedelta(days=1),'%Y-%m-%d')
df=pd.HDFStore('/disk2/support_files/archive/core/core_new_'+gun+'.h5','r')
reference={'/ab_int':['MSSGANH03','MSSBTCH02','MSSBHQH01'], '/ab_vtraff':['MSSGANH03', 'MSSBTCH02', 'MSSBHQH01'], 
'/abcf_basics':['btch02_CloudSE298002', 'cs3h03_CloudSE298002'], '/ats_basics':['btch02_ATS01', 'cs3h03_ATS01'], 
'/auth_sms_vlr':['MSSGANH03', 'MSSBTCH02', 'MSSBHQH01'], '/csfb_pag':['MSSGANH03', 'MSSBTCH02', 'MSSBHQH01'], 
'/ho_intramsc':['MSSGANH03', 'MSSBTCH02', 'MSSBHQH01'], '/hss_cpu_mem':['HSSBHQH01', 'HSSBTCH02'], 
'/hss_kpi':['HSSBHQH01', 'HSSBTCH02'], '/hss_subs':['SDBBHQH01', 'SDBBTCH02'], 
'/ims_usn':['USN9810BHQ01', 'USN9810BTC02', 'vUSN'], '/in_kpi':['MSSGANH03', 'MSSBTCH02', 'MSSBHQH01'], 
'/interconnect':['MSSGANH03', 'MSSBTCH02', 'MSSBHQH01'], '/mgw_mua':['MGWGANH03', 'MGWSVNH04', 'MGWNAKH05', 'MGWBHQH01', 'MGWBTCH02','MGWSVNH07'], 
'/mo_mt_ccr':['MSSGANH03', 'MSSBTCH02', 'MSSBHQH01'], 
'/pag_per_lac':['MSSGANH03', 'MSSBTCH02', 'MSSBHQH01'], '/ps_pag':['vUSN', 'USN9810BHQ01', 'USN9810BTC02', '10.64.168.36@cmmta-btc'],
'/scsf':['btch02_CSCF01', 'cs3h03_CSCF01'], '/srvcc':['MSSGANH03', 'MSSBTCH02', 'MSSBHQH01'],
'/traf':['UGWBHQ', 'UGWBTCH02', 'vDGW', '10.34.169.65@CSN3_CMG-CP','10.34.170.1@CSN3_CMG-UP', '10.64.169.65@BTC_CMG-CP',
       '10.64.170.1@BTC_CMG-UP'], '/ugw_pdp':['UGWBHQ', 'UGWBTCH02', '10.34.170.1@CSN3_CMG-UP',
       '10.64.169.65@BTC_CMG-CP', '10.64.170.1@BTC_CMG-UP'], 
'/usn_kpi':['USN9810BHQ01', 'USN9810BTC02', 'vUSN', '10.34.168.36@cmmta-ta','10.64.168.36@cmmta-btc'], '/usn_pdp_plmn':['USN9810BHQ01', 'USN9810BTC02', 'vUSN']}
#, '/mobile_cacti':['BKC BTC ISP', 'BKC BHQ ISP', 'AZF BTC ISP', 'AZF BHQ ISP','AZRC BTC', 'AZRC BHQ']}

h=[]
h2={}
h4=pd.DataFrame()
for i in df.keys():
    try:
        if (i=='/cem_cst') | (i=='/mobile_cacti'): continue
        a=df.select(i)
        b=pd.date_range(start=gun,freq='H',periods=24)
        a['tot']=a.select_dtypes(include='number').sum(axis=1)
        k=a.groupby(['Date','Site']).sum().reset_index().query('tot==0')
        sites=k.groupby('Site',as_index=False).count()[['Site','tot']].query('tot<24')['Site'].values
        #h4=pd.DataFrame()
        h4.loc[i,['missing_site','new_site','missing_date','missing_data_on_site']]=None
        h4[['missing_site','new_site','missing_date','missing_data_on_site']]=h4[['missing_site','new_site','missing_date','missing_data_on_site']].astype('object')
        h4.at[i,'missing_date']=[j for j in b if j not in a['Date'].unique()]
        h4.at[i,'missing_site']=list(set(reference[i])-set(a.pivot_table(index='Date',columns='Site',aggfunc='count',values=a.columns[-1]).columns))
        h4.at[i,'new_site']=list(set(a.pivot_table(index='Date',columns='Site',aggfunc='count',values=a.columns[-1]).columns)-set(reference[i]))
        if len(sites)>0:
            h4.at[i,'missing_data_on_site']=k[k['Site'].isin(sites)][['Date','Site']].values
        #h.append(h4)
    except:
        print(i)

df.close()
#h4=pd.concat(h)
h4=h4[([i!=[] for i in h4['missing_site']] and [i!=[] for i in h4['new_site']] and [i!=[] for i in h4['missing_date']]) | (h4['missing_data_on_site'].notnull())]
if len(h4)>0: 
    from send_notification import send_mail
    dist=['ismayilm@azerconnect.az','zahidh@azerconnect.az','yanab@azerconnect.az']
    send_mail(dist,'Missing Data check','Missing data observed in the file',h4.reset_index(),gun,False)

