import mechanize
import pandas as pd
#from bs4 import BeautifulSoup
#import urllib2 
from http.cookiejar import CookieJar
import datetime
from datetime import datetime as dt
import os

import ssl

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
# Legacy Python that doesn't verify HTTPS certificates by default
    pass
else:
# Handle target environment that doesn't support HTTPS verification
    ssl._create_default_https_context = _create_unverified_https_context

yesterday = datetime.date.today() - datetime.timedelta(1)
yesterday2 = datetime.date.today() - datetime.timedelta(2)
start= int(dt.timestamp(dt.strptime(dt.strftime(yesterday2,'%d.%m.%Y')+' 23:50','%d.%m.%Y %H:%M')))
end= int(dt.timestamp(dt.strptime(dt.strftime(yesterday,'%d.%m.%Y')+' 23:50','%d.%m.%Y %H:%M')))
graph_id={'24643':'Sumqayit_655-SW/CNC-Aggregation - Traffic - Gi1/0/25 Uplink/Sumqayit2/4/Huawei/Azertelekom',
'267':'Astara_EATS-Aggregation - Traffic - 25 - AZRT',
'25643':'Agcabedi_EATS-Aggregation - Traffic - 24/24',
'24001':'Agdash_EATS-Aggregation - Traffic - port 25',
'946':'Agstafa_EATS-Aggregation - Traffic - 25-AZRT',
'25592':'Balaken_EATS-Aggregation - Traffic - 25 - Uplink_AZRT',
'623':'Berde_EATS-Aggregation - Traffic - 5 - Uplink',
'24177':'Bilesuvar_EATS-Aggregation - Traffic - port 25',
'24012':'Celilabad_EATS-Aggregation - Traffic - port 28',
'24025':'Goranboy_EATS-Aggregation - Traffic - port 25',
'1112':'Goycay_EATS-Aggregation - Traffic - 27 - AZRT',
'25902':'Ismayilli_ATS-Agg - Traffic - Gi0/28 - Azrt_uplink',
'1164':'Kurdemir_EATS-Aggregation - Traffic - 25 - AZ',
'163':'Massalli_Bedelan_EATS-Aggregation - Traffic - 25 - Uplink',
'481':'Massalli_Boradigah_EATS-Aggregation - Traffic - 25 - Boradigah',
'24314':'Masalli_EATS-Aggregation - Traffic - Po1',
'401':'Masalli_EATS-Aggregation - Traffic - Gi0/49AZRT-LAG-1',
'402':'Masalli_EATS-Aggregation - Traffic - Gi0/50AZRT-LAG-2',
'26004':'Masalli_EATS-Aggregation - Traffic - Gi0/52 - AZRT-LAG-3',
'25623':'Mingecevir_4_EATS-Aggregation - Traffic - 25 - Uplink_AZRT',
'429':'Mingecevir_5_EATS-Aggregation - Traffic - 25 - MHM',
'54':'Qax_EATS-Aggregation - Traffic - 26-AZRT',
'25707':'Qusar_1_EATS-Aggregation - Traffic - Gi1/0/1 - AZRT',
'26026':'Qusculuq_EATS-Aggregation - Traffic - port 28',
'974':'Qurtulush_93_EATS-Aggregation - Traffic - 25 - Uplink',
'780':'Saatli_EATS-Aggregation - Traffic - 25 - AZRT',
'1302':'Sabirabad_EATS-Aggregation - Traffic - GI_1 - AZRT',
'808':'Saray_EATS-Aggregation - Traffic - GI_1 - Uplink',
'7014':'Shabran_EATS-Aggregation - Traffic - Gi0/1',
'7040':'Sheki_4_EATS-Aggregation - Traffic - 25 - Uplink',
'25606':'Sheki_5_EATS-Aggregation - Traffic - 25 - Uplink_MHM',
'25722':'Shemkir_EATS-Aggregation - Traffic - Gi0/1 - Azertelekom',
'296':'Shirvan_EATS-Aggregation - Traffic - 26 - AZRT',
'25415':'Siyezen_EATS-Aggregation - Traffic - port 17: Gigabit Copper/17',
'24181':'Sulutepe_EATS-Aggregation - Traffic - port 25',
'25513':'Tovuz_EATS-Aggregation - Traffic - port 25 - Azertelekom_Uplink',
'24685':'Ceyranbatan-SW/QLB-Aggregation - Traffic - Gi1/0/1/ZTE_Switch',
'25374':'Mehdiabad-SW/QLB-Aggregation - Traffic - port 25 - Uplink-342',
'24670':'Novxani_Kend-SW-Aggregation - Traffic - Fa0/24',
'25388':'Qebele-SW/QLB-Aggregation - Traffic - Port 7 - 7',
'25372':'Zengilan_EATS-SW/QLB-Aggregation - Traffic - port 9: Gigabit Copper/9',
'25825':'Sumqayit EATS 642 Cisco-Aggregation - Traffic - Gi1/0/28 - .: Azertelekom Uplink :.'
}

cj = CookieJar()
br = mechanize.Browser()
br.set_proxies({})
br.set_handle_robots(False)
br.set_cookiejar(cj)
br.open("http://172.20.20.24/cacti/index.php")

br.select_form(nr=0)
br.form['login_username'] = 'taleh'
br.form['login_password'] = 'F$$x@$ePt#*'
#br.form['realm'] = ['1',]
br.submit()
for i in graph_id.keys():
    br.retrieve('http://172.20.20.24/cacti/graph_xport.php?local_graph_id='+i+'&rra_id=0&view_type=tree&graph_start='+str(int(start))+'&graph_end='+str(int(end)),
                '/home/ismayil/Documents/fix/'+i+'_'+dt.strftime(yesterday,'%d.%m.%Y')+'.csv')[0]
br.close()
def splitter(string):
    #h=''
    #for i,j in enumerate(list(string)):
    #    if i!=0 and i%2!=0:
    #        h+=j
    h=string
    a=eval(h.split(',')[0])
    b=float(eval(h.split(',')[2]))
    c=float(eval(h.split(',')[4]))
    return a,b,c
all=pd.DataFrame()

for i in graph_id.keys(): #['146','81']:#
    #df=pd.read_fwf('/home/ismayil/Documents/fix/'+graph_id[i]+'_'+dt.strftime(yesterday,'%d.%m.%Y')+'.csv',skiprows=9,sep=',',encoding='UTF8')
    #u=[]
    #for j in range(len(df)):
    #    try:
    #        u.append(splitter(df.iloc[j,0]))
    #    except Exception as e:
    #        print(e,' from splitter')
    #        print(i)
    #        print(df.iloc[j,0])
    #        continue
    #df=pd.DataFrame(u,columns=['Date','Inbound','Outbound'])
    df=pd.read_csv('/home/ismayil/Documents/fix/'+i+'_'+dt.strftime(yesterday,'%d.%m.%Y')+'.csv',skiprows=9) # remove if read_csv doesn`t work
    df=df[['Date','Inbound','Outbound']] # remove if read_csv doesn`t work
    df[['Inbound','Outbound']]/=(1000*1000)
    df.columns=['Date','Inbound_mbps','Outbound_mbps']
#    df2=df.copy()
#    df2.columns=['Date','Inbound '+graph_id[i],'Outbound '+graph_id[i]]
 #   if len(all)>0:
 #       all=all.merge(df2,on='Date')
 #   else:
 #       all=df2
    try:
        df.insert(1,'Site',graph_id[i])
        df['Date']=pd.to_datetime(df['Date'])
        df.to_hdf('/disk2/support_files/archive/core/fix_'+dt.strftime(yesterday,"%Y-%m-%d")+'.h5','fix_cacti_3isp',append=True,
                                format='table', data_columns=['Date','Site'], min_itemsize={'Site':100},complevel=5)
    except Exception as e:
        print(e)
        1
#all.to_csv('/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/Core QA/scheduled_reports/Cacti_report_fix_uninet_citynet_'+dt.strftime(yesterday,'%d.%m.%Y')+'.csv',index=False)

#all.to_csv('/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/Core QA/scheduled_reports/Cacti_report_fix_ultel_'+dt.strftime(yesterday,'%d.%m.%Y')+'.csv',index=False)
os.popen('rm /home/ismayil/Documents/fix/*.csv')
#os.popen('rm /home/ismayil/Documents/*BHQ*.csv')

