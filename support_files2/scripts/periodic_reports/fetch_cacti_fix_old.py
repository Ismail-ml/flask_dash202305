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
graph_id={'6879':'Azrt_3_ISP_Intranet - SW_6807_BTC',
'5539':'SW_6807_BTC - Jun_64_PPPOE',
'5362':'SW_6807_BTC - SW_9606_BTC',
'5364':'JUN_64_PPPOE - SW_9606_BTC',
'6889':'JUN_64_PPPOE - NCS-BTC',
'5992':'Ultel_intranet - NCS-BTC',
'6260':'JUN_61 - NCS-61',
'6259':'SW_ATS_61 - NCS-61',
'6302':'JUN_68 - NCS-68',
'6934':'SW_ATS_68 - NCS-68',
'6292':'NCS-68 - NCS-30',
'6258':'NCS-68 - NCS-61',
'6257':'NCS-61 - NCS-BTC',
'5976':'NCS-BTC - NCS-66',
'5946':'NCS-BTC - NCS-40',
'5987':'NCS-BTC - NCS-93',
'5991':'NCS-BTC - ASR_BGP_BTC',
'10207':'NCS-BTC - Allot',
'5980':'NCS-BTC - SW_9606_BTC',
'5361':'SW_9606_BTC - Allot',
'10236':'SW_9606_BTC - JUN_93_test',
'5360':'SW_9606_BTC - ASR_BGP_BTC',
'1528':'ASR_BGP_BTC - Ultel_BGP',
'1430':'ASR_BGP_BTC - Azertelecom',
'7575':'Azertelecom - ASR_BGP_93',
'10202':'ASR_BGP_93 - NCS-93',
'6904':'SW_ATS_93 - NCS-93',
'10265':'NCS-93 - JUN_204_IPOE',
'6903':'NCS-93 - JUN_93_PPPOE',
'18358':'NCS-93 - NCS-40',
'6041':'NCS-93 - NCS-71',
'6026':'NCS-93 - NCS-38',
'6990':'NCS-38 - JUN_38',
'6034':'NCS-38 - SW_ATS_38',
'8651':'NCS_38 - NCS-32',
'6914':'NCS-32 - JUN_32',
'6913':'NCS-32 - SW_ATS_32',
'6931':'NCS-32 - NCS-30',
'6934':'NCS-30 - SW_ATS_30',
'6935':'NCS-30 - JUN_30',
'6372':'NCS-66 - JUN66',
'6371':'NCS-66 - SW_ATS_66',
'6954':'NCS-66 - NCS-23',
'6959':'NCS-23 - JUN_23',
'6957':'NCS-23 - SW_ATS_23',
'2181':'SW_ATS_23 - SW_ATS_28',
'6942':'NCS-23 - NCS-74',
'6945':'NCS-74 - SW_ATS_74',
'14526':'SW_ATS_74 - SW_ATS_77',
'6946':'NCS-74 - JUN_74',
'6040':'NCS-74 - NCS-71',
'6048':'NCS-71 - JUN_71',
'6967':'NCS-71 - SW_ATS_71',
'5954':'NCS-40 - JUN_40',
'5960':'NCS-40 - SW_ATS_40'
}
graph_id2={'12124':'Bras-MX-465 - SAS-Sx-465',
'326':'SAS-Sx-465 - Gate-MX-240-465',
'324':'CityNet - SAS-Sx-465',
'323':'CityNet Local - SAS-Sx-465',
'6876':'CityNet Local NCS - SAS-Sx-465',
'325':'AzerTK-IntX - SAS-Sx-465',
'2609':'BB TV - SAS-Sx-465',
'2447':'Core-LL - Gate-SR7',
'867':'SAS-M-NGN - Gate-SR7',
'305':'Gate-SR7 - SAS-Sx-490',
'866':'SAS-M-NGN - SAS-M-491',
'11986':'SAS-M-424 - SAS-M-465',
'11987':'SAS-M-ET - SAS-M-424',
'12114':'SAS-M-YH - SAS-M-ET',
'233':'SAS-Sx-490 - SAS-M-YH',
'12257':'SAS-Sx-490 - Bras-MX-490 (OU-MX-490)',
'4097':'SAS-Sx-490 - Core-LL-ASR',
'918':'SAS-M-SB - SAS-M-436',
'765':'SAS-M-493 - SAS-M-SB',
'764':'SAS-M-493 - SAS-M-STAT',
'850':'SAS-M-491 - SAS-M-STAT',
'738':'SAS-M-491 - SAS-Sx-490',
'302':'SAS-Sx-465 - SAS-Sx-490',
'365':'SAS-M-436 - SAS-Sx-465',
'11540':'SAS-Sx-465 - SAS-Sx-SB',
'11542':'SAS-Sx-SB - SAS-Sx-ALL',
'12636':'SAS-Sx-SB - SAS-M-Lepeler',
'12845':'SAS-Sx-SB - SAS-M-Halal',
'12844':'SAS-Sx-ALL - SAS-M-Halal',
'12637':'SAS-M-Lepeler - SAS-M-Rahat',
'12605':'SAS-M-Rahat - SAS-M-Inshaatcilar',
'12849':'SAS-M-Inshaatcilar - SAS-Sx-ALL',
'12048':'SAS-Sx-ALL - SAS-Sx-ET'}
graph_id3={}

cj = CookieJar()
br = mechanize.Browser()
br.set_proxies({})
br.set_handle_robots(False)
br.set_cookiejar(cj)
br.open("http://81.21.80.170/index.php")

br.select_form(nr=0)
br.form['login_username'] = 'taleh'
br.form['login_password'] = 'Passw0rd'
br.form['realm'] = ['1',]
br.submit()
for i in graph_id.keys():
    br.retrieve('http://81.21.80.170/graph_xport.php?local_graph_id='+i+'&rra_id=2&view_type=tree&graph_start='+str(int(start))+'&graph_end='+str(int(end)),
                '/home/ismayil/Documents/fix/'+graph_id[i]+'_'+dt.strftime(yesterday,'%d.%m.%Y')+'.csv')[0]
for i in graph_id2.keys():
    br.retrieve('http://81.21.80.170/graph_xport.php?local_graph_id='+i+'&rra_id=2&view_type=tree&graph_start='+str(int(start))+'&graph_end='+str(int(end)),
                '/home/ismayil/Documents/fix/'+graph_id2[i]+'_'+dt.strftime(yesterday,'%d.%m.%Y')+'.csv')[0]
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
    try:
        df=pd.read_csv('/home/ismayil/Documents/fix/'+graph_id[i]+'_'+dt.strftime(yesterday,'%d.%m.%Y')+'.csv',skiprows=9) # remove if read_csv doesn`t work
    except Exception as e:
        print(e, ' from ', graph_id[i])
        continue
    df=df[['Date','Inbound','Outbound']] # remove if read_csv doesn`t work
    df[['Inbound','Outbound']]/=(1024*1024)
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
        df.to_hdf('/disk2/support_files/archive/core/fix_'+dt.strftime(yesterday,"%Y-%m-%d")+'.h5','fix_cacti_uni_citynet',append=True,
                                format='table', data_columns=['Date','Site'], min_itemsize={'Site':100},complevel=5)
    except Exception as e:
        print(e)
        1
#all.to_csv('/mnt/raw_counters/Corporate Folder/CTO/Technology Governance and Central Support/Core QA/scheduled_reports/Cacti_report_fix_uninet_citynet_'+dt.strftime(yesterday,'%d.%m.%Y')+'.csv',index=False)

all=pd.DataFrame()
for i in graph_id2.keys(): #['146','81']:#
    #df=pd.read_fwf('/home/ismayil/Documents/fix/'+graph_id2[i]+'_'+dt.strftime(yesterday,'%d.%m.%Y')+'.csv',skiprows=9,sep=',',encoding='UTF8')
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
    df=pd.read_csv('/home/ismayil/Documents/fix/'+graph_id2[i]+'_'+dt.strftime(yesterday,'%d.%m.%Y')+'.csv',skiprows=9) # remove if read_csv doesn`t work
    df=df[['Date','Inbound','Outbound']] # remove if read_csv doesn`t work
    df[['Inbound','Outbound']]/=(1024*1024)
    df.columns=['Date','Inbound_mbps','Outbound_mbps']
  #  df2=df.copy()
  #  df2.columns=['Date','Inbound '+graph_id2[i],'Outbound '+graph_id2[i]]
  #  if len(all)>0:
  #      all=all.merge(df2,on='Date')
  #  else:
  #      all=df2
    try:
        df.insert(1,'Site',graph_id2[i])
        df['Date']=pd.to_datetime(df['Date'])
        df.to_hdf('/disk2/support_files/archive/core/fix_'+dt.strftime(yesterday,"%Y-%m-%d")+'.h5','fix_cacti_ultel',append=True,
                                format='table', data_columns=['Date','Site'], min_itemsize={'Site':100},complevel=5)
    except Exception as e:
        print(e)
        print(i)
        1
#all.to_csv('/mnt/raw_counters/Corporate Folder/CTO/Technology Governance and Central Support/Core QA/scheduled_reports/Cacti_report_fix_ultel_'+dt.strftime(yesterday,'%d.%m.%Y')+'.csv',index=False)
os.popen('rm /home/ismayil/Documents/fix/*.csv')
#os.popen('rm /home/ismayil/Documents/*BHQ*.csv')

