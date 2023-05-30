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
start= int(dt.timestamp(dt.strptime(dt.strftime(yesterday2,'%d.%m.%Y')+' 23:55','%d.%m.%Y %H:%M')))
end= int(dt.timestamp(dt.strptime(dt.strftime(datetime.date.today(),'%d.%m.%Y')+' 08:55','%d.%m.%Y %H:%M')))
graph_id={'150':'BKC BTC ISP','74':'BKC BHQ ISP','151':'AZF BTC ISP','75':'AZF BHQ ISP','146':'AZRC BTC','81':'AZRC BHQ'}

cj = CookieJar()
br = mechanize.Browser()
br.set_proxies({})
br.set_handle_robots(False)
br.set_cookiejar(cj)
br.open("https://10.64.38.82/cacti/graph_view.php")

br.select_form(nr=0)
br.form['login_username'] = 'cn_reporting'
br.form['login_password'] = 'Qa123!@#'
br.submit()
for i in graph_id.keys():
    br.retrieve('https://10.64.38.82/cacti/graph_xport.php?local_graph_id='+i+'&rra_id=0&view_type=tree&graph_start='+str(int(start))+'&graph_end='+str(int(end)),
                '/home/ismayil/Documents/'+graph_id[i]+'_'+dt.strftime(yesterday,'%d.%m.%Y')+'.csv')[0]
br.close()
def splitter(string):
    h=''
    for i,j in enumerate(list(string)):
        if i!=0 and i%2!=0:
            h+=j
    a=eval(h.split(',')[0])
    b=float(eval(h.split(',')[1]))
    c=float(eval(h.split(',')[2]))
    return a,b,c
all=pd.DataFrame()

for i in graph_id.keys(): #['146','81']:#
    df=pd.read_fwf('/home/ismayil/Documents/'+graph_id[i]+'_'+dt.strftime(yesterday,'%d.%m.%Y')+'.csv',skiprows=9,sep=',',encoding='UTF8')
    u=[]
    for j in range(len(df)):
        try:
            u.append(splitter(df.iloc[j,0]))
        except Exception as e:
            print(e)
            continue
    df=pd.DataFrame(u,columns=['Date','DL','UL'])
    df2=df.copy()
    df2.columns=['Date','DL '+graph_id[i],'UL '+graph_id[i]]
    if len(all)>0:
        all=all.merge(df2,on='Date')
    else:
        all=df2
    try:
        df.insert(1,'Site',graph_id[i])
        df['Date']=pd.to_datetime(df['Date'])
        for j in df['Date'].unique():
            try:
                file_name = datetime.datetime.strftime(datetime.datetime.utcfromtimestamp(j.astype(datetime.datetime) / 1e9),"%B_%Y")
                file_name2 = datetime.datetime.strftime(datetime.datetime.utcfromtimestamp(j.astype(datetime.datetime) / 1e9),"%Y-%m-%d")        
                a=pd.read_hdf('/disk2/support_files/archive/core/core_new_'+file_name2 +'.h5', 'mobile_cacti', where='Date=j and Site==graph_id[i]')
                if len(a)>0:
                    print(j,'not appended')
                    continue
                else: df.loc[df['Date']==j].to_hdf('/disk2/support_files/archive/core/core_new_'+file_name2+'.h5','mobile_cacti',append=True,
                                format='table', data_columns=['Date','Site'], min_itemsize={'Site':100},complevel=5)
            except:
                df.loc[df['Date']==j].to_hdf('/disk2/support_files/archive/core/core_new_'+file_name2+'.h5','mobile_cacti',append=True,
                                format='table', data_columns=['Date','Site'], min_itemsize={'Site':100},complevel=5)
    except Exception as e:
        print(e)
        1
all.to_csv('/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/Core QA/scheduled_reports/Cacti_report_'+dt.strftime(yesterday,'%d.%m.%Y')+'.csv',index=False)
os.popen('rm /home/ismayil/Documents/*BTC*.csv')
os.popen('rm /home/ismayil/Documents/*BHQ*.csv')

import fetch_cacti_fix
import fetch_cacti_fix_3isp
