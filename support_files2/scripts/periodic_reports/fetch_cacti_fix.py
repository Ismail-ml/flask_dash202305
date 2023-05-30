import mechanize
import pandas as pd
from bs4 import BeautifulSoup
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
def ids(link=''):
    try:
        a=br.open(link)
        soup = BeautifulSoup(a, "lxml")

        links = []
        for link in soup.findAll('area'):
            links.append(link.get('href'))

        bb=pd.DataFrame(links)
        bb[0]=bb[0].apply(lambda c: c[c.find('graph_id')+9:])
    except Exception as e:
        print(e)
        print(links)
        return set()
    return set(bb[0])

graph_id=ids('http://81.21.80.170/plugins/weathermap/weathermap-cacti-plugin.php?action=viewmap&id=96601d8089d4fdd3e76e')
graph_id2=ids('http://81.21.80.170/plugins/weathermap/weathermap-cacti-plugin.php?action=viewmap&id=2bd13d385c878c5601c3')

for i in graph_id:
    br.retrieve('http://81.21.80.170/graph_xport.php?local_graph_id='+i+'&rra_id=2&view_type=tree&graph_start='+str(int(start))+'&graph_end='+str(int(end)),
                '/home/ismayil/Documents/fix/'+i+'_'+dt.strftime(yesterday,'%d.%m.%Y')+'.csv')[0]

for i in graph_id2:
    br.retrieve('http://81.21.80.170/graph_xport.php?local_graph_id='+i+'&rra_id=2&view_type=tree&graph_start='+str(int(start))+'&graph_end='+str(int(end)),
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
print(graph_id)
for i in graph_id: #['146','81']:#
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
        site_name=pd.read_csv('/home/ismayil/Documents/fix/'+i+'_'+dt.strftime(yesterday,'%d.%m.%Y')+'.csv',nrows=1).columns[1]
        if ('cpu' in site_name) | ('CPU' in site_name):continue
        df=pd.read_csv('/home/ismayil/Documents/fix/'+i+'_'+dt.strftime(yesterday,'%d.%m.%Y')+'.csv',skiprows=9)
        #site_name=pd.read_csv('/home/ismayil/Documents/fix/'+i+'_'+dt.strftime(yesterday,'%d.%m.%Y')+'.csv',skiprows=1).columns[1] # remove if read_csv doesn`t work
        df=df[['Date','Inbound','Outbound']] # remove if read_csv doesn`t work
        df[['Inbound','Outbound']]/=(1000*1000)
        df.columns=['Date','Inbound_mbps','Outbound_mbps']
        df.insert(1,'Site',site_name)
        df['Date']=pd.to_datetime(df['Date'])
        df.to_hdf('/disk2/support_files/archive/core/fix_'+dt.strftime(yesterday,"%Y-%m-%d")+'.h5','fix_cacti_uni_citynet',append=True,
                                format='table', data_columns=['Date','Site'], min_itemsize={'Site':200},complevel=5)
        print(i,' success')
    except Exception as e:
        print(e, ' from ', i)
        continue
#all.to_csv('/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/Core QA/scheduled_reports/Cacti_report_fix_uninet_citynet_'+dt.strftime(yesterday,'%d.%m.%Y')+'.csv',index=False)

all=pd.DataFrame()
for i in graph_id2: #['146','81']:#
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
    try:
        site_name=pd.read_csv('/home/ismayil/Documents/fix/'+i+'_'+dt.strftime(yesterday,'%d.%m.%Y')+'.csv',nrows=1).columns[1]
        if ('cpu' in site_name) | ('CPU' in site_name): continue
        df=pd.read_csv('/home/ismayil/Documents/fix/'+i+'_'+dt.strftime(yesterday,'%d.%m.%Y')+'.csv',skiprows=9)
        df=df[['Date','Inbound','Outbound']] # remove if read_csv doesn`t work
        df[['Inbound','Outbound']]/=(1000*1000)
        df.columns=['Date','Inbound_mbps','Outbound_mbps']
        df.insert(1,'Site',site_name)
        df['Date']=pd.to_datetime(df['Date'])
        df.to_hdf('/disk2/support_files/archive/core/fix_'+dt.strftime(yesterday,"%Y-%m-%d")+'.h5','fix_cacti_ultel',append=True,
                                format='table', data_columns=['Date','Site'], min_itemsize={'Site':100},complevel=5)
    except Exception as e:
        print(e, ' from ', i)
        continue
#all.to_csv('/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/Core QA/scheduled_reports/Cacti_report_fix_ultel_'+dt.strftime(yesterday,'%d.%m.%Y')+'.csv',index=False)
os.popen('rm /home/ismayil/Documents/fix/*.csv')
#os.popen('rm /home/ismayil/Documents/*BHQ*.csv')
