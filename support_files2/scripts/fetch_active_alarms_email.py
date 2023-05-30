import imaplib
import base64
import os,shutil
import email

mail = imaplib.IMAP4_SSL('webmail.azerconnect.az')
mail.login('ismayilm','Ismail2018_S')
mail.select('Inbox')
dates=[]
type, data = mail.search(None, 'UnSeen')
for num in data[0].split():
    try:
        typ, data = mail.fetch(num, '(BODY.PEEK[HEADER])')
        raw_email = data[0][1]
    # converts byte literal to string removing b''
        raw_email_string = raw_email.decode('utf-8')
        email_message = email.message_from_string(raw_email_string)
        subject = str(email.header.make_header(email.header.decode_header(email_message['Subject'])))
        #print(subject, ' ddddddddddddddddd')
        if subject=='[Alarm report send] Active alarms':
    # downloading attachments
            typ, data = mail.fetch(num, '(RFC822)' )
            raw_email = data[0][1]
        # converts byte literal to string removing b''
            raw_email_string = raw_email.decode('utf-8')
            email_message = email.message_from_string(raw_email_string)
            for part in email_message.walk():
                # this part comes from the snipped I don't understand yet... 
                if part.get_content_maintype() == 'multipart':
                    continue
                if part.get('Content-Disposition') is None:
                    continue
                fileName = part.get_filename()
                status, d = mail.fetch(num, '(INTERNALDATE)')
                time=d[0].decode().split('"')[1][:17].replace(' ','_').replace(':','_')
                dates.append(time)
                if bool(fileName):
                    filePath = os.path.join('/home/ismayil/flask_dash/data/active_alarms/',time+'_'+fileName)
                    if not os.path.isfile(filePath) :
                        fp = open(filePath, 'wb')
                        fp.write(part.get_payload(decode=True))
                        fp.close()
                    subject = str(email_message).split("Subject: ", 1)[1].split("\nTo:", 1)[0]
                    #print(subject,' subject at the end')
            mail.store(num,'+FLAGS', '\\Deleted')
            mail.expunge()
    except Exception as e:
        print(e,' exception was raised')
        print(num, raw_email)
        continue
mail.logout()
print(dates)
shutil.copyfile('/mnt/raw_counters/Shared Folder/python script/SG_and_PG_export.xlsx','/home/ismayil/flask_dash/data/active_alarms/SG_PG_info.xlsx')

import pandas as pd
import glob
import numpy as np
import datetime


if len(dates)>0:
    for i in dates:
        #print(i)
        try:
            pg=pd.read_excel('/home/ismayil/flask_dash/data/active_alarms/SG_PG_info.xlsx')
            df=pd.read_excel(glob.glob('/home/ismayil/flask_dash/data/active_alarms/*'+i+'*AlarmReport*')[0])
            sitelist=pd.read_csv('/home/ismayil/flask_dash/support_files/site_name_lookup.csv')
            tracker=pd.read_csv('/home/ismayil/flask_dash/support_files/tracker.csv')
            df.rename(columns={'Alarm ID':'alarmid','Alarm Name':'alarmname','Alarm Source':'node','Site ID':'sitecode',
                    'Site Name':'sitename','Location':'location','Last Occurred On':'lastoccurrence','Remark':'remark'},inplace=True)
            df=df.merge(sitelist[['a','name']],left_on='node',right_on='a',how='left')
            df.loc[df['node'].str.contains('|'.join(['WBTS','BCF'])),'node']=df.loc[df['node'].str.contains('|'.join(['WBTS','BCF'])),'name']
            df['node'].fillna('tapilmadi',inplace=True)
            df.loc[df['node']=='tapilmadi','location']='Unkown_sites'
            df['site']=df['node'].apply(lambda x: x[1:8])
            # Set formula of 2G down
            df.loc[(df['alarmname']=="OML Fault"),'2G down']=df.loc[(df['alarmname']=="OML Fault")]['lastoccurrence']
            df.loc[(df['alarmname']=="BTS O&M LINK FAILURE"),'2G down'] =df.loc[(df['alarmname']=="BTS O&M LINK FAILURE")]['lastoccurrence']

            # Set formula of 3G down
            df.loc[(df['alarmname']=="FAILURE IN WCDMA WBTS O&M CONNECTION"),'3G down']=df.loc[(df['alarmname']=="FAILURE IN WCDMA WBTS O&M CONNECTION")]['lastoccurrence']
            df.loc[(df['alarmname']=="NodeB Unavailable"),'3G down'] =df.loc[(df['alarmname']=="NodeB Unavailable")]['lastoccurrence']


            # Set formula of LTE down
            df.loc[df['alarmname']=="S1ap Link Down",'LTE down'] = df.loc[df['alarmname']=="S1ap Link Down"]['lastoccurrence']

            # Set formula of MPF start
            df.loc[df['alarmname'].str.contains('MAINS POWER'),'MPF start'] =df.loc[df['alarmname'].str.contains('MAINS POWER')]['lastoccurrence'] 

            # Set formula of SG start
            df.loc[df['alarmname']=="GENERATOR WORKING",'SG start'] = df.loc[df['alarmname']=="GENERATOR WORKING"]['lastoccurrence']
            df7=df.groupby('sitecode').count()['location'].reset_index()
            df7.drop_duplicates(inplace=True)

            for m in ['2G down','3G down','LTE down','MPF start','SG start']:
            # Pivoted into df6
                if m not in df.columns:
                    df.loc[m,:]=np.nan
                tmp_df = df[[m, 'sitecode']]
                pivot_table = tmp_df.pivot_table(
                    index='sitecode',
                    columns=m,
                    values=m,
                    aggfunc={m: 'count'}
                    )
                #pivot_table.set_axis([flatten_column_header(col) for col in pivot_table.keys()], axis=1, inplace=True)
                pivot_table = pivot_table.reset_index()

            # Unpivoted df6_pivot into df6_pivot_unpivoted
                pivot_table = pivot_table.melt(id_vars='sitecode', value_vars=pivot_table.columns[1:])

            # Filtered value
                pivot_table = pivot_table[pivot_table['value'].notnull()]

            # Renamed columns 2G down
                pivot_table.rename(columns={'variable': m}, inplace=True)

            # Merged df6 and df6_pivot_unpivoted into df9
                df7 = df7.merge(pivot_table, left_on='sitecode', right_on='sitecode', 
                    how='left')
            df7.drop(columns=['value_x','value_y','value'],inplace=True)
            df7.rename(columns={'PG Start Time':'PG start','location_y':'location'},inplace=True)
            df7.drop_duplicates(keep = 'first',inplace = True)
            pg=pg[pg['PG stop Time'].isnull()]

            # find last update time of PG file. If it is more than 30 minute don`t use it
            f=os.path.getmtime('/home/ismayil/flask_dash/data/active_alarms/SG_PG_info.xlsx')
            a=datetime.datetime.now()
            last_update=(a-datetime.datetime.fromtimestamp(f)).total_seconds()/60
            
            #### condition to check last update time of the pg
            if 1:  # before was last_updates<=30
                df7=df7.merge(pg[['Site ID','PG Start Time']],left_on='sitecode',right_on='Site ID',how='outer')
                df7.loc[df7['sitecode'].isnull(),'sitecode']=df7.loc[df7['sitecode'].isnull(),'Site ID']
                df7.rename(columns={'PG Start Time':'PG start'},inplace=True)
                df7.drop(columns='Site ID',inplace=True)
            else:
                df7['PG start']=np.nan

            df7=df7.merge(tracker[['SITE_ID','Economical Region','District/Community']],left_on='sitecode',right_on='SITE_ID',how='left')
            df7['sitecode'].fillna('tapilmadi',inplace=True)
            df7.loc[df7['sitecode']=='tapilmadi','Economical Region']='Unkown_sites'
            df7['Unique down']=np.nan
            df7.loc[(df7['2G down'].notnull()) | (df7['3G down'].notnull()) | (df7['LTE down'].notnull()),'Unique down']=1
            df8=df7.copy()
            df8['time']=glob.glob('/home/ismayil/flask_dash/data/active_alarms/*'+i+'*AlarmReport*')[0][-34:-17]
            
            if os.path.exists('/home/ismayil/flask_dash/data/active_alarms/site_level.csv'):
                df8.to_csv('/home/ismayil/flask_dash/data/active_alarms/site_level.csv',index=False,mode='a',header=False)
            else:
                df8.to_csv('/home/ismayil/flask_dash/data/active_alarms/site_level.csv',index=False,mode='a')
            uu=df7.groupby(['Economical Region','District/Community']).count()[['2G down','3G down','LTE down','Unique down','MPF start','SG start','PG start']].reset_index()
            uu.rename(columns={'Economical Region':'location','District/Community':'admin_region'},inplace=True)
            uu['time']=glob.glob('/home/ismayil/flask_dash/data/active_alarms/*'+i+'*AlarmReport*')[0][-34:-17]
            #print(a) 
            if os.path.exists('/home/ismayil/flask_dash/data/active_alarms/active_alarms.csv'):
                uu.to_csv('/home/ismayil/flask_dash/data/active_alarms/active_alarms.csv',index=False,mode='a',header=False)
            else:
                uu.to_csv('/home/ismayil/flask_dash/data/active_alarms/active_alarms.csv',index=False,mode='a')
        except Exception as e:
            print(e)
            continue
