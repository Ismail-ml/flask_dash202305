import imaplib
import base64
import os,shutil
import email

mail = imaplib.IMAP4_SSL('webmail.azerconnect.az')
mail.login('ismayilm','Ismail2018_S')
mail.select('Inbox/ODS')
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
        if any([i in subject for i in ['All_Protocols','ODS']]):
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
                if '=?utf-8?q?BKC_traf?= =?utf-8?q?fic_calc?=' in fileName:
                    fileName='BKC traffic calculation.xlsx'
                elif '=?utf-8?q?AZF_traf?= =?utf-8?q?fic_calc?=' in fileName:
                    fileName='AZF traffic calculation.xlsx'
                status, d = mail.fetch(num, '(INTERNALDATE)')
                time=d[0].decode().split('"')[1][:17].replace(' ','_').replace(':','_')
                dates.append(time)
                if bool(fileName):
                    filePath = os.path.join('/home/ismayil/flask_dash/data/ods_dtms/',time+'_'+fileName)
                    if not os.path.isfile(filePath) :
                        fp = open(filePath, 'wb')
                        fp.write(part.get_payload(decode=True))
                        fp.close()
                    subject = str(email_message).split("Subject: ", 1)[1].split("\nTo:", 1)[0]
                    #print(subject,' subject at the end')
            #mail.store(num,'+FLAGS', '\\Deleted')
            #mail.expunge()
    except Exception as e:
        print(e,' exception was raised')
        print(num, raw_email)
        continue
mail.logout()
print(dates)
#shutil.copyfile('/mnt/raw_counters/Shared Folder/python script/SG_and_PG_export.xlsx','/home/ismayil/flask_dash/data/active_alarms/SG_PG_info.xlsx')

import pandas as pd
import glob
import numpy as np
import datetime

os.chdir('/home/ismayil/flask_dash/data/ods_dtms')

if len(dates)>0:
    for i in dates:
        try:
            if 'AZF traffic' in i:
                df=pd.read_csv(i)
                df.columns=df.columns.str.upper()
                df['OTIME']=pd.to_datetime(df['OTIME'],format='%d-%b-%y')
                df.insert(2,'MNO','AZF')
            elif 'BKC traffic' in i:
                df=pd.read_excel(i)
                df['OTIME']=pd.to_datetime(df['OTIME'],format='%Y%m%d')
                df.insert(2,'MNO','BKC')
            if 'all_protocols' not in i:
                df.rename(columns={'OTIME':'Date','HOUR':'Hour'},inplace=True)
                file_name2 = datetime.datetime.strftime(datetime.datetime.utcfromtimestamp(df['Date'][0].astype(datetime.datetime) / 1e9),"%Y-%m-%d")
                df.to_hdf('/disk2/support_files/archive/core/core_new_'+file_name2+'.h5','ods',append=True,
                        format='table', data_columns=['Date', 'Hour', 'MNO'], complevel=5)
            elif 'all_protocols' in i:
                df=pd.read_csv(i,skiprows=3)
                df=df.melt(id_vars='Protocol',value_vars=df.columns[1:],value_name='Traffic,MB',var_name='Date').query('Protocol!="Units measured in bytes."')
                if 'bkc' in i: mno='BKC'
                elif 'azf' in i: mno='AZF'
                df.insert(1,'MNO',mno)
                df['Date']=pd.to_datetime(df['Date'],format='%Y-%m-%d %H:%M')
                df[['Protocol','MNO']]=df[['Protocol','MNO']].astype(str)
                df['Traffic,MB']/=(1024*1024)
                file_name2 = datetime.datetime.strftime(datetime.datetime.utcfromtimestamp(df['Date'][0].astype(datetime.datetime) / 1e9),"%Y-%m-%d")
                df[['Date','MNO','Protocol','Traffic,MB']].to_hdf('/disk2/support_files/archive/core/core_new_'+file_name2+'.h5','dtms_traf',append=True,
                        format='table', data_columns=['Date', 'MNO', 'Protocol'], complevel=5,min_itemsize={'Protocol':100})
        except Exception as e:
            dates.remove(i)
            print(e)
            continue
    if len(dates)>0:[os.remove(i) for i in dates]
