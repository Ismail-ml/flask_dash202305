import smtplib
from email import encoders
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from pretty_html_table import build_table
import pandas as pd
def send_mail(receipents,subject,body,table,time,attachment=False):    
    strFrom = '10.240.104.155@azerconnect.az'
    strTo = receipents 
    msgRoot = MIMEMultipart('related')
    msgRoot['Subject'] = subject
    msgRoot['From'] = 'AZRC_QA&R <AZRC_QA&R@azerconnect.az>'
    msgRoot['To'] = str(receipents).replace('[','').replace(']','').replace("'",'')
    html_table_grey_light = build_table(table,'blue_light',font_size='13px')
    body='<b>'+body+'</b> <p>'+time+'</p><p>'+html_table_grey_light#+'</p'
    msgText = MIMEText(body, 'html')
    msgRoot.attach(msgText)
    if attachment:
        attach_file_name = '/home/ismayil/flask_dash/support_files/down_info.csv'
        attach_file = open(attach_file_name, 'rb')
        payload = MIMEBase('application', 'octet-stream')
        payload.set_payload((attach_file).read())
        #encoders.encode_base64(payload) #encode the attachment
        #add payload header with filename
        payload.add_header('Content-Disposition', 'attachment', filename='Neighbor_alarm_info.csv')
        msgRoot.attach(payload)
    smtp = smtplib.SMTP(host='webmail.azerconnect.az',port=25)
    smtp.sendmail(strFrom,strTo,msgRoot.as_string())
    smtp.quit()
