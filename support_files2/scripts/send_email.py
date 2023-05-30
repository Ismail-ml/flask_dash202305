import smtplib
from email import encoders
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
try:
        strFrom = '10.240.104.155@azerconnect.az'
        strTo = ['talehm@azerconnect.az','vugar.mahmudov@ultel.az','rizvans@azerconnect.az','ilgarn@azerconnect.az','ismayilm@azerconnect.az',\
          'iefendiyev@ultel.az','ruslanm@ultel.az','kananh@azerconnect.az','fidanb@azerconnect.az'] #'rashadha@azerconnect.az','rizvans@azerconnect.az',
        msgRoot = MIMEMultipart('related')
#recipients = ['talehm@azerconnect.az','vugar.mahmudov@ultel.az','ismayilm@azerconnect.az']
        msgRoot['Subject'] = 'Daily HP installation'
        msgRoot['From'] = 'AZRC_QA&R <AZRC_QA&R@azerconnect.az>'#ismayilm@azerconnect.az
        msgRoot['To'] ='talehm@azerconnect.az,vugar.mahmudov@ultel.az,rizvans@azerconnect.az,ilgarn@azerconnect.az,ismayilm@azerconnect.az,\
                iefendiyev@ultel.az,ruslanm@ultel.az,kananh@azerconnect.az,fidanb@azerconnect.az'#rashadha@azerconnect.az,rizvans@azerconnect.az,
#msgRoot['To']= ", ".join(recipients)
        #msgRoot['To'] = 'm.ismail.ie@gmail.com'
        msgRoot.preamble = 'Multi-part message in MIME format.'
        msgAlternative = MIMEMultipart('alternative')
        msgRoot.attach(msgAlternative)
        msgText = MIMEText('Alternative plain text message.')
        msgAlternative.attach(msgText)
#msgText = MIMEText('<b>Some <i>HTML</i> text</b> and an image.<br><img src="cid:image1"><br>KPI-DATA!', 'html')
        msgText = MIMEText('<b>Status of the HP installation for the last day:</b> <br><img src="cid:image1"><br>', 'html')
        msgAlternative.attach(msgText)
        fp = open('/home/ismayil/nese_yoxlamaq.jpeg', 'rb') #Read image
        msgImage = MIMEImage(fp.read())
        fp.close()
        msgImage.add_header('Content-ID', '<image1>')
        msgRoot.attach(msgImage)
        import smtplib
        smtp = smtplib.SMTP(host='webmail.azerconnect.az',port=25)
        #smtp.set_debuglevel(1)
        #smtp.starttls()
        #smtp.login('ismayilm@azerconnect.az', 'Ismail2018@S') #Username and Password of Account
        smtp.sendmail(strFrom,strTo,msgRoot.as_string())
        smtp.quit()
except Exception as e:
        print(e)
