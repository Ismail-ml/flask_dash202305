import pandas as pd
import subprocess
import json
tracker=pd.read_csv('/home/ismayil/flask_dash/support_files/tracker.csv')
h=[]
for j,i in enumerate(tracker['SITE_ID']):
    lat=tracker.loc[tracker['SITE_ID']==i,'Lat'].values[0]
    lon=tracker.loc[tracker['SITE_ID']==i,'Long'].values[0]
    reg=tracker.loc[tracker['SITE_ID']==i,'Economical Region'].values[0]
    params='windspeed_10m_max,winddirection_10m_dominant,windgusts_10m_max,temperature_2m_min,temperature_2m_max,precipitation_sum,et0_fao_evapotranspiration'
    text="https://archive-api.open-meteo.com/v1/era5?latitude="+str(lat)+"&longitude="+str(lon)+"&start_date=2022-11-16&end_date=2023-01-12&daily="+params+"&timezone=Europe/Moscow"
    print(text)
    with open('/home/ismayil/weather2.txt','w') as f:
        process=subprocess.Popen(['curl', text],stdout=f)
        process.communicate()
    with open('/home/ismayil/weather2.txt','r') as f:
        a=f.readlines()
    try:
        df=pd.DataFrame(json.loads(a[0])['daily'])
        df['site']=i
        df['region']=reg
        h.append(df)
        if (j+1)%100==0:
            print(i,' done ',j)
    except:
        print(i,' not done')

pd.concat(h).to_csv('hazir_weather.csv')
