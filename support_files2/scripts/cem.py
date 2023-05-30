import pandas as pd
import datetime

def run():
    df=pd.read_csv('/home/ismayil/flask_dash/data/cem/VoLTE_CST.csv')
    df['Date']=pd.to_datetime(df['hourly'],format='%Y/%m/%d %H')
    df=df[['Date','V2V_MO_Connection_Delay']]

    for j in df['Date'].unique():
        #file_name = datetime.datetime.strftime(datetime.datetime.utcfromtimestamp(j.astype(datetime.datetime) / 1e9),"%B_%Y")
        file_name2 = datetime.datetime.strftime(datetime.datetime.utcfromtimestamp(j.astype(datetime.datetime) / 1e9),"%Y-%m-%d")
        try:
            a=pd.read_hdf('/disk2/support_files/archive/core/core_new_'+file_name2 +'.h5', 'cem_cst', where='Date=j')
            if len(a)>0:
                print(j,'not appended')
                continue
            else: df.loc[df['Date']==j].to_hdf('/disk2/support_files/archive/core/core_new_'+file_name2+'.h5','cem_cst',append=True,
                            format='table', data_columns=['Date', 'V2V_MO_Connection_Delay'], complevel=5,
                            min_itemsize={'V2V_MO_Connection_Delay': 100})
        except:
            df.loc[df['Date']==j].to_hdf('/disk2/support_files/archive/core/core_new_'+file_name2+'.h5','cem_cst',append=True,
                            format='table', data_columns=['Date', 'V2V_MO_Connection_Delay'], complevel=5,
                            min_itemsize={'V2V_MO_Connection_Delay': 100})

    print("CEM part finish")
