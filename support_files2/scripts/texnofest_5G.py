import os
import pandas as pd
import glob,time
#import dask.dataframe as dd
import numpy as np
import datetime
import zipfile
def run(path1,tracker):
    print('Nokia 5G aggregation begin...')
    c1=time.time()
    nokia_5G_1={'MAC PDU trans data vol PDSCH':'float64','MAC PDU trans data vol PUSCH':'float64','MAC SDU data vol trans DL DTCH':'float64',
        'MAC SDU data vol rcvd UL DTCH':'float64','NX2CC_BEARER_ADD_ATT (M55112C03001)':'float64','NX2CC_BEARER_ADD_ACK (M55112C03002)':'float64',
        'NX2CC_RAB_REL_ABNORM_MENB (M55112C06003)':'float64','NX2CC_RAB_REL_ABNORM_SGNB (M55112C06006)':'float64','NX2CC_RAB_REL_ATT_MENB (M55112C06001)':'float64',
        'NX2CC_RAB_REL_ATT_SGNB (M55112C06004)':'float64','PDSCH_OFDM_SYMBOLS_TIME (M55308C03005)':'float64','DL Traf for thrp byte':'float64',
        'UL Traf for thrp byte':'float64','PUSCH_OFDM_SYMBOLS_TIME (M55308C03006)':'float64','PEAK_NUMBER_OF_NSA_USERS (M55114C00010)':'float64',
        'MAX_UE_DL_DRB_DATA (M55308C02001)':'float64','MAX_UE_UL_DRB_DATA (M55308C02003)':'float64'}

    an=[]
    nsn=pd.DataFrame()
    #tracker=pd.read_excel(r'\\file-server\AZERCONNECT_LLC_OLD\Corporate Folder\CTO\Technology trackers\RNP\Azerconnect_RNP_tracker.xlsx',skiprows=[0])
    path1='/home/ismayil/flask_dash/data/nokia'
    all_files = glob.glob(path1 + "/*.zip")
    #print('For dirpath:',dirpath,'len is:',len(all_files))
    if len(all_files)>0:
        li = []
        if os.path.isfile(os.path.join('/home/ismayil/flask_dash/data', 'files.csv')):
                existing_files = pd.read_csv(os.path.join('/home/ismayil/flask_dash/data', 'files.csv'), header=None)
        else: existing_files=pd.DataFrame()
        for filename in all_files:
            try:
                print(filename, ' begin')    
                if (filename.replace('/home/ismayil/flask_dash/data/nokia/','5G_') in existing_files.values): continue
                files2=zipfile.ZipFile(filename,'r')
                #if ('01_37_00' in filename): continue
                df=pd.read_csv(files2.open(files2.namelist()[9]),sep=';')
                
                df['rab_sr_num']=df['NX2CC_BEARER_ADD_ACK (M55112C03002)']
                df['rab_sr_den']=df['NX2CC_BEARER_ADD_ATT (M55112C03001)']
                df['dcr_num']=df['NX2CC_RAB_REL_ABNORM_MENB (M55112C06003)']+df['NX2CC_RAB_REL_ABNORM_SGNB (M55112C06006)']
                df['dcr_den']=df['NX2CC_RAB_REL_ATT_MENB (M55112C06001)']+df['NX2CC_RAB_REL_ATT_SGNB (M55112C06004)']
                df['dl_ps_traf']=df['MAC PDU trans data vol PDSCH']*1000000/(1024*1024)
                df['ul_ps_traf']=df['MAC PDU trans data vol PUSCH']*1000000/(1024*1024)
                df['dl_thrp_num']=df['DL Traf for thrp byte']*8
                df['dl_thrp_den']=df['PDSCH_OFDM_SYMBOLS_TIME (M55308C03005)']
                df['ul_thrp_num']=df['UL Traf for thrp byte']*8
                df['ul_thrp_den']=df['PUSCH_OFDM_SYMBOLS_TIME (M55308C03006)']
                df['Max_nsa_user']=df['PEAK_NUMBER_OF_NSA_USERS (M55114C00010)']
                df['Max_dl_drb_user']=df['MAX_UE_DL_DRB_DATA (M55308C02001)']
                df['Max_ul_drb_user']=df['MAX_UE_UL_DRB_DATA (M55308C02003)']

                df=df[['MRBTS name','PERIOD_START_TIME','rab_sr_num',
                        'rab_sr_den','dcr_num','dcr_den','dl_ps_traf','ul_ps_traf','dl_thrp_num','dl_thrp_den',
                        'ul_thrp_num','ul_thrp_den','Max_nsa_user','Max_dl_drb_user','Max_ul_drb_user']]
                li.append(df)
            except Exception as e:
                print(e)
                #print(df.columns)
                continue                
                
            #nsn = dd.concat(li, axis=0, ignore_index=True) # change to previous value
        nsn = pd.concat(li, axis=0, ignore_index=True)

#print('len of nsn:',len(nsn))
    an.append(nsn)
        
    print(time.time()-c1,'append finish')
    #nsn = dd.concat(an, axis=0, ignore_index=True,sort=False) # change to previous value
    nsn = pd.concat(an, axis=0, ignore_index=True, sort=False)
    print(time.time()-c1,'concat finish')
    #nsn=nsn.compute() # change to previous value
    print(time.time()-c1,'compute finish')

    try:
        nsn['Date']=pd.to_datetime(nsn['PERIOD_START_TIME'], format='%m.%d.%Y %H:%M:%S')
        nsn['Region']='Baku'
        nsn['Vendor']='Nokia'
        nsn.rename(columns={'MRBTS name':'Site_name'},inplace=True)

        nsn_agr=nsn[['Date','Vendor','Site_name','Region','rab_sr_num',
                    'rab_sr_den','dcr_num','dcr_den','dl_ps_traf','ul_ps_traf','dl_thrp_num','dl_thrp_den',
                    'ul_thrp_num','ul_thrp_den','Max_nsa_user','Max_dl_drb_user','Max_ul_drb_user']]
        keys=['rab_sr_num','rab_sr_den','dcr_num','dcr_den','dl_ps_traf','ul_ps_traf','dl_thrp_num','dl_thrp_den',
                    'ul_thrp_num','ul_thrp_den','Max_nsa_user','Max_dl_drb_user','Max_ul_drb_user']
        agg_dict=dict.fromkeys(keys, 'sum')
        nsn_agr.iloc[:,4:]=nsn_agr.iloc[:,4:].astype(np.float64)
        nsn_agr.drop_duplicates(keep='first',inplace=True)
        print(time.time()-c1,'new table created')
        nsn_rnc=nsn_agr.groupby(['Date','Vendor','Region']).agg(agg_dict)
        nsn_rnc.reset_index(inplace=True)
        print(time.time()-c1,'time to save')

        if os.path.exists('/disk2/support_files/archive/fiveG_bsc.csv'):
            existing=pd.read_csv('/disk2/support_files/archive/fiveG_bsc.csv')

        if os.path.exists('/disk2/support_files/archive/fiveG.csv'):
            nsn_agr=nsn_agr[~nsn_agr['Date'].isin(existing['Date'].unique())]
            nsn_agr.to_csv('/disk2/support_files/archive/fiveG.csv',header=False,index=False,mode='a')
        else:
            nsn_agr.to_csv('/disk2/support_files/archive/fiveG.csv',index=False,mode='a')
        
        if os.path.exists('/disk2/support_files/archive/fiveG_bsc.csv'):
            nsn_rnc=nsn_rnc[~nsn_rnc['Date'].isin(existing['Date'].unique())]
            nsn_rnc.to_csv('/disk2/support_files/archive/fiveG_bsc.csv',header=False,index=False,mode='a')
        else:
            nsn_rnc.to_csv('/disk2/support_files/archive/fiveG_bsc.csv',index=False,mode='a')
        
        pd.DataFrame(glob.glob(path1 + '/*.zip')).replace({'/home/ismayil/flask_dash/data/nokia/': '5G_'}, regex=True).\
            to_csv(os.path.join('/home/ismayil/flask_dash/data', 'files.csv'), mode='a', header=None, index=False)
        
    except Exception as e:
        print(e)
        print(nsn.columns)
        1
