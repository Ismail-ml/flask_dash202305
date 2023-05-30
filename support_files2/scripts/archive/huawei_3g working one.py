import pandas as pd
import glob, time, os, re
import dask.dataframe as dd
import concurrent.futures as cf
import numpy as np
import datetime

def run(path1,path2,path3,tracker):
    print('Huawei 3G aggregation begin...')
    c1 = time.time()

    hw=pd.DataFrame()
    def proccess_excel(diddd):
        #existing_files = pd.read_csv(os.path.join(path1, 'files.txt'), sep=" ", header=None)
        all_files=glob.glob(diddd + "/*.csv")
        for filename in all_files:
            #if os.path.basename(filename) in existing_files: continue
            df = pd.read_csv(filename,skiprows=[1])
            df.rename(columns={'73403774':'cs_inter_freq_ho_num','73403770':'cs_inter_freq_ho_den',
                                   '73403775':'ps_inter_freq_ho_num','73403771':'ps_inter_freq_ho_den',
                                   '73393850':'ps_sho_ho_num','73393849':'ps_sho_ho_den','67189755':'cs_inter_rat_ho_num',
                                   '67189754':'cs_inter_rat_ho_den','67190705':'hsdpa_sr_num','67190704':'hsdpa_sr_den',
                                   '67192115':'hsupa_sr_num','67192114':'hsupa_sr_den','67179860':'voice_sr_num2',
                                   '67179858':'voice_sr_den2','73403808':'r99_sr_num','73403809':'r99_sr_den'},inplace=True)
            df['cs_sho_ho_num']=df['67180520']+df['67180522']
            df['cs_sho_ho_den']=df['67180519']+df['67180521']
            df['voice_sr_num1']=df['67179457']+df['67179462']+df['67179466']
            df['voice_sr_den1']=df['67179329']+df['67179334']+df['67179338']-df['73423388']-df['73423392']-df['73423397']
            df['voice_dr_num']=df['67180082']+df['67192597']
            df['voice_dr_den']=df['67180082']+df['67190518']+df['67192597']+df['67192600']
            df['cs_traf']=df['67199620']+df['67199556']
            df['ps_ac_sr_num1']=df['67179460']+df['67179459']+df['67179465']+df['67179464']
            df['ps_ac_sr_den1']=df['67179332']+df['67179331']+df['67179337']+df['67179336']
            df['ps_ac_sr_num2']=df['67179925']+df['67179926']+df['67179927']+df['67179928']
            df['ps_ac_sr_den2']=df['67179921']+df['67179922']+df['67179923']+df['67179924']
            df['hsdpa_dr_num']=df['67191162']
            df['hsdpa_dr_den']=df['67191162']+df['67191164']+df['67191158']+df['67191160']+df['67189832']+df['67189833']
            df['hsupa_dr_num']=df['67192364']
            df['hsupa_dr_den']=df['67192364']+df['67192365']+df['67192474']+df['67192472']+df['67192470']+df['67192481']
            df['hspa_traf']=(df['67192486']+df['67189840'])/1024/1024
            df=df[['Object Name','Result Time','cs_inter_freq_ho_num',
                'cs_inter_freq_ho_den','ps_inter_freq_ho_num','ps_inter_freq_ho_den','ps_sho_ho_num','ps_sho_ho_den',
                'cs_inter_rat_ho_num','cs_inter_rat_ho_den','hsdpa_sr_num',
                'hsdpa_sr_den','hsupa_sr_num','hsupa_sr_den','voice_sr_num2','voice_sr_den2','r99_sr_num','r99_sr_den',
                          'hsdpa_dr_num','hsupa_dr_num','cs_sho_ho_num','cs_sho_ho_den','voice_sr_num1',
                       'voice_sr_den1','voice_dr_num','voice_dr_den','cs_traf','ps_ac_sr_num1','ps_ac_sr_den1','ps_ac_sr_num2',
                       'ps_ac_sr_den2','hsdpa_dr_den','hsupa_dr_den','hspa_traf','73403806','73403807']]
            df['Date']=pd.to_datetime(df['Result Time'], format='%Y-%m-%d %H:%M')
            df['RNC_name']=df['Object Name'].apply(lambda x: x[:6])
            df['Cell_name']=df['Object Name'].apply(lambda x: re.search(r'\w{4}\d{4}\w',x).group())
            df['Site_name']=df['Cell_name'].apply(lambda x: x[:8])
            li.append(df)
    h=[]
    [h.append(r) for r, d, folder in os.walk(path1)]
    li = [] 

    with cf.ThreadPoolExecutor() as executor:
        executor.map(proccess_excel,h)
        
    hw = pd.concat(li, axis=0, ignore_index=True,sort=False)
    print(time.time()-c1,'part1 finish')


    hw2=pd.DataFrame()
    def proccess_excel2(diddd):
        #existing_files = pd.read_csv(os.path.join(path2, 'files.txt'), sep=" ", header=None)
        all_files=glob.glob(diddd + "/*.csv")
        for filename in all_files:
            #if os.path.basename(filename) in existing_files: continue
            df = pd.read_csv(filename,skiprows=[1])
            try:
                df['cell_avail_blck_num']=df['67199736']+df['73428521']
            except:
                df['cell_avail_blck_num']=0
            df['cell_avail_den']=3600
            df['cell_avail_num']=df['cell_avail_den']-df['67204837']-df['cell_avail_blck_num']
            df['cell_avail_blck_den']=0
            df['r99_dl_traf']=(df['67183993']+df['67183995']+df['67183996']+df['67183997']+df['67183998']+df['67183999']+df['67184000']+df['67184001']+df['67184002']+df['67184003']+df['67184004']+df['67184005']+df['67184006']+df['67184007']+df['67184008']+df['67193548']+df['67193550']+df['67184009']+df['67184010']+df['67184011']+df['67184012']+df['67193552']+df['67193554']+df['73393908'])/(8*1024*1024)
            df['r99_ul_traf']=(df['67184014']+df['67184015']+df['67184016']+df['67184017']+df['67184018']+df['67184019']+df['67184020']+df['67184021']+df['67184022']+df['67184023']+df['67184024']+df['67184025']+df['67184026']+df['67184027']+df['67184028']+df['67184029']+df['67193556']+df['67184030']+df['67184031']+df['67184032']+df['67193558']+df['73393910'])/(8*1024*1024)
            df['r99_dr_num']=df['73421883']+df['73422169']+df['73421886']
            df['r99_dr_den']=df['73421883']+df['73421882']-df['67189831']-df['67189830']-df['73422165']-df['67192584']-df['73424897']
            df=df[['Object Name','Result Time','cell_avail_num',
                'cell_avail_den','cell_avail_blck_num','cell_avail_blck_den','r99_dl_traf','r99_ul_traf','r99_dr_num',
                'r99_dr_den','73422167','73422161','73422168','73422163']]
            df['Date']=pd.to_datetime(df['Result Time'], format='%Y-%m-%d %H:%M')
            df['RNC_name']=df['Object Name'].apply(lambda x: x[:6])
            df['Cell_name']=df['Object Name'].apply(lambda x: re.search(r'\w{4}\d{4}\w',x).group())
            #df['Site_name']=df['Cell_name'].apply(lambda x: x[:8])
            li.append(df)
    h=[]
    [h.append(r) for r, d, folder in os.walk(path2)]
    li = [] 

    with cf.ThreadPoolExecutor() as executor:
        executor.map(proccess_excel2,h)
        
    hw2 = pd.concat(li, axis=0, ignore_index=True,sort=False)
    print(time.time()-c1,'part2 finish')


    hw3=pd.DataFrame()
    def proccess_excel3(diddd):
        #existing_files = pd.read_csv(os.path.join(path3, 'files.txt'), sep=" ", header=None)
        all_files=glob.glob(diddd + "/*.csv")
        for filename in all_files:
            #if os.path.basename(filename) in existing_files: continue
            df = pd.read_csv(filename,skiprows=[1])
            df['hsdpa_thrp_num']=df['50341668']*60*60
            df['hsdpa_thrp_den']=df['50331724']*2
            df=df[['Object Name','Result Time','hsdpa_thrp_num',
                'hsdpa_thrp_den']]
            df['Date']=pd.to_datetime(df['Result Time'], format='%Y-%m-%d %H:%M')
            df['Site_name']=df['Object Name'].apply(lambda x: x[:8])
            df['Local_CI']=df['Object Name'].apply(lambda x: (re.search(r'ID=\d*',x).group()).replace('ID=',''))
            df['lookup']=df['Site_name']+'_'+df['Local_CI']
            li.append(df)
    h=[]
    [h.append(r) for r, d, folder in os.walk(path3)]
    li = [] 

    with cf.ThreadPoolExecutor() as executor:
        executor.map(proccess_excel3,h)
        
    hw3 = pd.concat(li, axis=0, ignore_index=True,sort=False)
    print(time.time()-c1,'part3 finish')


    dump=pd.read_csv(r'/home/ismayil/flask_dash/support_files/Local_Cell_ID.csv')
    dump['LOCELL']=dump['LOCELL'].astype(str)
    dump['lookup']=dump['NODEBNAME']+'_'+dump['LOCELL']
    dump['lookup'][dump['lookup'].duplicated()]
    hw3=pd.merge(hw3,dump[['lookup','CELLNAME','BSCName']],on='lookup',how='left')
    hw3.rename(columns={'CELLNAME':'Cell_name','BSCName':'RNC_name'},inplace=True)
    print(time.time()-c1,'lookup finish')

    hw_agr1=pd.merge(hw,hw2,on=['Cell_name','RNC_name','Date'],how='left')
    hw_agr=pd.merge(hw_agr1,hw3,on=['Cell_name','RNC_name','Date'],how='left')
    print(time.time()-c1)

    hw_agr['hsdpa_dr_num']=hw_agr['hsdpa_dr_num']-hw_agr['73422167']
    hw_agr['hsdpa_dr_den']=hw_agr['hsdpa_dr_den']+hw_agr['73422161']
    hw_agr['hsupa_dr_num']=hw_agr['hsupa_dr_num']-hw_agr['73422168']
    hw_agr['hsupa_dr_den']=hw_agr['hsupa_dr_den']+hw_agr['73422163']
    hw_agr['r99_dr_num']=hw_agr['73403806']-hw_agr['r99_dr_num']
    hw_agr['r99_dr_den']=hw_agr['73403806']+hw_agr['73403807']-hw_agr['r99_dr_den']
    hw_agr['ps_traf']=hw_agr['hspa_traf']+hw_agr['r99_dl_traf']+hw_agr['r99_ul_traf']
    hw_agr['Vendor']='Huawei'
    hw_agr['Site_name']=hw_agr['Site_name_x'].astype(str)
    hw_agr['lookup']=hw_agr['Site_name'].apply(lambda x: x[1:])
    #tracker=pd.read_excel(r'\\file-server\AZERCONNECT_LLC_OLD\Corporate Folder\CTO\Technology trackers\RNP\Azerconnect_RNP_tracker.xlsx',skiprows=[0])
    hw_agr=pd.merge(hw_agr,tracker[['SITE_ID','Economical Region']],left_on='lookup',right_on='SITE_ID',how='left')
    hw_agr.rename(columns={'Economical Region':'Region'},inplace=True)
    print(time.time()-c1,'end of merging')
    hw_agr_finish=hw_agr[['Date','Vendor','RNC_name','Site_name','Cell_name','Region','voice_sr_num1','voice_sr_den1',
                       'voice_sr_num2','voice_sr_den2','voice_dr_num','voice_dr_den','hsdpa_sr_num','hsdpa_sr_den','hsupa_sr_num','hsupa_sr_den',
                       'hsdpa_dr_num','hsdpa_dr_den','hsupa_dr_num','hsupa_dr_den','cell_avail_num','cell_avail_den',
                       'cell_avail_blck_den','cell_avail_blck_num','cs_traf','ps_traf','hsdpa_thrp_num','hsdpa_thrp_den','r99_sr_num','r99_sr_den',
                       'r99_dr_num','r99_dr_den','cs_inter_freq_ho_num','cs_inter_freq_ho_den',
                       'ps_inter_freq_ho_num','ps_inter_freq_ho_den','cs_sho_ho_num','cs_sho_ho_den','ps_sho_ho_num','ps_sho_ho_den',
                       'cs_inter_rat_ho_num','cs_inter_rat_ho_den','ps_ac_sr_num1','ps_ac_sr_den1','ps_ac_sr_num2','ps_ac_sr_den2']]
    print(time.time()-c1,'end of all')
    hw_agr_finish.iloc[:,6:]=hw_agr_finish.iloc[:,6:].astype(np.float64)
    hw_agr_finish.drop_duplicates(keep='first',inplace=True)
    hw_rnc=hw_agr_finish.groupby(['Date','RNC_name','Vendor','Region']).sum()
    hw_rnc.reset_index(inplace=True)
    print(time.time()-c1,'time to save')
    #file_name = datetime.datetime.strftime(hw_rnc['Date'].iloc[0], "%B_%Y") it was working
    #file_name2 = datetime.datetime.strftime(hw_rnc['Date'].iloc[0], "%Y-%m-%d") it was working

    #print('file name is ', file_name,' and file name2 is ',file_name2)
    for i in hw_rnc['Date'].unique():
        file_name = datetime.datetime.strftime(datetime.datetime.utcfromtimestamp(i.astype(datetime.datetime)/1e9), "%B_%Y")
        file_name2 = datetime.datetime.strftime(datetime.datetime.utcfromtimestamp(i.astype(datetime.datetime)/1e9), "%Y-%m-%d")
        hw_agr_finish.loc[hw_agr_finish['Date']==i].to_hdf(r'/disk2/support_files/archive/ran/' + file_name2 + '.h5', '/threeG', append=True,
                             format='table',data_columns=['Date', 'RNC_name', 'Site_name', 'Cell_name', 'Vendor', 'Region'], complevel=5,
                             min_itemsize={'RNC_name': 10, 'Site_name': 20, 'Cell_name': 20, 'Vendor': 10, 'Region': 15})
        hw_rnc.loc[hw_rnc['Date']==i].to_hdf(r'/disk2/support_files/archive/ran/' + file_name2 + '.h5', '/threeG/bsc', append=True,
                      format='table', data_columns=['Date', 'RNC_name', 'Vendor', 'Region'], complevel=5,
                      min_itemsize={'RNC_name': 10, 'Vendor': 10, 'Region': 15})
    #hw_agr_finish.to_hdf(r'/disk2/support_files/archive/'+file_name+'.h5','/threeG',append=True,
    #           format='table',data_columns=['Date','RNC_name','Site_name','Cell_name','Vendor','Region'],complevel=5,
    #                min_itemsize={'RNC_name':10,'Site_name':20,'Cell_name':20,'Vendor':10,'Region':15})
    #hw_rnc.to_hdf(r'/disk2/support_files/archive/'+file_name+'.h5','/threeG/bsc',append=True,
    #           format='table',data_columns=['Date','RNC_name','Vendor','Region'],complevel=5,
    #                min_itemsize={'RNC_name':10,'Vendor':10,'Region':15})
    hw_rnc.to_hdf(r'/disk2/support_files/archive/combined_bsc.h5', '/threeG',
                  append=True,
                  format='table', data_columns=['Date', 'RNC_name', 'Vendor', 'Region'], complevel=5,
                  min_itemsize={'RNC_name': 10, 'Vendor': 10, 'Region': 15})
    
    print(time.time()-c1,'end of all2')


