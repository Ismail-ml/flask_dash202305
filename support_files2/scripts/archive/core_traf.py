import pandas as pd
import os

#existing_files = pd.read_csv(os.path.join('/home/ismayil/flask_dash/data/core', 'files.txt'), sep=" ", header=None)
for k in ['AZF','BKC']:
    print(len(os.listdir('/home/ismayil/flask_dash/data/core')), 'total files')
    for file in os.listdir('/home/ismayil/flask_dash/data/core'):
        #if os.path.basename(file) in existing_files: continue
        if ('Data_Traf_APN_'+k) in file:
            df=pd.read_csv(os.path.join('/home/ismayil/flask_dash/data/core',file),skiprows=[1])
            df['Result Time'] = pd.to_datetime(df['Result Time'], format='%Y-%m-%d %H:%M:%S')
            df['Total 2g/3g vol']=df['134706896']+df['134706897']
            df['lte dl vol']=df['138413071']+df['142608006']
            df['lte ul vol']=df['138413069']+df['142608004']
            df['lte total vol']=df['lte dl vol']+df['lte ul vol']
            df['dl total vol']=df['134706897']+df['lte dl vol']
            df['ul total vol']=df['134706896']+df['lte ul vol']
            df['total netw vol']=df['dl total vol']+df['ul total vol']
            df.iloc[:,4:]=df.iloc[:,4:].apply(lambda x: x/1024)

            df_g=df[['Result Time','134706897','134706896','Total 2g/3g vol','lte dl vol','lte ul vol','lte total vol','dl total vol','ul total vol','total netw vol']].groupby('Result Time').sum()
            df_g.reset_index(inplace=True)
            
            df_g['2G/3G DL throughput, Mbit/s']=df_g['134706897']*8/3600
            df_g['2G/3G UL throughput, Mbit/s']=df_g['134706896']*8/3600
            df_g['2G/3G Total throughput, Mbit/s']=df_g['Total 2g/3g vol']*8/3600
            df_g['LTE DL throughput, Mbit/s']=df_g['lte dl vol']*8/3600
            df_g['LTE UL throughput, Mbit/s']=df_g['lte ul vol']*8/3600
            df_g['LTE Total throughput, Mbit/s']=df_g['lte total vol']*8/3600
            df_g['Total DL throughput, Mbit/s']=df_g['dl total vol']*8/3600
            df_g['Total UL throughput, Mbit/s']=df_g['ul total vol']*8/3600
            df_g['Total NW throughput, Mbit/s']=df_g['total netw vol']*8/3600
            df_g.rename(columns={'Result Time': 'Date','134706897':'2G/3G DL data traffic, MB','134706896':'2G/3G UL data traffic, MB',
                                 'Total 2g/3g vol':'2G/3G Total data traffic, MB','lte dl vol':'LTE DL data traffic, MB',
                                 'lte ul vol':'LTE UL data traffic, MB','lte total vol':'LTE Total data traffic, MB',
                                 'dl total vol':'DL data traffic, MB','ul total vol':'UL data traffic, MB','total netw vol':'Total data traffic, MB'}, inplace=True)
            df_g.insert(1,'MNO',k)


            df_g.to_hdf('/disk2/support_files/archive/core_inputs.h5','/data',append=True,
                  format='table', data_columns=['Date', 'MNO'], complevel=5)

lu_sr,pag,cst,pdp,s1=([] for i in range(5))
for file in os.listdir('/home/ismayil/flask_dash/data/core'):
    if ('LU_SR' in file) or ('Pag_SR' in file) or ('C_setup' in file) or ('usn_kpis' in file) or ('LTE_rep' in file):
        df=pd.read_csv(os.path.join('/home/ismayil/flask_dash/data/core',file),skiprows=[1])
        df_g=df.groupby('Result Time').sum()
        df_g.reset_index(inplace=True)
        df_g['Date']=pd.to_datetime(df['Result Time'])
        if 'LU_SR' in file:
            #df_g['LU_SR_VLR']=df_g['84151990']/df_g['84151989']*100
            #df_g['LU_SR_Roaming']=df_g['84151992']/df_g['84151991']*100
            df_gg=df_g[['Date','84151989','84151990','84151991','84151992']]
            lu_sr.append(df_gg)
        elif 'Pag_SR' in file:
            #df_g['2G_PAG_SR']=(df_g['84152186']+df_g['84152188'])/df_g['84152185']*100
            #df_g['3G_PAG_SR']=(df_g['84152192']+df_g['84152190'])/df_g['84152189']*100
            df_gg=df_g[['Date','84152186','84152188','84152185','84152192','84152190','84152189']]
            pag.append(df_gg)
        elif 'C_setup' in file:
            #df_g['2G_CST']=df_g['84148248']/df_g['84148235']
            #df_g['3G_CST']=df_g['84148278']/df_g['84148265']
            df_gg=df_g[['Date','84148248','84148235','84148278','84148265']]
            cst.append(df_gg)
        elif 'usn_kpis' in file:
            #df_g['2G_Attach_SR']=df_g['117454514']/df_g['117454513']*100
            #df_g['3G_Attach_SR']=df_g['117456614']/df_g['117456613']*100
            #df_g['2G_PDP_Ctx_SR']=df_g['117458514']/df_g['117458513']*100
            #df_g['3G_PDP_Ctx_SR']=df_g['117459414']/df_g['117459413']*100
            df_gg=df_g[['Date','117454514','117454513',
                        '117456614','117456613','117458514','117458513','117459414','117459413']]
            pdp.append(df_gg)
        elif 'LTE_rep' in file:
            #df_g['Bearer_Setup_SR']=df_g['117495953']/df_g['117495952']*100
            df_gg=df_g[['Date','117495953','117495952']]
            s1.append(df_gg)

for i in ['lu_sr','pag','cst','pdp','s1']:
    pd.concat(eval(i)).to_hdf('/disk2/support_files/archive/core_inputs.h5',i,append=True,
                  format='table', data_columns=['Date'], complevel=5)
pd.DataFrame(os.listdir(os.path.join('/home/ismayil/flask_dash/data', 'core'))).to_csv(os.path.join('/home/ismayil/flask_dash/data', 'files.csv'), mode='a', header=None, index=False)
#for file in os.listdir('/home/ismayil/flask_dash/data/core'):
    # if os.path.basename(file) in existing_files: continue
#    if 'VLR_reg_subs' in file:
 #       df = pd.read_csv(os.path.join('/home/ismayil/flask_dash/data/core', file), skiprows=[1])
  #      df.rename(columns={'84152372':'Total VLR Subs','84166045':'2G VLR Total Subs','84166046':'3G VLR Total Subs','84152335':'BKC VLR Local Subs'},inplace=True)
   #     df['Result Time'] = pd.to_datetime(df['Result Time'], format='%Y-%m-%d %H:%M:%S')
    #   df = df.groupby('Result Time').sum()
     #   df.reset_index(inplace=True)
      #  df['Date'] = pd.to_datetime(df['Result Time']).dt.date
       # df['Hour'] = pd.to_datetime(df['Result Time']).dt.time
        #df=df[df['Hour'].astype(str)=='20:00:00']
        #df_final=df[['Date','Total VLR Subs','2G VLR Total Subs','3G VLR Total Subs','BKC VLR Local Subs','AZF total VLR Subs']]
        #if len(df_final)>0:
         #   df_final.to_hdf('/home/ismayil/flask_dash/support_files/core_inputs.h5','/vlr',append=True,
          #        format='table', data_columns=['Date'], complevel=5)
#pd.DataFrame(os.listdir(os.path.join('/home/ismayil/flask_dash/data', 'core'))).to_csv(os.path.join('/home/ismayil/flask_dash/data', 'files.csv'), mode='a', header=None, index=False)

