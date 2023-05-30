import pandas as pd
import glob,sys
import os,datetime
import subprocess as sp
from send_notification import send_mail
from datetime import datetime as dt
os.chdir('/disk2/support_files/archive/core')
h,h2,h3,h4,h5,h6=[[] for i in range(6)]
cs_bh=20
ps_bh=22
yesterday = datetime.date.today() - datetime.timedelta(3)
files = pd.date_range(start=yesterday,periods=3, freq='24H').strftime('core_new_%Y-%m-%d.h5').tolist()
folder = pd.date_range(start=yesterday,periods=3, freq='24H').strftime('%Y/%B/%d.%m.%Y').tolist()
files2 = pd.date_range(start=yesterday,periods=3, freq='24H').strftime('_60_%Y%m%d1900').tolist()
main ='/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/RAN QA/Daily/Raw_counters/Core/'
mapping=pd.read_excel('/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/Core QA/Mapping for schedule/mapping.xlsx',
                        sheet_name='2&3G LAC Region')
map2=pd.read_excel('/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/Core QA/Mapping for schedule/mapping.xlsx',
                        sheet_name='TAC Region')
lac,tac=[],[]

for num,i in enumerate(files):
      try:
            df=pd.HDFStore(i,'r')
            a=df.select('pag_per_lac').drop_duplicates()
            a=a[a['LAC']!=1505]
            ### fdsfgdsfgdsfg
            b=a.merge(mapping[['LAC','EReg','QReg','comment']],on='LAC',how='left')
            b['tot']=b.iloc[:,4:-4].sum(axis=1)
            if len(b.loc[(b['EReg'].isnull()) & (b['tot']>0)])>0:
                  if len(b.loc[(b['EReg'].isnull()) & (b['tot']>0) & (b['comment']!='off')])>0:
                        print('condition met from b')
                        lac.append(b.loc[(b['EReg'].isnull()) & (b['tot']>0) & (b['comment']!='off'),['LAC','EReg','VLR 2G LAI Subs','VLR 3G LAI Subs']].drop_duplicates())
                  elif datetime.date.today().weekday()==3:
                        lac.append(b.loc[(b['EReg'].isnull()) & (b['tot']>0),['LAC','EReg','VLR 2G LAI Subs','VLR 3G LAI Subs','comment']].drop_duplicates())
            b['loc_req']=b['VLR Location Update Requests']+b['Roaming Location Update Requests']
            b['loc_suc']=b['VLR Location Update Success']+b['Roaming Location Update Success']
            loc_up=pd.DataFrame(b[b['Date'].dt.hour==cs_bh].groupby('EReg').sum()['loc_req']).T
            loc_up['merge']='a'
            tot=b[b['Date'].dt.hour==cs_bh].groupby('EReg').sum()[['loc_req','loc_suc']].reset_index()
            tot['loc_sr']=tot['loc_suc']/tot['loc_req']*100
            tot=tot[['EReg','loc_sr']].T
            tot.columns=tot.iloc[0,:].values
            tot=tot.iloc[1:]
            tot['a']=None
            tot['merge']='a'
            tot.insert(0,'Date',b['Date'].dt.date.iloc[0])
            h.append(tot.merge(loc_up,how='outer',on='merge').drop(columns='merge'))

            subs_2g=pd.DataFrame(b[b['Date'].dt.hour==cs_bh].groupby('EReg').sum()['VLR 2G LAI Subs']).T
            subs_2g['a']=None
            subs_2g['merge']='a'
            subs_3g=pd.DataFrame(b[b['Date'].dt.hour==cs_bh].groupby('EReg').sum()['VLR 3G LAI Subs']).T
            subs_3g['merge']='a'
            subs=subs_2g.merge(subs_3g,on='merge',how='outer')
            #subs['merge']='a'
            a2=df.select('ps_pag').drop_duplicates()
            a2=a2[a2['LAC']!=1505]
            a2['LAC']=a2['LAC'].astype(int)
            b2=a2.merge(mapping[['LAC','EReg','QReg','comment']],on='LAC',how='left')
            if len(b2.loc[(b2['mode']!='S1 mode') & (b2['EReg'].isnull()) & ((b2['Packet Paging request']>0) | (b2['Max attached subs']>0))])>0:
                  if len(b2.loc[(b2['mode']!='S1 mode') & (b2['EReg'].isnull()) & ((b2['Packet Paging request']>0) | (b2['Max attached subs']>0)) & (b2['comment']!='off')])>0:
                        print('condition met from b2')
                        lac.append(b2.loc[(b2['mode']!='S1 mode') & (b2['EReg'].isnull()) & ((b2['Packet Paging request']>0) | (b2['Max attached subs']>0)) & (b2['comment']!='off'),\
                                    ['LAC','EReg','Packet Paging request','Max attached subs']].drop_duplicates())
                  elif datetime.date.today().weekday()==3:
                        lac.append(b2.loc[(b2['mode']!='S1 mode') & (b2['EReg'].isnull()) & ((b2['Packet Paging request']>0) | (b2['Max attached subs']>0)),\
                                    ['LAC','EReg','Packet Paging request','Max attached subs','comment']].drop_duplicates())

            s1=a2.merge(map2[['TAC','EReg','QReg','comment']],left_on='LAC',right_on='TAC',how='left')
            if len(s1.loc[(s1['mode']=='S1 mode') & (s1['EReg'].isnull()) & ((s1['Packet Paging request']>0) | (s1['Max attached subs']>0))])>0:
                  if len(s1.loc[(s1['mode']=='S1 mode') & (s1['EReg'].isnull()) & ((s1['Packet Paging request']>0) | (s1['Max attached subs']>0)) & (s1['comment']!='off')])>0:
                        print('condition met from s1')

                        tac.append(s1.loc[(s1['mode']=='S1 mode') & (s1['EReg'].isnull()) & ((s1['Packet Paging request']>0) | (s1['Max attached subs']>0)) & (s1['comment']!='off'),\
                                    ['LAC','EReg','Packet Paging request','Max attached subs']].drop_duplicates())
                  elif datetime.date.today().weekday()==3:
                        tac.append(s1.loc[(s1['mode']=='S1 mode') & (s1['EReg'].isnull()) & ((s1['Packet Paging request']>0) | (s1['Max attached subs']>0)),\
                                    ['LAC','EReg','Packet Paging request','Max attached subs','comment']].drop_duplicates())                  
            #s2=s1[(s1['Date'].dt.hour==ps_bh)].groupby(['mode','EReg']).sum().reset_index()
            ss=s1.loc[s1['mode']=='S1 mode'].groupby('Date').sum()
            aa=ss.loc[ss['Max attached subs']==ss['Max attached subs'].max()].index
            s2=s1.set_index('Date').loc[aa].reset_index().groupby(['Date','mode','EReg']).sum().reset_index() ### sonradan artirilib yuxaridakinin evezine
            c=b2[(b2['Date'].dt.hour==ps_bh)].groupby(['mode','EReg']).sum().reset_index()
            s2_pag=s1[(s1['Date'].dt.hour==ps_bh)].groupby(['mode','EReg']).sum().reset_index().copy()
            b2_pag=c.copy()
            c=c.pivot_table(columns='EReg',values='Max attached subs',aggfunc='sum',index='mode')
            c['merge']='a'
            subs['b']=None
            subs['f']=None
            subs=subs.merge(pd.DataFrame(c.loc['Gb mode']).T,on='merge')
            subs['c']=None
            subs=subs.merge(pd.DataFrame(c.loc['Iu mode']).T,on='merge')
            subs['d']=None
            subs['e']=None
            s3=s2[['EReg','Max attached subs']].T.iloc[1:]
            s3[9]=s3[0]+s3[2]
            s3.drop(columns=[0,2],inplace=True)
            s3['merge']='a'
            subs.insert(0,'Date',b['Date'].dt.date.iloc[0])
            h2.append(subs.merge(s3,on='merge',how='outer').drop(columns=['merge','Qarabag_x']))

            pag=pd.DataFrame(b[b['Date'].dt.hour==cs_bh].groupby('EReg').sum()['TwoG_CS Paging SR den']).T
            pag['merge']='a'
            pag3=pd.DataFrame(b[b['Date'].dt.hour==cs_bh].groupby('EReg').sum()['ThreeG_CS Paging SR den']).T
            pag3['merge']='a'
            tot=b[b['Date'].dt.hour==cs_bh].groupby('EReg').sum()[['TwoG_CS Paging SR den','TwoG_CS Paging SR num', 'ThreeG_CS Paging SR den','ThreeG_CS Paging SR num']].reset_index()
            tot['pag sr 2G']=tot['TwoG_CS Paging SR num']/tot['TwoG_CS Paging SR den']*100
            tot['pag sr 3G']=tot['ThreeG_CS Paging SR num']/tot['ThreeG_CS Paging SR den']*100
            cc=pd.DataFrame(tot.pivot_table(columns='EReg',values=['pag sr 2G','TwoG_CS Paging SR den','pag sr 3G','ThreeG_CS Paging SR den'],aggfunc='sum').stack()).T
            cc['a']=None
            cc['Date']=b['Date'].dt.date.iloc[0]
            h3.append(pd.DataFrame([[cc['Date'].values[0],*cc.loc[0,'pag sr 2G'],*cc.loc[0,'a'],*cc.loc[0,'TwoG_CS Paging SR den'],*cc.loc[0,'a'],*cc.loc[0,'pag sr 3G'],\
                           *cc.loc[0,'a'],*cc.loc[0,'ThreeG_CS Paging SR den']]]).drop(columns=[7,17]))

            ps_pagi=pd.concat([b2_pag,s2_pag.drop(columns='TAC')])
            ps_pagi['pag_sr']=ps_pagi['Packet Paging response']/ps_pagi['Packet Paging request']*100
            ps_pagi2=ps_pagi.pivot_table(columns=['mode','EReg'],values=['pag_sr','Packet Paging request'],aggfunc='sum')
            h4.append(pd.DataFrame([[b['Date'].dt.date.iloc[0],*ps_pagi2.loc['pag_sr','Gb mode'],'',*ps_pagi2.loc['Packet Paging request','Gb mode'],'',
                        *ps_pagi2.loc['pag_sr','Iu mode'],'',*ps_pagi2.loc['Packet Paging request','Iu mode'],'',
                        *ps_pagi2.loc['pag_sr','S1 mode'],'',*ps_pagi2.loc['Packet Paging request','S1 mode']]]))


            qreg=b[b['Date'].dt.hour==cs_bh].groupby('QReg').sum().reset_index()
            qreg['2G_cs_pag_sr']=qreg['TwoG_CS Paging SR num']/qreg['TwoG_CS Paging SR den']*100
            qreg['3G_cs_pag_sr']=qreg['ThreeG_CS Paging SR num']/qreg['ThreeG_CS Paging SR den']*100
            qreg2=qreg[['QReg','2G_cs_pag_sr','3G_cs_pag_sr']].pivot_table(columns='QReg',aggfunc='sum')
            ps_pagi3=pd.concat([b2[(b2['Date'].dt.hour==ps_bh)].groupby(['mode','QReg']).sum().reset_index(),
                              s1[(s1['Date'].dt.hour==ps_bh)].groupby(['mode','QReg']).sum().reset_index().drop(columns='TAC')])
            ps_pagi3['pag_sr']=ps_pagi3['Packet Paging response']/ps_pagi3['Packet Paging request']*100
            ps_pagi4=ps_pagi3.pivot_table(columns=['mode','QReg'],aggfunc='sum')
            h5.append(pd.DataFrame([[b['Date'].dt.date.iloc[0],*qreg2.loc['2G_cs_pag_sr'].values,*qreg2.loc['3G_cs_pag_sr'].values,*ps_pagi4.loc['pag_sr'].values]]))

            csfb=pd.read_hdf(i,'ab_vtraff').drop_duplicates()
            csfb2=csfb[csfb['Date'].dt.hour==cs_bh].pivot_table(index='Date',columns='Office',values='Terminated CSFB Call setup duration_sec',aggfunc='mean')
            csfb2['day']=csfb2.reset_index()['Date'].dt.date[0]
            h6.append(csfb2[['day','ASRNCBTCN21','RNCGANH01','RNCLANH02','RNCSHKH03','RNCKHAH04','RNCQUBH05','RNCSVNH07','IPQRBHRNC08','MCRNCBTCN08','MCRNCBTCN09']])
            df.close()
      except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            print(e)
            print(i)
            continue
dist=['ismayilm@azerconnect.az','zahidh@azerconnect.az','yanab@azerconnect.az'] #
if len(lac)>0:
      if dt.now().hour==9:
            if 'comment' in pd.concat(lac):
                  send_mail(dist,'New LAC','There is no region mapping for below LACs',\
                        pd.concat(lac).drop_duplicates().groupby(['LAC','comment']).sum().reset_index(),datetime.datetime.strftime(b['Date'].dt.date.iloc[0],'%d.%m.%Y'),False)
            else: 
                  send_mail(dist,'New LAC','There is no region mapping for below LACs',\
                        pd.concat(lac).drop_duplicates().groupby('LAC').sum().reset_index(),datetime.datetime.strftime(b['Date'].dt.date.iloc[0],'%d.%m.%Y'),False)
if len(tac)>0:
      if dt.now().hour==9:
            if 'comment' in pd.concat(tac):
                  send_mail(dist,'New TAC','There is no region mapping for below TACs',\
                        pd.concat(tac).drop_duplicates().groupby(['LAC','comment']).sum().reset_index(),datetime.datetime.strftime(s1['Date'].dt.date.iloc[0],'%d.%m.%Y'),False)
            else:
                  send_mail(dist,'New TAC','There is no region mapping for below TACs',\
                        pd.concat(tac).drop_duplicates().groupby('LAC').sum().reset_index(),datetime.datetime.strftime(s1['Date'].dt.date.iloc[0],'%d.%m.%Y'),False)
pd.concat(h).to_csv('/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/Core QA/scheduled_reports/region/EReg_lu_'+files2[-1][4:-4]+'.csv',index=False)
pd.concat(h2).to_csv('/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/Core QA/scheduled_reports/region/subs_per_region_'+files2[-1][4:-4]+'.csv',index=False)
pd.concat(h3).to_csv('/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/Core QA/scheduled_reports/region/cs_pag_reg_'+files2[-1][4:-4]+'.csv',index=False)
pd.concat(h4).to_csv('/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/Core QA/scheduled_reports/region/ps_pag_reg_'+files2[-1][4:-4]+'.csv',index=False)
pd.concat(h5).to_csv('/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/Core QA/scheduled_reports/region/gl_reg_pag_'+files2[-1][4:-4]+'.csv',index=False)
pd.concat(h6).to_csv('/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/Core QA/scheduled_reports/region/csfb_times_'+files2[-1][4:-4]+'.csv',index=False)
