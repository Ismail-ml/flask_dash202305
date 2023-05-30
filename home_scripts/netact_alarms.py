import pandas as pd
import glob
import os

dfs=[]
#def proccess_excel(li):
lil=glob.glob("/mnt/raw_counters/Corporate Folder/CTO/Alarm_LOG/NETACT*202204*.csv")
print(len(lil))
i=0
for file in lil:
    if ('NETACT' in file):
        #print(file)
        try:
           df=pd.read_csv(file,index_col=False)
           df=df[df['TEXT'].notnull()]
           dfs.append(df[df['TEXT'].str.contains('POWER|MPF|RECTIFIER|MAINS')])
           i+=1
           if i%500==0:print(i)
        except Exception as e:
           print(e, file)
           continue
all=pd.concat(dfs)
all.to_csv('Netact_Apr22.csv')
all.drop_duplicates().to_csv('Netact_Apr22_without_duplicates.csv')
