import os
import pandas as pd
import dask.dataframe as dd
import time
import glob
import numpy as np
path='/mnt/raw_counters/Corporate Folder/CTO/ALARM_2020'

dfs=[]
dfs2=[]
read=[]
errored=[]
dtypes={'NAME':'str','DN':'str','NENAME':'str','ALARM_TIME':'str','CLEAR_TIME':'str','ALARM_NUMBER':'str','ALARM_NAME':'str'}

i=0
all_files=glob.glob(path+"/*U2000*.csv")

	#print(all_files)
for file in all_files:
	if ('U2000' in file):
		try:
			#print(file)
			c1=time.time()
			dfn=pd.read_csv(file,chunksize=100000)
			#print('read',file,time.time()-c1)
			for df in dfn:
				#dfs.append(df.values)
				df=df[df['ALARM_NAME'].notnull()]
				dfs.append(df[df['ALARM_NAME'].str.contains('Power|power|Rectifier|PHASE|Phase')].values)
			#print('appended',time.time()-c1)
			#df.to_csv(os.path.join('/disk2/support_files/archive',str(i)+'.csv'))
			read.append(file)
			i+=1
			if i%100==0: print(i)
		except Exception as e:
			print(e)
			errored.append(file)
			i+=1
			continue
	print('len of dfs',len(dfs))
	if i==100:
		df=pd.DataFrame(np.vstack(dfs))
		#df=pd.concat(dfs)
		df=df.drop_duplicates()
		dfs2.append(df)
		print('combined appended')

print('exit from loop')
df_n=pd.concat(dfs)
df_n.to_csv('U2000_2020_alarms1.csv')
df_n.to_csv('U2000_2020_alarms1_duplicates_removed.csv')
print('shape',df_n.shape)
print(len(errored),'errored len')
print(len(read),'read len')
