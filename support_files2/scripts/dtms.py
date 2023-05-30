import pandas as pd
import glob
import os,datetime
import shutil
def rr():
	os.chdir('/home/ismayil/flask_dash/data/ods_dtms')
	dd={}
	for k in glob.glob('*QA*.csv'):
		try:
			if 'An Error Occurred' in pd.read_csv(k,nrows=0).columns[0]: 
				#shutil.copy(k,'/home/ismayil/')
				continue
			if 'QA_Subscriber_count_by_PLAN' in k:
				df=pd.read_csv(k,skiprows=4)
				df=df[(df['Plan']!='Plan') & (~df['Plan'].str.contains('Units measured')) & (~df['Plan'].str.contains('Plan:'))]
				df=df.set_index(df.columns[0]).T
				df.reset_index(inplace=True)
				#print(df)
				df=df.melt(id_vars='index',value_vars=df.columns[1:])
				name=pd.read_csv(k,nrows=0).columns[0]
				if 'AZF' in k: df.insert(1,'MNO','AZF')
				else: df.insert(1,'MNO','BKC')
				if name in dd.keys():
					dd[name]=pd.DataFrame(dd[name]).append(df)#melt(id_vars=t.columns[0],value_vars=t.columns[1:])])
				else: dd[name]=df
				continue
			else: df=pd.read_csv(k,skiprows=2)
			name=pd.read_csv(k,nrows=0).columns[0]
			seperator=df[(df.iloc[:,1].isnull()) & (~ df.iloc[:,0].str.contains('Units measured|Date'))].iloc[:,0].to_list()
			d={i:df.set_index(df.columns[0]).index.get_loc(seperator[j]) for j,i in enumerate(seperator)}
			p=0
			for i in range(len(d.keys())+1): 
				#print(p,' begins')
				if 'An Error Occurred' in name: 
					#shutil.copy(k,'/home/ismayil/')
					continue
				if 'Errors' in name: continue
				if (i+1)>len(d.keys()):
					t=df.iloc[p:-1]
					t.sort_values(by=t.columns[0],inplace=True)
					#print(t.set_index(t.columns[0]).T.columns)
					t=t.set_index(t.columns[0]).T
					if 'MNO' not in t.columns:
						if 'AZF' in k: t.insert(0,'MNO','AZF')
						else: t.insert(0,'MNO','BKC')
					if name in dd.keys():
						dd[name]=pd.DataFrame(dd[name]).append(t)#melt(id_vars=t.columns[0],value_vars=t.columns[1:])])
					else: dd[name]=t#melt(id_vars=t.columns[0],value_vars=t.columns[1:])
					break
				t=df.iloc[p:d[list(d.keys())[i]]-1]
				t.sort_values(by=t.columns[0],inplace=True)
				#print(t.set_index(t.columns[0]).T.columns)
				t=t.set_index(t.columns[0]).T
				if 'MNO' not in t.keys():
					if 'AZF' in k: t.insert(0,'MNO','AZF')
					else: t.insert(0,'MNO','BKC')
				if name in dd.keys():
					dd[name]=pd.DataFrame(dd[name]).append(t)#melt(id_vars=t.columns[0],value_vars=t.columns[1:])])
				else: dd[name]=t#melt(id_vars=t.T)#columns[0],value_vars=t.columns[1:])
				p=d[list(d.keys())[i]]+3
				name=list(d.keys())[i]
		except Exception as e:
			print(k)
			print(e)
			shutil.copy(k,'/home/ismayil/')
			continue
	return dd

dd=rr()
#print(dd.keys())
print('agg finished')
for i in dd.keys():
	try:
		c=dd[i]
		if 'Peak Subscribers by Plan' not in i:
			#c.columns=c.iloc[0,:].values
			c=c.loc[c[c.columns[0]]!=c.columns[0]].reset_index()
			c=c[c['index']!='Service ID']
			if i not in ['Gx Answer Messages','Gx Request Messages','Peak Subscribers']:
				c=c.melt(id_vars=c.columns[:2],value_vars=c.columns[2:])
		c.columns=c.columns.map(lambda x: x.lower())
		c.rename(columns={'index':'date'},inplace=True)
		c['date']=pd.to_datetime(c['date'])
		c.iloc[:,-1]=c.iloc[:,-1].astype(float)
		if i in ['Gx Answer Messages','Gx Request Messages']:
			c.insert(2,'type',i[3:i.find(' Messages')].lower())
			file='gx_messages'
		elif i in ['New Users','Peak Users']:
			c.insert(2,'type',i.replace(' ','_').lower())
			file='users'
		elif i in ['Total Unaccounted Usage','Unaccounted Received Usage','Unaccounted Transmitted Usage']:
			c.insert(2,'type',i.replace('Unaccounted','').replace(' ','').replace('Usage','').lower())
			file='unaccounted_usage'
		else:
			file=i
		for m in c['date'].unique():
			try:
				file_name2 = datetime.datetime.strftime(datetime.datetime.utcfromtimestamp(m.astype(datetime.datetime)/1e9), "%Y-%m-%d")
		#if (file_name2=='2022-03-27') or (file_name2=='2022-03-28'): continue
			#c.to_csv(i+'.csv',index=False)
				#c.dropna(how='any',inplace=True)
				c.loc[c['date']==m].to_hdf('/disk2/support_files/archive/dtms/dtms_' + file_name2 + '.h5', file.replace(' ','_').lower(), append=True,format='table', 
						complevel=5,data_columns=['date','mno'],min_itemsize={u:20 for u in c.columns[1:]})
			except Exception as e:
				print(e)
				print(m, i)
				continue
	except Exception as e:
		print(e)
		print(i)
		continue
pd.DataFrame(glob.glob('*QA*.csv')).to_csv('files_transferred.csv',mode='a',index=False)
for i in glob.glob('*QA*.csv'): os.remove(i)
