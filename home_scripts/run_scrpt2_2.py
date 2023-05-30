import os,time
import pandas as pd 
from multiprocessing import Pool
#from functools import partial
# wrap your csv importer in a function that can be mapped
k=0
h=[]
def read_csv(filename):
    'converts a filename to a pandas dataframe'
    global k
    try:
    	df=pd.read_csv(os.path.join('/mnt/raw_counters/Corporate Folder/CTO/ALARM_2021',filename))
    	df=df[df['ALARM_NAME'].notnull()]
    	df=df[df['ALARM_NAME'].str.contains('Power|power|Rectifier|PHASE|Phase')]
    	k+=1
    	#print('read ',k,filename,'len h:',len(h))
    except:
    	print('error in file',filename)
    	df=pd.DataFrame()
    	k+=1 
    return df


def main(i,j):
    # set up your pool
    c1=time.time()
    #df_list=Manager().list()
    pool = Pool(processes=8) # or whatever your hardware can support

    # get a list of file names
    files = os.listdir('/mnt/raw_counters/Corporate Folder/CTO/ALARM_2021')
    file_list = [filename for filename in files if (filename.split('.')[1]=='csv') & ('U2000' in filename)]
    # have your pool map the file names to dataframes
    if j==-1:
    	df_list = pool.map(read_csv, file_list[i:990])
    else:
    	df_list = pool.map(read_csv, file_list[i:j])
    #res = pool.map_async(partial(read_csv,df_list),file_list[i:j])
    #res.wait()
    #print(len(df_list))
    # reduce the list of dataframes to a single dataframe
    combined_df = pd.concat(df_list, ignore_index=True)
    #print(combined_df.shape)
    #print(combined_df.drop_duplicates().shape)
    print(time.time()-c1)
    h.append(combined_df.drop_duplicates())
    #combined_df.to_csv('u2000_alarms2.csv')
    #combined_df.drop_duplicates().to_csv('u2000_alarms2_without_duplicates.csv')

i=0
j=50
new=[]
while True:
    print(i,j,'length of i,j')
    if j>=950:
    	main(j,-1)
    	new.append(pd.concat(h).drop_duplicates())
    	break
    main(i,j)
    i+=50
    j+=50
    if len(h)==5:
    	#print('entered here')
    	new.append(pd.concat(h).drop_duplicates())
    	h=[]
#print(len(new),'h len')
#print(pd.concat(new).shape)

pd.concat(new).to_csv('u2000_alarms3.csv')
pd.concat(new).drop_duplicates().to_csv('u2000_alarms3_without_duplicates.csv')
