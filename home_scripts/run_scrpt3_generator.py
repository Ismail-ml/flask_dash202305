import os,time
import pandas as pd 
from multiprocessing import Pool
import glob
#from functools import partial
# wrap your csv importer in a function that can be mapped
k=0
h=[]
def read_csv(filename):
    'converts a filename to a pandas dataframe'
    global k
    try:
    	#df=pd.read_csv(os.path.join('/mnt/raw_counters/Corporate Folder/CTO/ALARM_LOG',filename))
    	df=pd.read_csv(filename,compression='zip')
    	df=df[df['AlarmName'].notnull()]
    	df=df[df['AlarmName'].str.contains('GENERATOR|Generator|generator')]
    	k+=1
    	#print('read ',k,filename,'len h:',len(h))
    except:
    	print('error in file',filename)
    	df=pd.DataFrame()
    	k+=1 
    return df

file_list=glob.glob("/mnt/raw_counters/Corporate Folder/CTO/ALARM_LOG/2022021*.zip")
print(len(file_list))
def main(i,j):
    # set up your pool
    c1=time.time()
    #df_list=Manager().list()
    pool = Pool(processes=8) # or whatever your hardware can support

    # get a list of file names
   ###### #file_list=glob.glob("/mnt/raw_counters/Corporate Folder/CTO/ALARM_LOG/202110*.zip")
    #files = os.listdir('/mnt/raw_counters/Corporate Folder/CTO/ALARM_LOG')
    #file_list = [filename for filename in files if (filename.split('.')[1]=='zip')]
    # have your pool map the file names to dataframes
    if j==-1:
    	df_list = pool.map(read_csv, file_list[i:])
    else:
    	df_list = pool.map(read_csv, file_list[i:j])
    #res = pool.map_async(partial(read_csv,df_list),file_list[i:j])
    #res.wait()
    #print(len(df_list))
    # reduce the list of dataframes to a single dataframe
    if len(df_list)>0:
        combined_df = pd.concat(df_list, ignore_index=True)
    #print(combined_df.shape)
    #print(combined_df.drop_duplicates().shape)
        print(time.time()-c1)
        h.append(combined_df.drop_duplicates())
    #combined_df.to_csv('u2000_alarms2.csv')
    #combined_df.drop_duplicates().to_csv('u2000_alarms2_without_duplicates.csv')

i=0
j=500
new=[]
while True:
    print(i,j,'length of i,j')
    if j>=len(file_list):  #386500
    	main(i,-1)
    	new.append(pd.concat(h).drop_duplicates())
    	break
    main(i,j)
    i+=500
    j+=500
    if len(h)==5:
    	#print('entered here')
    	new.append(pd.concat(h).drop_duplicates())
    	h=[]
    if (len(new)%10==0) and (len(new)>0):
    	pd.concat(new).drop_duplicates().to_csv('u2000_alarms'+str(j)+'_Feb22_gen_without_duplicates.csv')
    	new=[]
#print(len(new),'h len')
#print(pd.concat(new).shape)

pd.concat(new).to_csv('u2000_alarms_Feb22_gen.csv')
pd.concat(new).drop_duplicates().to_csv('u2000_alarms_Feb22_gen_without_duplicates.csv')
