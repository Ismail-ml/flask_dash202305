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
    	df=df[df['AlarmName'].str.contains('POWER|Power|power|Rectifier|RECTIFIER|PHASE|Phase')]
    	k+=1
    	print('read ',k,filename,'len h:',len(h))
    except:
    	print('error in file',filename)
    	df=pd.DataFrame()
    	k+=1 
    return df


def main():
    # set up your pool
    c1=time.time()
    #df_list=Manager().list()
    pool = Pool(processes=8) # or whatever your hardware can support

    # get a list of file names
    file_list=glob.glob("/mnt/raw_counters/Corporate Folder/CTO/ALARM_LOG/202111*.zip")
    #files = os.listdir('/mnt/raw_counters/Corporate Folder/CTO/ALARM_LOG')
    #file_list = [filename for filename in files if (filename.split('.')[1]=='zip')]
    # have your pool map the file names to dataframes
    #if j==-1:
    #	df_list = pool.map(read_csv, file_list[i:])
    #else:
    df_list = pool.map(read_csv, file_list)
    #res = pool.map_async(partial(read_csv,df_list),file_list[i:j])
    #res.wait()
    #print(len(df_list))
    # reduce the list of dataframes to a single dataframe
    combined_df = pd.concat(df_list, ignore_index=True)
    combined_df.to_csv('u2000_alarms_Nov.csv')
    combined_df.drop_duplicates().to_csv('u2000_alarms_Nov_without_duplicates.csv')
    #print(combined_df.shape)
    #print(combined_df.drop_duplicates().shape)
    print(time.time()-c1)
    #h.append(combined_df.drop_duplicates())
    #combined_df.to_csv('u2000_alarms2.csv')
    #combined_df.drop_duplicates().to_csv('u2000_alarms2_without_duplicates.csv')
main()
#i=0
#j=100
#new=[]
#while True:
#    print(i,j,'length of i,j')
#    if j>=386500:
#    	main(j,-1)
#    	new.append(pd.concat(h).drop_duplicates())
# 	break
#    main(i,j)
#    i+=100
#    j+=100
#    if len(h)==5:
    	#print('entered here')
#    	new.append(pd.concat(h).drop_duplicates())
#    	h=[]
#    if (len(new)%10==0) and (len(new)>0):
#    	pd.concat(new).drop_duplicates().to_csv('u2000_alarms'+str(j)+'_NEW_without_duplicates.csv')
#    	new=[]
#print(len(new),'h len')
#print(pd.concat(new).shape)

#pd.concat(new).to_csv('u2000_alarms_NEW.csv')
#pd.concat(new).drop_duplicates().to_csv('u2000_alarms_NEW_without_duplicates.csv')
