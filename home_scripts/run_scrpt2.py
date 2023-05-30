import os
import pandas as pd 
from multiprocessing import Pool

# wrap your csv importer in a function that can be mapped
def read_csv(filename):
    'converts a filename to a pandas dataframe'
    #print('read ',filename)
    return pd.read_csv(os.path.join('/mnt/raw_counters/Corporate Folder/CTO/ALARM_2020',filename))


def main():
    # set up your pool
    pool = Pool(processes=8) # or whatever your hardware can support

    # get a list of file names
    files = os.listdir('/mnt/raw_counters/Corporate Folder/CTO/ALARM_2020')
    file_list = [filename for filename in files if (filename.split('.')[1]=='csv') & ('U2000' in filename)]

    # have your pool map the file names to dataframes
    df_list = pool.map(read_csv, file_list)
    print(len(df_list))
    # reduce the list of dataframes to a single dataframe
    combined_df = pd.concat(df_list, ignore_index=True)

if __name__ == '__main__':
    main()
