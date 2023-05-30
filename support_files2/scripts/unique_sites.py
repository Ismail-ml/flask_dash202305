import pandas as pd
pd.DataFrame(pd.read_hdf('/disk2/support_files/archive/ran/2022-05-12.h5','twoG').sort_values(by='Site_name')['Site_name'].unique())\
.to_csv('/home/ismayil/flask_dash/support_files/2G_sites.csv')
pd.DataFrame(pd.read_hdf('/disk2/support_files/archive/ran/2022-05-12.h5','threeG').sort_values(by='Site_name')['Site_name'].unique())\
.to_csv('/home/ismayil/flask_dash/support_files/3G_sites.csv')
pd.DataFrame(pd.read_hdf('/disk2/support_files/archive/ran/2022-05-12.h5','fourG').sort_values(by='Site_name')['Site_name'].unique())\
.to_csv('/home/ismayil/flask_dash/support_files/4G_sites.csv')

