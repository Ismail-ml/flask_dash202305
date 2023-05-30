import pandas as pd
df=pd.HDFStore('/disk2/support_files/archive/ran/2022-11-30.h5','r+')
t=df.select('twoG',where='Vendor=Huawei and Date>="2022-11-30"').drop_duplicates()
tb=df.select('twoG/bsc',where='Vendor=Huawei and Date>="2022-11-30"').drop_duplicates()
th=df.select('threeG',where='Vendor=Huawei and Date>="2022-11-30"').drop_duplicates()
thb=df.select('threeG/bsc',where='Vendor=Huawei and Date>="2022-11-30"').drop_duplicates()
f=df.select('fourG',where='Vendor=Huawei and Date>="2022-11-30"').drop_duplicates()
fb=df.select('fourG/bsc',where='Vendor=Huawei and Date>="2022-11-30"').drop_duplicates()
file_name2='2022-11-30'
df.remove('twoG',where='Vendor=Huawei and Date>="2022-11-30"')
df.remove('twoG/bsc',where='Vendor=Huawei and Date>="2022-11-30"')
df.remove('threeG',where='Vendor=Huawei and Date>="2022-11-30"')
df.remove('threeG/bsc',where='Vendor=Huawei and Date>="2022-11-30"')
df.remove('fourG',where='Vendor=Huawei and Date>="2022-11-30"')
df.remove('fourG/bsc',where='Vendor=Huawei and Date>="2022-11-30"')
df.close()
t.to_hdf(r'/disk2/support_files/archive/ran/' + file_name2 + '.h5', '/twoG',append=True,
                           format='table', data_columns=['Date', 'BSC_name', 'Site_name', 'Cell_name', 'Vendor', 'Region'],
                           complevel=5,min_itemsize={'BSC_name': 10, 'Site_name': 20, 'Cell_name': 20, 'Vendor': 10, 'Region': 15})
tb.to_hdf(r'/disk2/support_files/archive/ran/' + file_name2 + '.h5', '/twoG/bsc',
                           append=True,format='table', data_columns=['Date', 'BSC_name', 'Vendor', 'Region'], complevel=5,
                           min_itemsize={'BSC_name': 10, 'Vendor': 10, 'Region': 15})
th.to_hdf(r'/disk2/support_files/archive/ran/' + file_name2 + '.h5', '/threeG', append=True,
                       format='table', data_columns=['Date', 'RNC_name', 'Site_name', 'Cell_name', 'Vendor', 'Region'],
                       complevel=5,
                       min_itemsize={'RNC_name': 10, 'Site_name': 20, 'Cell_name': 20, 'Vendor': 10, 'Region': 15})
thb.to_hdf(r'/disk2/support_files/archive/ran/' + file_name2 + '.h5', '/threeG/bsc', append=True,
                       format='table', data_columns=['Date', 'RNC_name', 'Vendor', 'Region'], complevel=5,
                       min_itemsize={'RNC_name': 10, 'Vendor': 10, 'Region': 15})
f.to_hdf(r'/disk2/support_files/archive/ran/' + file_name2 + '.h5', '/fourG', append=True,
                           format='table', data_columns=['Date', 'Site_name', 'Cell_name', 'Vendor', 'Region'], complevel=5,
                           min_itemsize={'Site_name': 20, 'Cell_name': 20, 'Vendor': 10, 'Region': 15})
fb.to_hdf(r'/disk2/support_files/archive/ran/' + file_name2 + '.h5', '/fourG/bsc', append=True,
                           format='table', data_columns=['Date', 'Vendor', 'Region'], complevel=5,
                           min_itemsize={'Vendor': 10, 'Region': 15})
df=pd.HDFStore('/disk2/support_files/archive/combined_bsc.h5','r+')
t=df.select('twoG',where='Vendor=Huawei and Date>="2022-11-30"').drop_duplicates()
th=df.select('threeG',where='Vendor=Huawei and Date>="2022-11-30"').drop_duplicates()
f=df.select('fourG',where='Vendor=Huawei and Date>="2022-11-30"').drop_duplicates()
fn=df.select('fourGn',where='Vendor=Huawei and Date>="2022-11-30"').drop_duplicates()
df.remove('twoG',where='Vendor=Huawei and Date>="2022-11-30"')
df.remove('threeG',where='Vendor=Huawei and Date>="2022-11-30"')
df.remove('fourG',where='Vendor=Huawei and Date>="2022-11-30"')
df.remove('fourGn',where='Vendor=Huawei and Date>="2022-11-30"')
df.close()
t.to_hdf(r'/disk2/support_files/archive/combined_bsc.h5', '/twoG',
                       append=True,
                       format='table', data_columns=['Date', 'BSC_name', 'Vendor', 'Region'], complevel=5,
                       min_itemsize={'BSC_name': 10, 'Vendor': 10, 'Region': 15})
th.to_hdf(r'/disk2/support_files/archive/combined_bsc.h5', '/threeG',append=True,
                   format='table', data_columns=['Date', 'RNC_name', 'Vendor', 'Region'], complevel=5,
                   min_itemsize={'RNC_name': 10, 'Vendor': 10, 'Region': 15})
f.to_hdf(r'/disk2/support_files/archive/combined_bsc.h5', '/fourG',append=True,
                       format='table', data_columns=['Date', 'Vendor', 'Region'], complevel=5,
                       min_itemsize={'Vendor': 10, 'Region': 15})
fn.to_hdf(
            r'/disk2/support_files/archive/combined_bsc.h5', '/fourGn', append=True, format='table', data_columns=['Date', 'Vendor', 'Region'], complevel=5,
            min_itemsize={'Vendor': 10, 'Region': 15})
