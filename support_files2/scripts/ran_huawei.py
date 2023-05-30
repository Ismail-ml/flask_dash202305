import pandas as pd
import os
import huawei_2g, huawei_3g, huawei_4g
tracker=pd.read_csv('/home/ismayil/flask_dash/support_files/tracker.csv')
folder_main="/home/ismayil/flask_dash/data"
sub_folder=['huawei/2g/1','huawei/2g/2','huawei/2g/3','huawei/3g/1','huawei/3g/2','huawei/3g/3','huawei/4g/1','huawei/4g/2','huawei/2g/ta_inter']

try:
    huawei_2g.run(*[os.path.join(folder_main,sub_folder[i]) for i in (0,1,2,8)],tracker)
    for j in [0,1,2,8]:
        pd.DataFrame(os.listdir(os.path.join(folder_main, sub_folder[j]))).to_csv(os.path.join(folder_main, 'files.csv'), mode='a', header=None, index=False)
except: 1
try:
    huawei_3g.run(*[os.path.join(folder_main,sub_folder[i]) for i in (3,4,5)],tracker)
    for j in [3,4,5]:
        pd.DataFrame(os.listdir(os.path.join(folder_main, sub_folder[j]))).to_csv(os.path.join(folder_main, 'files.csv'), mode='a', header=None, index=False)
except: 1
try:
    huawei_4g.run(*[os.path.join(folder_main,sub_folder[i]) for i in (6,7)],tracker)
    for j in [6,7]:
        pd.DataFrame(os.listdir(os.path.join(folder_main, sub_folder[j]))).to_csv(os.path.join(folder_main, 'files.csv'), mode='a', header=None, index=False)
except: 1
