import os
import re
import datetime
import subprocess

try:
    os.popen('cp /home/qarftp/Huawei/*counter*.csv /mnt/raw_counters/"Corporate Folder"/CTO/SOC/QA/"RAN QA"/Daily/Raw_counters/Pool/Huawei').read()
#os.popen('cp /home/qarftp/Nokia/*counter*.csv /mnt/raw_counters/"Corporate Folder"/CTO/SOC/QA/"RAN QA"/Daily/Raw_counters/Pool/Nokia').read()
    os.popen('cp /home/qarftp/Nokia/*.zip /mnt/raw_counters/"Corporate Folder"/CTO/SOC/QA/"RAN QA"/Daily/Raw_counters/Pool/Nokia').read()
    os.chdir('/home/qarftp/Nokia')
#os.popen('cp *Power_Util*.csv *LCG*.csv *CE_Util*.csv *PRB_Util*.csv *RSRAN131*.csv *3G_FRM*.csv *LTE_Frame*.csv /mnt/raw_counters/"Corporate Folder"/CTO/SOC/QA/"RAN QA"/Daily/Raw_counters/Pool/Utilization').read()
    os.chdir('/home/qarftp/Huawei')
    os.popen('cp *3G_TCP*.csv *Frame_Loss*.csv *IPPM*.csv *LTE_RRC*.csv *"LTE RRC"*.csv *LTE_PRB*.csv /mnt/raw_counters/"Corporate Folder"/CTO/SOC/QA/"RAN QA"/Daily/Raw_counters/Pool/Utilization').read()
    os.popen('cp *qar_*.csv /mnt/raw_counters/"Corporate Folder"/CTO/SOC/QA/"RAN QA"/Daily/Raw_counters/Pool/Core').read()
#subprocess.call([r'D:\Schedule\import_raw_data.bat'])
#subprocess.call([r'D:\Schedule\move files.bat'])
    path='/mnt/raw_counters/Corporate Folder/CTO/SOC/QA/RAN QA/Daily/Raw_counters'
except Exception as e:
    print(e)
    1

for vendor in ['Nokia','Huawei','Core','Utilization']:

    filenames = os.listdir(os.path.join(path,'Pool',vendor))

    for file in filenames:
        if (vendor != 'Core') and (vendor != 'Utilization'):
            if vendor=='Huawei':
                try:
                    tarix=re.search(r"60_\d{8}",file)[0].replace('60_','')
                    d=datetime.datetime.strptime(tarix,'%Y%m%d')
                    day=str(datetime.datetime.strftime(d,'%e').strip(' '))
                    tech=re.search(r"\wG",file)[0]
                    extension=re.search(r"counter_\w",file)[0][-1]
                except Exception as e:
                    print(file)
                    print(e,'exception')
            elif vendor=='Nokia':
                try:
                    tarix=re.search(r"\d{4}_\d{2}_\d{2}",file)[0]
                    d=datetime.datetime.strptime(tarix,'%Y_%m_%d')
                    if '00:37' in file:
                        day=str(datetime.datetime.strftime(d-datetime.timedelta(1),'%e').strip(' '))
                    else:
                        day = str(datetime.datetime.strftime(d, '%e').strip(' '))

                    tech='common'
                    extension='counters'
                except Exception as e:
                    print(file)
                    print(e,'exception')
            if '2G_intef_ta' in file:
                extension='interf_ta' 
            year=datetime.datetime.strftime(d,'%Y')
            month_name=datetime.datetime.strftime(d,'%B')
            month=datetime.datetime.strftime(d,'%m')
            if not os.path.exists(os.path.join(path,vendor,tech,extension)):
                os.mkdir(os.path.join(path,vendor,tech))
                os.mkdir(os.path.join(path,vendor,tech,extension))
            if not os.path.exists(os.path.join(path,vendor,tech,extension,str(year),str(month_name),str(day+"."+month+"."+year))):
                if not os.path.exists(os.path.join(path,vendor,tech,extension,str(year))):
                    os.mkdir(os.path.join(path,vendor,tech,extension,str(year)))
                if not os.path.exists(os.path.join(path,vendor,tech,extension,str(year),str(month_name))):
                    os.mkdir(os.path.join(path,vendor,tech,extension,str(year),str(month_name)))
                os.mkdir(os.path.join(path,vendor,tech,extension,str(year),str(month_name),str(day+"."+month+"."+year)))
            os.replace(src=os.path.join(path,'Pool',vendor,file),dst=os.path.join(path,vendor,tech,extension,str(year),str(month_name),str(day+"."+month+"."+year),file))
        elif vendor == 'Core':
            try:
                tarix = re.search(r"60_\d{8}", file)[0].replace('60_', '')
                d = datetime.datetime.strptime(tarix, '%Y%m%d')
                day = str(datetime.datetime.strftime(d, '%e').strip(' '))
                year=datetime.datetime.strftime(d,'%Y')
                month_name=datetime.datetime.strftime(d,'%B')
                month=datetime.datetime.strftime(d,'%m')
                if not os.path.exists(os.path.join(path,vendor,str(year),str(month_name),str(day+"."+month+"."+year))):
                    if not os.path.exists(os.path.join(path,vendor,str(year))):
                        os.mkdir(os.path.join(path,vendor,str(year)))
                    if not os.path.exists(os.path.join(path,vendor,str(year),str(month_name))):
                        os.mkdir(os.path.join(path,vendor,str(year),str(month_name)))
                    os.mkdir(os.path.join(path,vendor,str(year),str(month_name),str(day+"."+month+"."+year)))
                
                os.replace(src=os.path.join(path,'Pool',vendor,file),dst=os.path.join(path,vendor,str(year),str(month_name),str(day+"."+month+"."+year),file))
            except: 1
        else:
            try:
                if (('Power' in file) or ('LCG_Baseband' in file) or ('CE_Util' in file) or ('RSRAN131' in file) or (
                        'PRB_Utilization-' in file) or ('3G_FRM' in file) or ('LTE_Frame' in file)):
                    if ('-07_' not in file):
                        os.remove(os.path.join(path,'Pool',vendor,file))
                    tarix = re.search(r"\d{4}_\d{2}_\d{2}", file)[0]
                    d = datetime.datetime.strptime(tarix, '%Y_%m_%d')
                    day = str(datetime.datetime.strftime(d - datetime.timedelta(1), '%e').strip(' '))
                else:
                    tarix = re.search(r"60_\d{8}", file)[0].replace('60_', '')
                    d = datetime.datetime.strptime(tarix, '%Y%m%d')
                    day = str(datetime.datetime.strftime(d, '%e').strip(' '))
                year = datetime.datetime.strftime(d, '%Y')
                month_name = datetime.datetime.strftime(d, '%B')
                month = datetime.datetime.strftime(d, '%m')
                if not os.path.exists(os.path.join(path,vendor,str(year),str(month_name),str(day+"."+month+"."+year))):
                    if not os.path.exists(os.path.join(path,vendor,str(year))):
                        os.mkdir(os.path.join(path,vendor,str(year)))
                    if not os.path.exists(os.path.join(path,vendor,str(year),str(month_name))):
                        os.mkdir(os.path.join(path,vendor,str(year),str(month_name)))
                    os.mkdir(os.path.join(path,vendor,str(year),str(month_name),str(day+"."+month+"."+year)))
                os.replace(src=os.path.join(path, 'Pool', vendor, file), dst=os.path.join(path, vendor, str(year), str(month_name), str(day + "." + month + "." + year), file))
            except: 1

