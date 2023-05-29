import atoti as tt
import numpy as np
#import webbrowser
import pandas as pd
import time, os
#import glob

#branding_config = tt.BrandingConfig(
#    favicon="/home/ismayil/flask_dash/static/high-performance.ico",
#    logo="/home/ismayil/flask_dash/static/logo.svg"
#    #title="Hey hey hey",
#)

session=tt.Session(port=55707,user_content_storage= "/disk2/support_files/atoti/content_main")#branding=branding_config, name="Rebranded")

file=pd.HDFStore('/disk2/support_files/archive/combined_bsc.h5','r')
df2=file.select('twoG',where='Date>="2020-10-01 00:00"')
df3=file.select('threeG',where='Date>="2020-10-01 00:00"')
df4=file.select('fourG',where='Date>="2020-10-01 00:00"')
df4_2=file.select('fourGn',where='Date>="2020-10-01 00:00"')
#df4_2_2=file.select('fourGnn',where='Date>="2020-10-01 00:00"')
file.close()

#traf=[]
#for i in glob.glob('/disk2/support_files/archive/core/*.h5'):
#	traf.append(pd.read_hdf(i,'traf'))
#core_traf=pd.concat(traf)
#core_traf.rename(columns={'2G/3G DL':'2G_3G DL','2G/3G UL':'2G_3G UL'},inplace=True)

### prepare tables
df2.rename(columns={'ps_traffic_mb':'ps_traf','cs_traffic_erl':'cs_traf'},inplace=True)
df2=df2[['Date','Region','cell_avail_den', 'cell_avail_num','ps_traf','cs_traf','cell_avail_blck_num',
	'cell_avail_blck_den','cssr_num1','cssr_den1','cssr_num2','cssr_den2','cssr_num3','cssr_den3','drop_rate_num','drop_rate_den',
	'comb_thrp_num','comb_thrp_den','tbf_est_sr_num','tbf_est_sr_den','tbf_drop_rate_den']]
df2.columns=[*df2.columns[:2],*df2.columns[2:].map(lambda x: '2G_'+x)]
df2=df2.groupby(['Date','Region'],as_index=False).sum()

df2['Day']=df2['Date'].dt.date
df2['Month']=df2['Date'].dt.month
df2['Year']=df2['Date'].dt.year.astype(str)
df2['Hour']=df2['Date'].dt.hour
df2.dropna(inplace=True)
#df2.to_csv('/disk2/support_files/atoti/bi/2G_first.csv',index=False)

df3.replace(regex=['Qarabagh'],value='Qarabag',inplace=True)
df3=df3[['Date','Region','cell_avail_den', 'cell_avail_num','ps_traf','cs_traf','cell_avail_blck_num',
	'cell_avail_blck_den','voice_sr_num1','voice_sr_den1','voice_sr_num2','voice_sr_den2','voice_dr_num','voice_dr_den',
	'hsdpa_sr_num','hsdpa_sr_den','hsdpa_dr_num','hsdpa_dr_den','hsdpa_thrp_num','hsdpa_thrp_den']]
df3.columns=[*df3.columns[:2],*df3.columns[2:].map(lambda x: '3G_'+x)]
df3=df3.groupby(['Date','Region'],as_index=False).sum()

df3['Day']=df3['Date'].dt.date
df3['Month']=df3['Date'].dt.month
df3['Year']=df3['Date'].dt.year.astype(str)
df3['Hour']=df3['Date'].dt.hour
df3.dropna(inplace=True)
#df3=df3[df3['Date'].isin(list(df3['Date'].unique())[:-3])]
#df3.to_csv('/disk2/support_files/atoti/bi/3G_first.csv',index=False)

df4=df4[['Date','Region','cell_avail_den', 'cell_avail_num','dl_ps_traf','ul_ps_traf','cell_avail_blck_num',
	'cell_avail_blck_den','rrc_sr_num','rrc_sr_den','rab_sr_num','rab_sr_den','csfb_sr_num','csfb_sr_den','dcr_num','dcr_den',
	'dl_thrp_num','dl_thrp_den']]


df4_2.drop(columns='Vendor',inplace=True)

df4.columns=[*df4.columns[:2],*df4.columns[2:].map(lambda x: '4G_'+x)]
df4_2.columns=[*df4_2.columns[:2],*df4_2.columns[2:].map(lambda x: '4G_'+x)]
df4=df4.groupby(['Date','Region'],as_index=False).sum()
df4_2=df4_2.groupby(['Date','Region'],as_index=False).sum()

df4 = df4.merge(df4_2[['Date','Region','4G_volte_dl_ps_traf','4G_volte_ul_ps_traf','4G_volte_sr_num','4G_volte_sr_den','4G_volte_dr_num','4G_volte_dr_den','4G_volte_cs_traf',
				'4G_Voice_DL_Silent_Num','4G_Voice_UL_Silent_Num','4G_volte_srvcc_e2w_num','4G_volte_srvcc_e2w_den',
				'4G_Voice_VQI_DL_Accept_Times','4G_Voice_VQI_DL_Bad_Times','4G_Voice_VQI_DL_Excellent_Times',
                  '4G_Voice_VQI_DL_Good_Times','4G_Voice_VQI_DL_Poor_Times','4G_Voice_VQI_UL_Accept_Times',
                  '4G_Voice_VQI_UL_Bad_Times','4G_Voice_VQI_UL_Excellent_Times','4G_Voice_VQI_UL_Good_Times','4G_Voice_VQI_UL_Poor_Times']],
how='left',on=['Date','Region'])

# for test #######################
#df4_2_2.drop(columns='Vendor',inplace=True)
#df4_2_2['dl_vqi_tot']=df4_2_2['Voice_VQI_DL_Good_Times']+df4_2_2['Voice_VQI_DL_Poor_Times']+df4_2_2['Voice_VQI_DL_Accept_Times']+ \
#	df4_2_2['Voice_VQI_DL_Bad_Times']+df4_2_2['Voice_VQI_DL_Excellent_Times']
#df4_2_2['ul_vqi_tot']=df4_2_2['Voice_VQI_UL_Good_Times']+df4_2_2['Voice_VQI_UL_Poor_Times']+df4_2_2['Voice_VQI_UL_Accept_Times']+ \
#	df4_2_2['Voice_VQI_UL_Bad_Times']+df4_2_2['Voice_VQI_UL_Excellent_Times']
#df4_2_2.columns=[*df4_2_2.columns[:2],*df4_2_2.columns[2:].map(lambda x: '4G_'+x)]
#df4_2_2=df4_2_2.groupby(['Date','Region'],as_index=False).sum()
#df4 = df4.merge(df4_2_2[['Date','Region','4G_Voice_DL_Silent_Num','4G_Voice_UL_Silent_Num','4G_volte_srvcc_e2w_num','4G_volte_srvcc_e2w_den',
#				'4G_Voice_VQI_DL_Accept_Times','4G_Voice_VQI_DL_Bad_Times','4G_Voice_VQI_DL_Excellent_Times',
#                  '4G_Voice_VQI_DL_Good_Times','4G_Voice_VQI_DL_Poor_Times','4G_dl_vqi_tot','4G_Voice_VQI_UL_Accept_Times',
#                  '4G_Voice_VQI_UL_Bad_Times','4G_Voice_VQI_UL_Excellent_Times','4G_Voice_VQI_UL_Good_Times','4G_Voice_VQI_UL_Poor_Times','4G_ul_vqi_tot']],
#how='left',on=['Date','Region'])
######################################

df4['Day']=df4['Date'].dt.date
df4['Month']=df4['Date'].dt.month
df4['Year']=df4['Date'].dt.year.astype(str)
df4['Hour']=df4['Date'].dt.hour
df4.fillna(0,inplace=True)
#df4.to_csv('/disk2/support_files/atoti/bi/4G_first.csv',index=False)

#two=session.read_csv("/disk2/support_files/atoti/bi/2G_first.csv",keys=['Date','Region'],table_name="KPI_table_2G")
#three=session.read_csv("/disk2/support_files/atoti/bi/3G_first.csv",keys=['Date','Region'],table_name="KPI_table_3G")
#four=session.read_csv("/disk2/support_files/atoti/bi/4G_first.csv",keys=['Date','Region'],table_name="KPI_table_4G")

two = session.read_pandas(df2,table_name='KPI_table_2G',keys=['Date','Region'])
three = session.read_pandas(df3,table_name='KPI_table_3G',keys=['Date','Region'])
four = session.read_pandas(df4,table_name='KPI_table_4G',keys=['Date','Region'])

# join tables
three.join(two, mapping={'Date':'Date','Region':'Region','Day':'Day','Month':'Month','Year':'Year'})
three.join(four, mapping={'Date':'Date','Region':'Region','Day':'Day','Month':'Month','Year':'Year'})
	
#core = session.read_pandas(core_traf,table_name='Core_traffic',keys=['Date'])
# create cube and add measures
cube=session.create_cube(three,mode='no_measures')
#cube2=session.create_cube(core)
h,l,m = cube.hierarchies, cube.levels, cube.measures
three['Month'].default_value=1
three['Hour'].default_value=22
h['Month']=[three['Month']]
h['Year']=[three['Year']]
h['Hour']=[three['Hour']]
#h['Date_hierarchy']=[three['Year'],three['Month'],three['Day']]
cube.create_date_hierarchy("Date_hierarchy",column=three['Date'],levels={'Year':'yyyy',"Month": "MM",'Day':'dd'})
#cube.create_date_hierarchy("Month",column=three['Date'],levels={"Month": "MM"})
#cube.create_date_hierarchy("Year",column=three['Date'],levels={"Year": "yyyy"})
#cube.create_date_hierarchy("Day",column=three['Date'],levels={"Day": "dd"})


#tt.experimental.create_date_hierarchy("Date_hierarchy", cube=cube, column=three["Date"], levels={"Month": "b", "Year": "YYYY",'Day':'dd'})
def add_measures():
#	for i in m.keys():
#		del m[i]

	m['2G_Availability']=(tt.agg.sum(two['2G_cell_avail_num'])+tt.agg.sum(two['2G_cell_avail_blck_num']))/ \
	(tt.agg.sum(two['2G_cell_avail_den'])-tt.agg.sum(two['2G_cell_avail_blck_den']))*100 
	m['3G_Availability']=(tt.agg.sum(three['3G_cell_avail_num'])+tt.agg.sum(three['3G_cell_avail_blck_num']))/ \
		(tt.agg.sum(three['3G_cell_avail_den'])-tt.agg.sum(three['3G_cell_avail_blck_den']))*100
	m['4G_Availability']=(tt.agg.sum(four['4G_cell_avail_num'])+tt.agg.sum(four['4G_cell_avail_blck_num']))/  \
		(tt.agg.sum(four['4G_cell_avail_den'])-tt.agg.sum(four['4G_cell_avail_blck_den']))*100
	m['Total_Availability']=(tt.agg.sum(four['4G_cell_avail_num'])+tt.agg.sum(four['4G_cell_avail_blck_num'])+ \
							tt.agg.sum(two['2G_cell_avail_num'])+tt.agg.sum(two['2G_cell_avail_blck_num'])+ \
							tt.agg.sum(three['3G_cell_avail_num'])+tt.agg.sum(three['3G_cell_avail_blck_num']))/  \
		(tt.agg.sum(four['4G_cell_avail_den'])-tt.agg.sum(four['4G_cell_avail_blck_den'])+ \
		tt.agg.sum(two['2G_cell_avail_den'])-tt.agg.sum(two['2G_cell_avail_blck_den']) + \
		tt.agg.sum(three['3G_cell_avail_den'])-tt.agg.sum(three['3G_cell_avail_blck_den']))*100


	m['2G_Voice_Traffic_Erl']=tt.agg.sum(two['2G_cs_traf'])
	m['3G_Voice_Traffic_Erl']=tt.agg.sum(three['3G_cs_traf'])
	m['Total_Voice_Traffic_KErl']=(tt.agg.sum(two['2G_cs_traf'])+tt.agg.sum(three['3G_cs_traf'])+tt.agg.sum(four['4G_volte_cs_traf']))/1000

	m['2G_Data_Traffic_GB']=tt.agg.sum(two['2G_ps_traf'])/1024
	m['3G_Data_Traffic_GB']=tt.agg.sum(three['3G_ps_traf'])/1024
	m['4G_Data_Traffic_GB']=(tt.agg.sum(four['4G_dl_ps_traf'])+tt.agg.sum(four['4G_ul_ps_traf']))/1024
	m['Total_Data_Traffic_TB']=(m['2G_Data_Traffic_GB']+m['3G_Data_Traffic_GB']+m['4G_Data_Traffic_GB'])/1024

	m['Region_Data_share']=m["Total_Data_Traffic_TB"] / tt.parent_value(m["Total_Data_Traffic_TB"],degrees={h["Region"]: 1},apply_filters=True)
	m['Region_Voice_share']=m["Total_Voice_Traffic_KErl"] / tt.parent_value(m["Total_Voice_Traffic_KErl"],degrees={h["Region"]: 1},apply_filters=True)
	m['Region_Data_share'].formatter="DOUBLE[0.00%]"
	m['Region_Voice_share'].formatter="DOUBLE[0.00%]"

	m['3G_Voice_share']=(tt.agg.sum(three['3G_cs_traf'])/(tt.agg.sum(three['3G_cs_traf'])+tt.agg.sum(two['2G_cs_traf'])))
	m['4G_Data_share']=m['4G_Data_Traffic_GB']/(m['Total_Data_Traffic_TB']*1024)
	m['4G_Data_share'].formatter="DOUBLE[0.00%]"
	m['3G_Voice_share'].formatter="DOUBLE[0.00%]"

	m['2G_Call_Setup_SR']=(1-(tt.agg.sum(two['2G_cssr_num1'])/tt.agg.sum(two['2G_cssr_den1'])))* \
						(tt.agg.sum(two['2G_cssr_num2'])/tt.agg.sum(two['2G_cssr_den2'])) * \
						(tt.agg.sum(two['2G_cssr_num3'])/tt.agg.sum(two['2G_cssr_den3']))*100
	m['3G_Call_Setup_SR']=(tt.agg.sum(three['3G_voice_sr_num1'])/tt.agg.sum(three['3G_voice_sr_den1'])) * \
							(tt.agg.sum(three['3G_voice_sr_num2'])/tt.agg.sum(three['3G_voice_sr_den2']))*100
	m['Total_Call_Setup_SR']=(m['2G_Call_Setup_SR']*(1-m['3G_Voice_share'])+m['3G_Call_Setup_SR']*m['3G_Voice_share'])
						
	m['2G_Call_DR']=tt.agg.sum(two['2G_drop_rate_num'])/tt.agg.sum(two['2G_drop_rate_den'])*100
	m['3G_Call_DR']=tt.agg.sum(three['3G_voice_dr_num'])/tt.agg.sum(three['3G_voice_dr_den'])*100
	m['Total_Call_DR']=(m['2G_Call_DR']*(1-m['3G_Voice_share'])+m['3G_Call_DR']*m['3G_Voice_share'])
	m['2G_Call_DR'].formatter="DOUBLE[0.00]"
	m['3G_Call_DR'].formatter="DOUBLE[0.00]"
	m['Total_Call_DR'].formatter="DOUBLE[0.00]"


	m['4G_Data_Setup_SR']=(tt.agg.sum(four['4G_rrc_sr_num'])/tt.agg.sum(four['4G_rrc_sr_den'])) * \
						(tt.agg.sum(four['4G_rab_sr_num'])/tt.agg.sum(four['4G_rab_sr_den']))*100
	m['3G_Data_Setup_SR']=tt.agg.sum(three['3G_hsdpa_sr_num'])/tt.agg.sum(three['3G_hsdpa_sr_den'])*100
	m['Total_Data_Setup_SR']=(m['3G_Data_Setup_SR']*(1-m['4G_Data_share'])+m['4G_Data_Setup_SR']*m['4G_Data_share'])

	m['4G_Data_DR']=tt.agg.sum(four['4G_dcr_num'])/tt.agg.sum(four['4G_dcr_den'])*100
	m['3G_Data_DR']=tt.agg.sum(three['3G_hsdpa_dr_num'])/tt.agg.sum(three['3G_hsdpa_dr_den'])*100
	m['Total_Data_DR']=(m['3G_Data_DR']*(1-m['4G_Data_share'])+m['4G_Data_DR']*m['4G_Data_share'])/100
	m['4G_Data_DR'].formatter="DOUBLE[0.00]"
	m['3G_Data_DR'].formatter="DOUBLE[0.00]"
	m['Total_Data_DR'].formatter="DOUBLE[0.00]"

	m['4G_DL_thrp(Mbps)']=tt.agg.sum(four['4G_dl_thrp_num'])/tt.agg.sum(four['4G_dl_thrp_den'])/1024
	m['3G_DL_thrp(Mbps)']=tt.agg.sum(three['3G_hsdpa_thrp_num'])/tt.agg.sum(three['3G_hsdpa_thrp_den'])/1024
	m['Total_DL_thrp(Mbps)']=(m['3G_DL_thrp(Mbps)']*(1-m['4G_Data_share'])+m['4G_DL_thrp(Mbps)']*m['4G_Data_share'])
	
	m['VoLTE_Data_Traffic_GB']=(tt.agg.sum(four['4G_volte_dl_ps_traf'])+tt.agg.sum(four['4G_volte_ul_ps_traf']))/1024
	m['VoLTE_Voice_Traffic_Erl']=tt.agg.sum(four['4G_volte_cs_traf'])
	m['VoLTE_Call_Setup_SR']=(tt.agg.sum(four['4G_volte_sr_num'])/tt.agg.sum(four['4G_volte_sr_den']))*100
	m['VoLTE_Call_DR']=(tt.agg.sum(four['4G_volte_dr_num'])/tt.agg.sum(four['4G_volte_dr_den']))*100
	m['VoLTE_Call_DR'].formatter="DOUBLE[0.00]"

	m['VoLTE_Voice_share']=tt.agg.sum(four['4G_volte_cs_traf'])/(tt.agg.sum(three['3G_cs_traf'])+tt.agg.sum(two['2G_cs_traf'])+tt.agg.sum(four['4G_volte_cs_traf']))
	m['VoLTE_Voice_share'].formatter="DOUBLE[0.00%]"
	m['Silent_call_dl']= tt.agg.sum(four['4G_Voice_DL_Silent_Num'])
	m['Silten_call_ul']= tt.agg.sum(four['4G_Voice_UL_Silent_Num'])
	m['VoLTE_SRVCC_SR']= (tt.agg.sum(four['4G_volte_srvcc_e2w_num'])/tt.agg.sum(four['4G_volte_srvcc_e2w_den']))*100

	m['VoLTE_dl_vqi_tot']=tt.agg.sum(four['4G_Voice_VQI_DL_Good_Times'])+tt.agg.sum(four['4G_Voice_VQI_DL_Poor_Times'])+tt.agg.sum(four['4G_Voice_VQI_DL_Accept_Times'])\
							+tt.agg.sum(four['4G_Voice_VQI_DL_Bad_Times'])+tt.agg.sum(four['4G_Voice_VQI_DL_Excellent_Times'])
	m['VoLTE_ul_vqi_tot']=tt.agg.sum(four['4G_Voice_VQI_UL_Good_Times'])+tt.agg.sum(four['4G_Voice_VQI_UL_Poor_Times'])+tt.agg.sum(four['4G_Voice_VQI_UL_Accept_Times'])\
							+tt.agg.sum(four['4G_Voice_VQI_UL_Bad_Times'])+tt.agg.sum(four['4G_Voice_VQI_UL_Excellent_Times'])
	m['VoLTE_DL_VQI(Voice_Quality_Index)'] = (tt.agg.sum(four['4G_Voice_VQI_DL_Excellent_Times'])/m['VoLTE_dl_vqi_tot'])*5 + \
						(tt.agg.sum(four['4G_Voice_VQI_DL_Good_Times'])/m['VoLTE_dl_vqi_tot'])*4 + \
						(tt.agg.sum(four['4G_Voice_VQI_DL_Accept_Times'])/m['VoLTE_dl_vqi_tot'])*3 + \
						(tt.agg.sum(four['4G_Voice_VQI_DL_Poor_Times'])/m['VoLTE_dl_vqi_tot'])*2 + \
						(tt.agg.sum(four['4G_Voice_VQI_DL_Bad_Times'])/m['VoLTE_dl_vqi_tot'])*1
	m['VoLTE_UL_VQI(Voice_Quality_Index)'] = (tt.agg.sum(four['4G_Voice_VQI_UL_Excellent_Times'])/m['VoLTE_ul_vqi_tot'])*5 + \
						(tt.agg.sum(four['4G_Voice_VQI_UL_Good_Times'])/m['VoLTE_ul_vqi_tot'])*4 + \
						(tt.agg.sum(four['4G_Voice_VQI_UL_Accept_Times'])/m['VoLTE_ul_vqi_tot'])*3 + \
						(tt.agg.sum(four['4G_Voice_VQI_UL_Poor_Times'])/m['VoLTE_ul_vqi_tot'])*2 + \
						(tt.agg.sum(four['4G_Voice_VQI_UL_Bad_Times'])/m['VoLTE_ul_vqi_tot'])*1	
	m['VoLTE_Total_VQI(Voice_Quality_Index)'] = ((tt.agg.sum(four['4G_Voice_VQI_DL_Excellent_Times'])+tt.agg.sum(four['4G_Voice_VQI_UL_Excellent_Times']))/ \
						(m['VoLTE_ul_vqi_tot']+m['VoLTE_dl_vqi_tot']))*5 + \
						((tt.agg.sum(four['4G_Voice_VQI_DL_Good_Times'])+tt.agg.sum(four['4G_Voice_VQI_UL_Good_Times']))/ \
						(m['VoLTE_ul_vqi_tot']+m['VoLTE_dl_vqi_tot']))*4 + \
						((tt.agg.sum(four['4G_Voice_VQI_DL_Accept_Times'])+tt.agg.sum(four['4G_Voice_VQI_UL_Accept_Times']))/ \
						(m['VoLTE_ul_vqi_tot']+m['VoLTE_dl_vqi_tot']))*3 + \
						((tt.agg.sum(four['4G_Voice_VQI_DL_Poor_Times'])+tt.agg.sum(four['4G_Voice_VQI_UL_Poor_Times']))/ \
						(m['VoLTE_ul_vqi_tot']+m['VoLTE_dl_vqi_tot']))*2 + \
						((tt.agg.sum(four['4G_Voice_VQI_DL_Bad_Times'])+tt.agg.sum(four['4G_Voice_VQI_UL_Bad_Times']))/ \
						(m['VoLTE_ul_vqi_tot']+m['VoLTE_dl_vqi_tot']))*1
	m['VoLTE_DL_silent_calls'] = tt.agg.sum(four['4G_Voice_DL_Silent_Num'])
	m['VoLTE_UL_silent_calls'] = tt.agg.sum(four['4G_Voice_UL_Silent_Num'])

add_measures()				

import numpy as np

df_core=pd.read_csv('/home/ismayil/Documents/core_ready.csv')
df_core.drop_duplicates(inplace=True)
df_core=df_core[df_core['Date']!='Date']
df_core['Date']=pd.to_datetime(df_core['Date'])
a=['Site', 'HO_type', 'Entity_name', 'Direction', 'Type', 'Identifier', 'mode', 'Region', 'MNO', 'BSC_RNC'] #'APN',
#
df_core[a]=df_core[a].fillna('All')
#df_core['LAC']=df_core['LAC'].replace({'All':100}).astype(int)
#df_core['RAC']=df_core['RAC'].replace({'All':100}).astype(float)
df_core['Day']=df_core['Date'].dt.date
df_core['Month']=df_core['Date'].dt.month
df_core['Year']=df_core['Date'].dt.year.astype(str)
df_core['Hour']=df_core['Date'].dt.hour
#
df_core.columns=df_core.columns.map(lambda x: x.replace('/','_'))
#
#session=tt.Session(port=55707)
#
a.append('Date')
a.append('Day')
a.append('Month')
a.append('Year')
a.append('Hour')
b=[i for i in df_core.columns if i not in a]
df_core[b]=df_core[b].astype(float)
core = session.read_pandas(df_core,table_name='Core_KPIs',keys=a)
#
cube_core=session.create_cube(core,mode='no_measures')
#cube2=session.create_cube(core)
h1,l1,m1 = cube_core.hierarchies, cube_core.levels, cube_core.measures
#core['Month'].default_value=2
#core['Hour'].default_value=22
h1['Month']=[core['Month']]
h1['Year']=[core['Year']]
h1['Hour']=[core['Hour']]
#l['LAC']=[core['LAC']]
#h['Date_hierarchy']=[three['Year'],three['Month'],three['Day']]
cube_core.create_date_hierarchy("Date_hierarchy",column=core['Date'],levels={'Year':'yyyy',"Month": "MM",'Day':'dd'})
#
def add_core_measures():
    try:
        m1['2G_Call_Completion_SR']=tt.agg.sum(core['TwoG Call Completion'])/tt.agg.sum(core['TwoG Call Attempt'])*100
        m1['3G_Call_Completion_SR']=tt.agg.sum(core['ThreeG Call Completion'])/tt.agg.sum(core['ThreeG Call Attempt'])*100
        m1['2G_3G_Call_Completion_SR']=(tt.agg.sum(core['TwoG Call Completion'])+tt.agg.sum(core['ThreeG Call Completion']))/\
                                        (tt.agg.sum(core['TwoG Call Attempt'])+tt.agg.sum(core['ThreeG Call Attempt']))*100
        m1['2G_Call_attempt']=tt.agg.sum(core['TwoG Call Attempt'])
        m1['3G_Call_attempt']=tt.agg.sum(core['ThreeG Call Attempt'])
    #    
        m1['2G_Call_Setup_Time_sec']=tt.agg.sum(core['TwoG Call Setup time'])/tt.agg.sum(core['TwoG Call Completion'])
        m1['3G_Call_Setup_Time_sec']=tt.agg.sum(core['ThreeG Call Setup time'])/tt.agg.sum(core['ThreeG Call Completion'])
        m1['2G_3G_Call_Setup_Time_sec']=(tt.agg.sum(core['TwoG Call Setup time'])+tt.agg.sum(core['ThreeG Call Setup time']))/\
                                            (tt.agg.sum(core['TwoG Call Completion'])+tt.agg.sum(core['ThreeG Call Completion']))
    #   
        m1['2G_pag_att']=tt.agg.sum(core['TwoG_CS Paging SR den'])
        m1['2G_pag_suc']=tt.agg.sum(core['TwoG_CS Paging SR num'])
        m1['3G_pag_att']=tt.agg.sum(core['ThreeG_CS Paging SR den'])
        m1['3G_pag_suc']=tt.agg.sum(core['ThreeG_CS Paging SR num'])
        #m1['2G_CS_Paging_SR']=tt.filter(m1['2G_pag_suc'],(l1['LAC']!=1505) & (l1['LAC']!=100))/tt.filter(m1['2G_pag_att'],(l1['LAC']!=1505) & (l1['LAC']!=100))*100
        #m1['3G_CS_Paging_SR']=tt.filter(m1['3G_pag_suc'],(l1['LAC']!=1505) & (l1['LAC']!=100))/tt.filter(m1['3G_pag_att'],(l1['LAC']!=1505) & (l1['LAC']!=100))*100
        m1['2G_3G_CS_Paging_SR']=(tt.agg.sum(core['TwoG_CS Paging SR num'])+tt.agg.sum(core['ThreeG_CS Paging SR num']))/\
                                (tt.agg.sum(core['TwoG_CS Paging SR den'])+tt.agg.sum(core['ThreeG_CS Paging SR den']))*100
        #m1['CommonLac_CS_Paging_SR']=(tt.filter(m1['2G_pag_suc'],l1['LAC']==1505)+tt.filter(m1['3G_pag_suc'],l1['LAC']==1505))/tt.filter(m1['3G_pag_att'],l1['LAC']==1505)*100
        #
        m1['Location_Update_SR_VLR']=tt.agg.sum(core['VLR Location Update Success'])/tt.agg.sum(core['VLR Location Update Requests'])*100
        m1['Location_Update_SR_Roaming']=tt.agg.sum(core['Roaming Location Update Success'])/tt.agg.sum(core['Roaming Location Update Requests'])*100
        m1['Location_Update_SR_total']=(tt.agg.sum(core['VLR Location Update Success'])+tt.agg.sum(core['Roaming Location Update Success']))/\
                                        (tt.agg.sum(core['VLR Location Update Requests'])+tt.agg.sum(core['Roaming Location Update Requests']))*100
        m1['VLR_Location_update_request']=tt.agg.sum(core['VLR Location Update Requests'])
        m1['Roaming_Location_update_request']=tt.agg.sum(core['Roaming Location Update Requests'])
        #
        m1['MO_SMS_SR']=tt.agg.sum(core['SMS MO Success'])/tt.agg.sum(core['SMS MO Attempt'])*100
        m1['MT_SMS_SR']=tt.agg.sum(core['SMS MT Success'])/tt.agg.sum(core['SMS MT Attempt'])*100
        m1['Authentication_SR']=tt.agg.sum(core['Auth_success'])/tt.agg.sum(core['Auth_requests'])*100
        m1['MO_SMS_attempt']=tt.agg.sum(core['SMS MO Attempt'])
        m1['MT_SMS_attempt']=tt.agg.sum(core['SMS MT Attempt'])
        m1['Auth_request']=tt.agg.sum(core['Auth_requests'])
        #
        m1['Intra_MSC_HOSR']=tt.agg.sum(core['Intra_MSC_HO_success'])/tt.agg.sum(core['Intra_MSC_HO_request'])*100
        m1['CSFB_Paging_SR']=tt.agg.sum(core['Paging response'])/(tt.agg.sum(core['CSFB First paging request'])+tt.agg.sum(core['CSFB Second paging request'])+\
                                                                    tt.agg.sum(core['CSFB Third paging request']))*100
        m1['CSFB_1st_Paging_SR']=tt.agg.sum(core['Paging response'])/tt.agg.sum(core['CSFB First paging request'])*100
        m1['Intra_MSC_HO_request']=tt.agg.sum(core['Intra_MSC_HO_request'])
        m1['CSFB_Paging_request']=(tt.agg.sum(core['CSFB First paging request'])+tt.agg.sum(core['CSFB Second paging request'])+tt.agg.sum(core['CSFB Third paging request']))
        for element in [m1['2G_Call_Completion_SR'],m1['3G_Call_Completion_SR'],m1['2G_3G_Call_Completion_SR'],m1['2G_Call_Setup_Time_sec'],m1['3G_Call_Setup_Time_sec'],
                        m1['2G_3G_Call_Setup_Time_sec'],m1['2G_3G_CS_Paging_SR'],#m1['2G_CS_Paging_SR'],m1['3G_CS_Paging_SR'],m1['CommonLac_CS_Paging_SR'],
                        m1['Location_Update_SR_VLR'],m1['Location_Update_SR_Roaming'],m1['Location_Update_SR_total'],m1['MO_SMS_SR'],m1['MT_SMS_SR'],
                        m1['Authentication_SR'],m1['Intra_MSC_HOSR'],m1['CSFB_Paging_SR'],m1['CSFB_1st_Paging_SR'],m1['2G_pag_att'],m1['3G_pag_att'],
                        m1['2G_pag_suc'],m1['3G_pag_suc'],m1['2G_Call_attempt'],m1['3G_Call_attempt'],m1['VLR_Location_update_request'],
                        m1['Roaming_Location_update_request'],m1['MO_SMS_attempt'],m1['MT_SMS_attempt'],m1['Auth_request'],m1['Intra_MSC_HO_request'],
                        m1['CSFB_Paging_request']]:
                element.folder='Voice KPIs'
        m1['Data_Attach_SR']=tt.agg.sum(core['Data accept'])/tt.agg.sum(core['Data attach'])*100
        m1['Inter_SGSN_MME_RAU_SR']=tt.agg.sum(core['Inter_SGSN_MME RAU success'])/tt.agg.sum(core['Inter_SGSN_MME RAU request'])*100
        m1['Intra_SGSN_MME_RAU_SR']=tt.agg.sum(core['Intra_SGSN_MME RAU success'])/tt.agg.sum(core['Intra_SGSN_MME RAU request'])*100
        m1['PS_Paging_SR']=(1-tt.agg.sum(core['packet paging none_response'])/tt.agg.sum(core['packet paging request']))*100
        m1['Initial_PDP_Context_Activation_SR']=tt.agg.sum(core['MS init PDP_bear context act suc'])/tt.agg.sum(core['MS init PDP_bear context act'])*100
        m1['PDP_SR']=tt.agg.sum(core['PDP success'])/tt.agg.sum(core['PDP request'])*100
        m1['Data_attach']=tt.agg.sum(core['Data attach'])
        m1['Inter_SGSN_MME_RAU_request']=tt.agg.sum(core['Inter_SGSN_MME RAU request'])
        m1['Intra_SGSN_MME_RAU_request']=tt.agg.sum(core['Intra_SGSN_MME RAU request'])
        m1['PS_paging_request']=tt.agg.sum(core['packet paging request'])
        m1['Initial_PDP_Context_Activation_request']=tt.agg.sum(core['MS init PDP_bear context act'])
        m1['PDP_request']=tt.agg.sum(core['PDP request'])
        for element in [m1['Data_Attach_SR'],m1['Inter_SGSN_MME_RAU_SR'],m1['Intra_SGSN_MME_RAU_SR'],m1['PS_Paging_SR'],m1['Initial_PDP_Context_Activation_SR'],
                        m1['PDP_SR'],m1['Data_attach'],m1['Inter_SGSN_MME_RAU_request'],m1['Intra_SGSN_MME_RAU_request'],m1['PS_paging_request'],
                        m1['Initial_PDP_Context_Activation_request'],m1['PDP_request']]:
            element.folder='Data KPIs'
        #
        m1['Local_VLR_Attach_subscribers']=tt.agg.sum(core['VLR IMSI attached Subscribers'])-tt.agg.sum(core['VLR IMSI Attached Inter Roam Subs'])-\
                                            tt.agg.sum(core['VLR IMSI Attached National Roam Subs'])
        m1['Local_2G_subscribers']=tt.agg.sum(core['VLR 2G LAI Subs'])-tt.agg.sum(core['International Roam 2G LAI Subs'])-tt.agg.sum(core['National Roam 2G LAI Subs'])
        m1['Local_3G_subscribers']=tt.agg.sum(core['VLR 3G LAI Subs'])-tt.agg.sum(core['International Roam 3G LAI Subs'])-tt.agg.sum(core['National Roam 3G LAI Subs'])
        m1['VLR_Total_Subscribers']=tt.agg.sum(core['VLR Total Subscribers'])
        m1['VLR_2G_LAI_Subs']=tt.agg.sum(core['VLR 2G LAI Subs'])
        m1['VLR_3G_LAI_Subs']=tt.agg.sum(core['VLR 3G LAI Subs'])
        m1['VLR_IMSI_attached_Subs']=tt.agg.sum(core['VLR IMSI attached Subscribers'])
        m1['IMSI_Attached_2G_LAI_Subs']=tt.agg.sum(core['IMSI Attached 2G LAI Subs'])
        m1['IMSI_Attached_3G_LAI_Subs']=tt.agg.sum(core['IMSI Attached 3G LAI Subs'])
        m1['VLR_Local_Subs']=tt.agg.sum(core['VLR Local Subs'])
        m1['VLR_International_Roam_Subs']=tt.agg.sum(core['VLR International Roam Subs'])
        m1['International_Roam_2G_LAI_Subs']=tt.agg.sum(core['International Roam 2G LAI Subs'])
        m1['International_Roam_3G_LAI_Subs']=tt.agg.sum(core['International Roam 3G LAI Subs'])
        m1['VLR_IMSI_Attached_Inter_Roam_Subs']=tt.agg.sum(core['VLR IMSI Attached Inter Roam Subs'])
        m1['IMSI_Attached_Inter_Roam_2G_LAI_Subs']=tt.agg.sum(core['IMSI Attached International Roam 2G LAI Subs'])
        m1['IMSI_Attached_Inter_Roam_3G_LAI_Subs']=tt.agg.sum(core['IMSI Attached International Roam 3G LAI Subs'])
        m1['National_Roam_2G_LAI_Subs']=tt.agg.sum(core['National Roam 2G LAI Subs'])
        m1['National_Roam_3G_LAI_Subs']=tt.agg.sum(core['National Roam 3G LAI Subs'])
        m1['VLR_National_Roam_Subs']=tt.agg.sum(core['VLR National Roam Subs'])
        m1['VLR_IMSI_Attached_National_Roam_Subs']=tt.agg.sum(core['VLR IMSI Attached National Roam Subs'])
        m1['IMSI_Attached_National_Roam_2G_LAI_Subs']=tt.agg.sum(core['IMSI Attached National Roam 2G LAI Subs'])
        m1['IMSI_Attached_National_Roam_3G_LAI_Subs']=tt.agg.sum(core['IMSI Attached National Roam 3G LAI Subs'])
        m1['EPS_bearers']=tt.agg.sum(core['Max_simult_act_PGW_EPS_bearer'])+tt.agg.sum(core['Max_simult_act_S_PGW_EPS_bearer'])
        m1['2G_3G_bearers']=tt.agg.sum(core['Max_active_utran_bearers'])+tt.agg.sum(core['Max_active_geran_bearers'])
        m1['2G_bearers']=tt.agg.sum(core['Max_active_geran_bearers'])
        m1['3G_bearers']=tt.agg.sum(core['Max_active_utran_bearers'])
        m1['4G_bearers']=tt.agg.sum(core['Max_active_eutran_bearers'])
        m1['Max_simult_activ_PDP_ctx']=tt.agg.sum(core['Max_simult_act_PDP_context'])
        m1['Total_bearers']=m1['EPS_bearers']+m1['Max_simult_activ_PDP_ctx']+m1['3G_bearers']+m1['2G_bearers']+m1['4G_bearers']
        m1['Gb_mode_max_attached_user']=tt.agg.sum(core['Gb mode max attached user'])
        m1['Iu_mode_max_attached_user']=tt.agg.sum(core['Iu mode max attached user'])
        m1['S1_mode_max_attached_user']=tt.agg.sum(core['S1 mode max attached user'])
        m1['Total_attach_user']=m1['Gb_mode_max_attached_user']+m1['Iu_mode_max_attached_user']+m1['S1_mode_max_attached_user']
        m1['Gb_mode_max_user_with_pdp_activated_ctx']=tt.agg.sum(core['Gb mode max user with pdp activated context'])
        m1['Iu_mode_max_user_with_pdp_activated_ctx']=tt.agg.sum(core['Iu mode max user with pdp activated context'])
        m1['S1_mode_max_user_with_pdn_connection']=tt.agg.sum(core['S1 mode max PDN connection num'])
        m1['Total_user_with_pdp_activated_ctx']=m1['Gb_mode_max_user_with_pdp_activated_ctx']+m1['Iu_mode_max_user_with_pdp_activated_ctx']+m1['S1_mode_max_user_with_pdn_connection']
        for element in [m1['Local_VLR_Attach_subscribers'],m1['Local_2G_subscribers'],m1['Local_3G_subscribers'],m1['VLR_Total_Subscribers'],m1['VLR_2G_LAI_Subs'],
                        m1['VLR_3G_LAI_Subs'],m1['VLR_IMSI_attached_Subs'],m1['IMSI_Attached_2G_LAI_Subs'],m1['IMSI_Attached_3G_LAI_Subs'],m1['VLR_Local_Subs'],
                        m1['VLR_International_Roam_Subs'],m1['International_Roam_2G_LAI_Subs'],m1['International_Roam_3G_LAI_Subs'],
                        m1['VLR_IMSI_Attached_Inter_Roam_Subs'],m1['IMSI_Attached_Inter_Roam_2G_LAI_Subs'],m1['IMSI_Attached_Inter_Roam_3G_LAI_Subs'],
                        m1['National_Roam_2G_LAI_Subs'],m1['National_Roam_3G_LAI_Subs'],m1['VLR_National_Roam_Subs'],m1['VLR_IMSI_Attached_National_Roam_Subs'],
                        m1['IMSI_Attached_National_Roam_2G_LAI_Subs'],m1['IMSI_Attached_National_Roam_3G_LAI_Subs'],m1['EPS_bearers'],m1['2G_3G_bearers'],
                        m1['2G_bearers'],m1['3G_bearers'],m1['4G_bearers'],m1['Max_simult_activ_PDP_ctx'],m1['Total_bearers'],m1['Gb_mode_max_attached_user'],
                        m1['Iu_mode_max_attached_user'],m1['S1_mode_max_attached_user'],m1['Total_attach_user'],m1['Gb_mode_max_user_with_pdp_activated_ctx'],
                        m1['Iu_mode_max_user_with_pdp_activated_ctx'],m1['S1_mode_max_user_with_pdn_connection'],m1['Total_user_with_pdp_activated_ctx']]:
             element.folder='Users'
        #
        m1['VoLTE_MO_Call_Completion_SR']=tt.agg.sum(core['MO Connected'])/tt.agg.sum(core['MO Attempts'])*100
        m1['VoLTE_MT_Call_Completion_SR']=tt.agg.sum(core['MT Connected'])/tt.agg.sum(core['MT Attempts'])*100
        m1['VoLTE_MO_Call_Answer_Rate']=tt.agg.sum(core['MO Answered'])/tt.agg.sum(core['MO Attempts'])*100
        m1['VoLTE_MT_Call_Answer_Rate']=tt.agg.sum(core['MT Answered'])/tt.agg.sum(core['MT Attempts'])*100
        m1['VoLTE_MO_Call_Interrupt_Rate']=tt.agg.sum(core['MO Interruptions'])/tt.agg.sum(core['MO Connected'])*100
        m1['VoLTE_MT_Call_Interrupt_Rate']=tt.agg.sum(core['MT Interruptions'])/tt.agg.sum(core['MT Connected'])*100
        m1['VoLTE_MO_Call_setup_time_sec']=tt.agg.sum(core['Mean duration of connected MO session_ms'])/1000
        m1['VoLTE_MT_Call_setup_time_sec']=tt.agg.sum(core['Mean duration of connected MT session_ms'])/1000
        m1['VoLTE_BHCA_per_subs']=tt.agg.sum(core['BHCA per Subscriber'])
        m1['VoLTE_Mean_duration_of_MO_calls_sec']=tt.agg.sum(core['Mean duration of MO seized sessions_sec'])
        m1['VoLTE_Mean_duration_of_MT_calls_sec']=tt.agg.sum(core['Mean duration of MT seized sessions_sec'])
        m1['VoLTE_peak_simultaneous_sessions']=tt.agg.sum(core['Peak num of ATS simultaneous sessions'])
        m1['VoLTE_subscribers']=tt.agg.sum(core['ATS VoLTE registered subs'])
        m1['VoLTE_Initial_Registration_SR']=tt.agg.sum(core['Initial register success'])/tt.agg.sum(core['Initial register attempt'])*100
        m1['VoLTE_Re_registration_SR']=tt.agg.sum(core['Re-registration success'])/tt.agg.sum(core['Re-registration attempt'])*100
        m1['VoLTE_Deregistration_SR']=tt.agg.sum(core['Deregistration success'])/tt.agg.sum(core['Deregistration attempt'])*100
        m1['VoLTE_Notify_messages_for_Reg_SR']=tt.agg.sum(core['Notify Messages for Reg num'])/tt.agg.sum(core['Notify Messages for Reg den'])*100
        m1['VoLTE_Subscribe_requests_for_Reg_SR']=tt.agg.sum(core['Subscribe Requests for Registration num'])/tt.agg.sum(core['Subscribe Requests for Registration den'])*100
        m1['VoLTE_Third_party_Deregistrations_SR']=tt.agg.sum(core['Third_party deregistration success'])/tt.agg.sum(core['Third_party deregistration attempt'])*100
        m1['VoLTE_Third_party_Registrations_SR']=tt.agg.sum(core['Third_party registration success'])/(tt.agg.sum(core['Third_party registration success'])+\
                                                                                                        tt.agg.sum(core['Third_party registration unsuccess']))*100
        m1['VoLTE_Authentication_SR']=tt.agg.sum(core['Authentication success'])/tt.agg.sum(core['Authentication attempt'])*100
        m1['VoLTE_MO_Call_Drop_Rate']=tt.agg.sum(core['MO Call Drop'])/tt.agg.sum(core['MO Answered'])*100
        m1['VoLTE_MT_Call_Drop_Rate']=tt.agg.sum(core['MT Call Drop'])/tt.agg.sum(core['MT Answered'])*100
        m1['LTE_to_GSM_SRVCC_HOSR']=tt.agg.sum(core['LTE_to_GSM SRVCC num'])/tt.agg.sum(core['LTE_to_GSM SRVCC den'])*100
        m1['LTE_to_UMTS_SRVCC_HOSR']=tt.agg.sum(core['LTE_to_UMTS SRVCC num'])/tt.agg.sum(core['LTE_to_UMTS SRVCC den'])*100
        m1['S1_mode_IMS_PDN_connections']=tt.agg.sum(core['Maximum number of PDN connections'])
        m1['IMS_bearer_activation_SR']=tt.agg.sum(core['Voice bearer success over S11'])/tt.agg.sum(core['Voice bearer request over S11'])*100
        m1['VoLTE_Voice_Bearer_Activation_SR(IMS)']=tt.agg.sum(core['Voice bearer success'])/tt.agg.sum(core['Voice bearer request'])*100
        m1['VoLTE_PDN_Connect_SR(IMS)']=tt.agg.sum(core['PDN Connect success'])/tt.agg.sum(core['PDN Connect request'])*100
        m1['IMS_Voice_Drop_Rate']=tt.agg.sum(core['IMS_Voice_dr_num'])/tt.agg.sum(core['Voice bearer success'])*100
        m1['VoLTE_Inter_MME_Voice_Bearer_HOSR']=tt.agg.sum(core['Inter S1 Handover num'])/tt.agg.sum(core['Inter S1 Handover den'])*100
        m1['VoLTE_Intra_MME_Voice_Bearer_HOSR']=(tt.agg.sum(core['X2 Handover num'])+tt.agg.sum(core['Intra S1 Handover num']))/\
                                                (tt.agg.sum(core['X2 Handover den'])+tt.agg.sum(core['Intra S1 Handover den']))*100
        m1['VoLTE_MO_attempt']=tt.agg.sum(core['MO Attempts'])
        m1['VoLTE_MT_attempt']=tt.agg.sum(core['MT Attempts'])
        m1['VoLTE_MO_answered']=tt.agg.sum(core['MO Answered'])
        m1['VoLTE_MT_answered']=tt.agg.sum(core['MT Answered'])
        m1['VoLTE_MO_connected']=tt.agg.sum(core['MO Connected'])
        m1['VoLTE_MT_connected']=tt.agg.sum(core['MT Connected'])
        m1['VoLTE_MO_interruptions']=tt.agg.sum(core['MO Interruptions'])
        m1['VoLTE_MT_interruptions']=tt.agg.sum(core['MT Interruptions'])
        m1['VoLTE_Initial_Registration_request']=tt.agg.sum(core['Initial register attempt'])
        m1['VoLTE_Re_registration_request']=tt.agg.sum(core['Re-registration attempt'])
        m1['VoLTE_Deregistration_request']=tt.agg.sum(core['Deregistration attempt'])
        m1['VoLTE_Notify_messages_for_Reg']=tt.agg.sum(core['Notify Messages for Reg den'])
        m1['VoLTE_Subscribe_requests_for_Reg']=tt.agg.sum(core['Subscribe Requests for Registration den'])
        m1['VoLTE_Third_party_Deregistrations']=tt.agg.sum(core['Third_party deregistration attempt'])
        m1['VoLTE_Third_party_Registrations']=(tt.agg.sum(core['Third_party registration success'])+tt.agg.sum(core['Third_party registration unsuccess']))
        m1['VoLTE_Auth_request']=tt.agg.sum(core['Authentication attempt'])
        m1['VoLTE_MO_Call_drops']=tt.agg.sum(core['MO Call Drop'])
        m1['VoLTE_MT_Call_drops']=tt.agg.sum(core['MT Call Drop'])
        m1['LTE_to_GSM_SRVCC_HO_attempt']=tt.agg.sum(core['LTE_to_GSM SRVCC den'])
        m1['LTE_to_UMTS_SRVCC_HO_attempt']=tt.agg.sum(core['LTE_to_UMTS SRVCC den'])
        m1['IMS_bearer_activation_request']=tt.agg.sum(core['Voice bearer request over S11'])
        m1['VoLTE_bearer_request']=tt.agg.sum(core['Voice bearer request'])
        m1['VoLTE_PDN_Connect_request']=tt.agg.sum(core['PDN Connect request'])
        m1['IMS_voice_drop']=tt.agg.sum(core['IMS_Voice_dr_num'])
        m1['VoLTE_Inter_MME_Voice_Bearer_HO_request']=tt.agg.sum(core['Inter S1 Handover den'])
        m1['VoLTE_Intra_MME_Voice_Bearer_HO_request']=(tt.agg.sum(core['X2 Handover den'])+tt.agg.sum(core['Intra S1 Handover den']))
        #
        for element in [m1['VoLTE_MO_Call_Completion_SR'],m1['VoLTE_MT_Call_Completion_SR'],m1['VoLTE_MO_Call_Answer_Rate'],m1['VoLTE_MT_Call_Answer_Rate'],
        m1['VoLTE_MO_Call_Interrupt_Rate'],m1['VoLTE_MT_Call_Interrupt_Rate'],m1['VoLTE_MO_Call_setup_time_sec'],m1['VoLTE_MT_Call_setup_time_sec'],
        m1['VoLTE_BHCA_per_subs'],m1['VoLTE_Mean_duration_of_MO_calls_sec'],m1['VoLTE_Mean_duration_of_MT_calls_sec'],m1['VoLTE_peak_simultaneous_sessions'],
        m1['VoLTE_subscribers'],m1['VoLTE_Initial_Registration_SR'],m1['VoLTE_Re_registration_SR'],m1['VoLTE_Deregistration_SR'],m1['VoLTE_Notify_messages_for_Reg_SR'],
        m1['VoLTE_Subscribe_requests_for_Reg_SR'],m1['VoLTE_Third_party_Deregistrations_SR'],m1['VoLTE_Third_party_Registrations_SR'],m1['VoLTE_Authentication_SR'],
        m1['VoLTE_MO_Call_Drop_Rate'],m1['VoLTE_MT_Call_Drop_Rate'],m1['LTE_to_GSM_SRVCC_HOSR'],m1['LTE_to_UMTS_SRVCC_HOSR'],m1['S1_mode_IMS_PDN_connections'],
        m1['IMS_bearer_activation_SR'],m1['VoLTE_Voice_Bearer_Activation_SR(IMS)'],m1['VoLTE_PDN_Connect_SR(IMS)'],m1['IMS_Voice_Drop_Rate'],
        m1['VoLTE_Inter_MME_Voice_Bearer_HOSR'],m1['VoLTE_Intra_MME_Voice_Bearer_HOSR'],m1['VoLTE_MO_attempt'],m1['VoLTE_MT_attempt'],
        m1['VoLTE_MO_answered'],m1['VoLTE_MT_answered'],m1['VoLTE_MO_connected'],m1['VoLTE_MT_connected'],m1['VoLTE_MO_interruptions'],m1['VoLTE_MT_interruptions'],
        m1['VoLTE_Initial_Registration_request'],m1['VoLTE_Re_registration_request'],m1['VoLTE_Deregistration_request'],m1['VoLTE_Notify_messages_for_Reg'],
        m1['VoLTE_Subscribe_requests_for_Reg'],m1['VoLTE_Third_party_Deregistrations'],m1['VoLTE_Third_party_Registrations'],m1['VoLTE_Auth_request'],
        m1['VoLTE_MO_Call_drops'],m1['VoLTE_MT_Call_drops'],m1['LTE_to_GSM_SRVCC_HO_attempt'],m1['LTE_to_UMTS_SRVCC_HO_attempt'],m1['IMS_bearer_activation_request'],
        m1['VoLTE_bearer_request'],m1['VoLTE_PDN_Connect_request'],m1['IMS_voice_drop'],m1['VoLTE_Inter_MME_Voice_Bearer_HO_request'],
        m1['VoLTE_Intra_MME_Voice_Bearer_HO_request']]:
             element.folder='VoLTE'
        #
        m1['2G_3G_DL_traf_GB']=tt.agg.sum(core['2G_3G DL'])/1024/1024
        m1['2G_3G_UL_traf_GB']=tt.agg.sum(core['2G_3G UL'])/1024/1024
        m1['2G_3G_traf_GB']=m1['2G_3G_DL_traf_GB']+m1['2G_3G_UL_traf_GB']
        m1['4G_DL_traf_GB']=tt.agg.sum(core['4G DL'])/1024/1024
        m1['4G_UL_traf_GB']=tt.agg.sum(core['4G UL'])/1024/1024
        m1['4G_traf_GB']=m1['4G_DL_traf_GB']+m1['4G_UL_traf_GB']
        m1['Total_DL_traf_TB']=(m1['2G_3G_DL_traf_GB']+m1['4G_DL_traf_GB'])/1024
        m1['Total_UL_traf_TB']=(m1['2G_3G_UL_traf_GB']+m1['4G_UL_traf_GB'])/1024
        m1['Total_traf_TB']=m1['Total_DL_traf_TB']+m1['Total_UL_traf_TB']
        m1['Total_DL_traf_Gbps']=m1['Total_DL_traf_TB']*1024*1024*8/3600
        m1['Total_UL_traf_Gbps']=m1['Total_UL_traf_TB']*1024*1024*8/3600
        m1['Total_traf_Gbps']=m1['Total_DL_traf_Gbps']+m1['Total_UL_traf_Gbps']
        #
        for element in [m1['2G_3G_DL_traf_GB'],m1['2G_3G_UL_traf_GB'],m1['2G_3G_traf_GB'],m1['4G_DL_traf_GB'],m1['4G_UL_traf_GB'],m1['4G_traf_GB'],m1['Total_DL_traf_TB'],
                        m1['Total_UL_traf_TB'],m1['Total_traf_TB'],m1['Total_DL_traf_Gbps'],m1['Total_UL_traf_Gbps'],m1['Total_traf_Gbps']]:
             element.folder='Data Traffic'
    except Exception as e:
        print(e)
        1
add_core_measures()				



from watchdog.observers.polling import PollingObserver
from watchdog.events import FileSystemEventHandler, FileCreatedEvent, FileModifiedEvent
import glob

class AtotiWatcher(FileSystemEventHandler):
	def on_modified(self, event: FileModifiedEvent): #FileCreatedEvent
	#	try:
		print(event.src_path)
		#pd.DataFrame(os.listdir("/disk2/support_files/atoti/bi")).to_csv('/disk2/support_files/atoti/filesss.csv',index=False,mode='a')
		two_g=pd.concat(map(pd.read_csv,glob.glob("/disk2/support_files/atoti/bi/2G*.csv")),ignore_index=True)
		two_g=two_g.groupby(['Date','Region']).sum().reset_index()
		#two.load_csv("/disk2/support_files/atoti/bi/2G*.csv")
		two_g['Date']=pd.to_datetime(two_g['Date'])
		two_g['Day']=two_g['Date'].dt.date
		two_g['Month']=two_g['Date'].dt.month
		two_g['Year']=two_g['Date'].dt.year.astype(str)
		two_g['Hour']=two_g['Date'].dt.hour
		
		two.load_pandas(two_g)
		#os.remove("disk2/support/files/atoti/bi/2G*.csv")
		#three.load_csv("/disk2/support_files/atoti/bi/3G*.csv")
		three_g=pd.concat(map(pd.read_csv,glob.glob("/disk2/support_files/atoti/bi/3G*.csv")),ignore_index=True)
		three_g=three_g.groupby(['Date','Region']).sum().reset_index()
		three_g['Date']=pd.to_datetime(three_g['Date'])
		three_g['Day']=three_g['Date'].dt.date
		three_g['Month']=three_g['Date'].dt.month
		three_g['Year']=three_g['Date'].dt.year.astype(str)
		three_g['Hour']=three_g['Date'].dt.hour
		three.load_pandas(three_g)
		#os.remove("disk2/support/files/atoti/bi/3G*.csv")
		#four.load_csv("/disk2/support_files/atoti/bi/4G*.csv")
		four_g=pd.concat(map(pd.read_csv,glob.glob("/disk2/support_files/atoti/bi/4G*.csv")),ignore_index=True)
		four_g=four_g.groupby(['Date','Region']).sum().reset_index()
		four_g['Date']=pd.to_datetime(four_g['Date'])
		four_g['Day']=four_g['Date'].dt.date
		four_g['Month']=four_g['Date'].dt.month
		four_g['Year']=four_g['Date'].dt.year.astype(str)
		four_g['Hour']=four_g['Date'].dt.hour
		four.load_pandas(four_g)
		#os.remove("disk2/support/files/atoti/bi/4G*.csv")
		core_inputs=pd.read_csv("/disk2/support_files/atoti/bi/core_inputs.csv").drop_duplicates()
		core_inputs=core_inputs[core_inputs['Date']!='Date']
		core_inputs['Date']=pd.to_datetime(core_inputs['Date'])
		a=['Site', 'HO_type', 'Entity_name', 'Direction', 'Type', 'Identifier', 'mode', 'Region', 'MNO', 'BSC_RNC'] #'APN', 
        #
		core_inputs[a]=core_inputs[a].fillna('All')
		#core_inputs['LAC']=core_inputs['LAC'].replace({'All':100}).astype(int)
		#core_inputs['RAC']=core_inputs['RAC'].replace({'All':100}).astype(float)
		core_inputs['Day']=core_inputs['Date'].dt.date
		core_inputs['Month']=core_inputs['Date'].dt.month
		core_inputs['Year']=core_inputs['Date'].dt.year.astype(str)
		core_inputs['Hour']=core_inputs['Date'].dt.hour
        #
		core_inputs.columns=core_inputs.columns.map(lambda x: x.replace('/','_'))
		core_inputs[b]=core_inputs[b].astype(float)
		core.load_pandas(core_inputs)

#		except Exception as error:
#			print(error)
#			1
		#	with open('/disk2/support_files/atoti/readme.txt', 'w') as f: [f.write(i) for i in error]
			#pd.DataFrame(list(error)).to_csv('/disk2/support_files/atoti/error.csv',index=False,mode='a')
	
observer = PollingObserver()
#observer.schedule(AtotiWatcher(), "/disk2/support_files/atoti/bi/4G_nokia.csv")
observer.schedule(AtotiWatcher(), "/disk2/support_files/atoti/bi/core_inputs.csv")
observer.start()



try:
	while True:	
		time.sleep(10)
		print(observer.is_alive())
finally:
	observer.stop()
	observer.join()

#if __name__ == '__main__':
#    run()
