import pandas as pd
from dash.dependencies import Input, Output, State
import plotly.graph_objs as go
from dash import ctx, dash_table
import time, datetime
import os,glob
from datetime import datetime as dt

KPIs = {  # 'Data Traffic':'Data Traffic, GB',
    'Call Setup Time': 'Call Setup Time',
    'Paging SR': 'Paging SR',
    'Attach SR': 'Attach SR',
    'Location Update SR': 'Location Update SR',
    'PDP_Ctx and Bearer Setup SR': 'PDP_Ctx and Bearer Setup SR'
    # 'Data':'Data Traffic, GB'
}

DRILLDOWN_FILTERS = [
    'Technology',
    'Region'
]

yesterday = datetime.datetime.now() - datetime.timedelta(1)


def prepare_data():
    b_avail, db_avail, br_avail, dbr_avail, dcc_session,cpu,cpu2=[],[],[],[],[],[],[]
    needed_period = datetime.datetime.strftime(datetime.date.today() - datetime.timedelta(1),'%Y-%m-%d 00:00')
    ######################## Board Availability ########################
    for mno in ['BKC','AZF']:
        d=[]
        if mno == 'BKC':
            #for i in glob.glob('/disk2/support_files/archive/it/*.h5'):
            #    if 'azf' in i:continue
            #    d.append(pd.read_hdf(i,'OSRuntime'))
            br=pd.read_csv('/disk2/support_files/archive/it_trend_files/OSRuntime_bkc.csv')

        else:
            #for i in glob.glob('/disk2/support_files/archive/it/*azf_*.h5'):
            #    d.append(pd.read_hdf(i,'OSRuntime'))
            br=pd.read_csv('/disk2/support_files/archive/it_trend_files/OSRuntime_azf.csv')
        #br = pd.concat(d)
        br.drop_duplicates(inplace=True)
        br['Date']=pd.to_datetime(br['Date'])
        br=br[br['Date']>=needed_period]
        br.sort_values(by=['Date','UserLabel'],inplace=True)
        br.reset_index(inplace=True,drop=True)
        br['check']=br['UserLabel']==br['UserLabel'].shift(1)
        br['interval']=br['Date']-br['Date'].shift(1)
        br['nese']=br['interval']*br['check']
        br['interval']=br['nese'].apply(lambda x: x.total_seconds()/60/15)
        br.loc[br['check']==False,'interval']=1
        br['hwHostSysUptime']=br['hwHostSysUptime'].astype(int)
        #br[br['hwHostSysUptime']!=2147483647]
        br['delta']=(br['hwHostSysUptime']-br['hwHostSysUptime'].shift(1))*br['check']
        br.loc[br['delta']<0,'delta']=90000-br['hwHostSysUptime']
        br['delta2']=br['delta']
        br.loc[br['hwHostSysUptime']!=2147483647,'delta2']=(br.loc[br['hwHostSysUptime']!=2147483647,'delta']-100*60*15*br.loc[br['hwHostSysUptime']!=2147483647,'interval'])/100
        br.loc[abs(br['delta2'])<10,'delta2']=0
        br.loc[br['check']==False,'delta2']=0
        br['Availability']=(15*60*br['interval']+br['delta2'])/(15*60*br['interval'])*100
        br['avail_num']=15*60*br['interval']+br['delta2']
        br['avail_den']=15*60*br['interval']
        br.insert(0,'MNO',mno)  
        br_avail.append(br)
        a=br.groupby('Date').sum()[['avail_num','avail_den']]
        b=a.reset_index()
        #b=a.resample('1D').sum()
        b['Availability']=b['avail_num']/b['avail_den']*100
        b.loc[b['Availability']>100,'Availability']=100
        b.insert(0,'MNO',mno)        
        b_avail.append(b)
        #################### DB Availability ############################
        d=[]
        if mno == 'BKC':
            #for i in glob.glob('/disk2/support_files/archive/it/*.h5'):
            #    if 'azf' in i:continue
            #    d.append(pd.read_hdf(i,'persistRunningTime'))
            dbr=pd.read_csv('/disk2/support_files/archive/it_trend_files/persistRunningTime_bkc.csv')
        else:
            dbr=pd.read_csv('/disk2/support_files/archive/it_trend_files/persistRunningTime_azf.csv')
            #for i in glob.glob('/disk2/support_files/archive/it/*azf_*.h5'):
            #    d.append(pd.read_hdf(i,'persistRunningTime'))
        #dbr=pd.concat(d)
        dbr.drop_duplicates(inplace=True)
        dbr['Date']=pd.to_datetime(dbr['Date'])
        dbr=dbr[dbr['Date']>=needed_period]
        dbr.sort_values(by=['Date','UserLabel'],inplace=True)
        dbr.reset_index(inplace=True,drop=True)
        dbr['check']=dbr['UserLabel']==dbr['UserLabel'].shift(1)
        dbr['interval']=dbr['Date']-dbr['Date'].shift(1)
        dbr['nese']=dbr['interval']*dbr['check']
        dbr['interval']=dbr['nese'].apply(lambda x: x.total_seconds()/60/5)
        dbr.loc[dbr['check']==False,'interval']=1
        dbr['hwDbSysUptime']=dbr['hwDbSysUptime'].astype(int)
        #br[br['hwHostSysUptime']!=2147483647]
        dbr['delta']=(dbr['hwDbSysUptime']-dbr['hwDbSysUptime'].shift(1))*dbr['check']
        dbr.loc[dbr['delta']<0,'delta']=300-dbr['hwDbSysUptime']
        dbr.loc[:,'delta2']=(dbr.loc[:,'delta']-60*5*dbr.loc[:,'interval'])
        dbr.loc[abs(dbr['delta2'])<10,'delta2']=0
        dbr.loc[dbr['delta']==0,'delta2']=0
        dbr.loc[dbr['check']==False,'delta2']=0
        dbr['availability']=(5*60*dbr['interval']+dbr['delta2'])/(5*60*dbr['interval'])*100
        dbr['avail_num']=5*60*dbr['interval']+dbr['delta2']
        dbr['avail_den']=5*60*dbr['interval']
        dbr['avail_num_corrected']=dbr['avail_num']
        filt=abs(dbr['avail_den']-dbr['avail_num'])<50
        dbr.loc[filt,'avail_num_corrected']=dbr.loc[filt,'avail_den']
        dbr['Availability']=dbr['avail_num_corrected']/dbr['avail_den']*100
        dbr.insert(0,'MNO',mno)  
        dbr_avail.append(dbr)
        a=dbr.groupby('Date').sum()[['avail_num_corrected','avail_num','avail_den']]
        b=a.reset_index()
        #b=a.resample('1H').sum()
        b['Availability']=b['avail_num_corrected']/b['avail_den']*100
        b.loc[b['Availability']>100,'Availability']=100
        b.insert(0,'MNO',mno)
        db_avail.append(b)

        c=pd.read_csv('/disk2/support_files/archive/it_trend_files/CPU_'+str.lower(mno)+'.csv')
        c['CpuUsage']=c['CpuUsage'].astype(float)
        c['LogicalCPUName']=''
        c2=pd.read_csv('/disk2/support_files/archive/it_trend_files/LogicalCPU_'+str.lower(mno)+'.csv')
        c2['LogicalCPUUsage']=c2['LogicalCPUUsage'].astype(float)
        c2.rename(columns={'LogicalCPUUsage':'CpuUsage'},inplace=True)
        c=pd.concat([c[['Date','Period','LocalDn','UserLabel','LogicalCPUName','CpuUsage']],c2[['Date','Period','LocalDn','UserLabel','LogicalCPUName','CpuUsage']]])
        c['Date']=pd.to_datetime(c['Date'])
        b=c.groupby('Date')['CpuUsage'].agg(['max','median']).reset_index()
        b.insert(0,'MNO',mno)
        cpu.append(b)
        c.insert(0,'MNO',mno)
        cpu2.append(c)

        ############################# DCC Session ################################
        #d=[]
        #if mno == 'BKC':
        #    #for i in glob.glob('*.h5'):
        #    #    if 'azf' in i:continue
        #    #    if '2022-05-20' in i:continue
        #    #    d.append(pd.read_hdf(i,'hwBillingDCCSessionNumber'))
        #    dcc_ses=pd.read_csv('/disk2/support_files/archive/DCCSession_bkc.csv')
        #else:
        #    dcc_ses=pd.read_csv('/disk2/support_files/archive/DCCSession_azf.csv')
        #    #for i in glob.glob('*azf_*.h5'):
        #    #    if '2022-05-20' in i:continue
        #    #    d.append(pd.read_hdf(i,'hwBillingDCCSessionNumber'))
        ##dcc_ses=pd.concat(d)
        #dcc_ses.drop_duplicates(inplace=True)
        #dcc_ses['Date']=pd.to_datetime(dcc_ses['Date'])
        ##dcc_ses['day']=dcc_ses['Date'].dt.date
        #dcc_ses.iloc[:,6:-1]=dcc_ses.iloc[:,6:-1].astype(float)
        #dcc_ses['hwEventType']=dcc_ses['hwEventType'].astype(str)
        #b=dcc_ses.drop(columns=['Period','hwBEID']).groupby(['Date','hwEventType'],as_index=False)\
        #        .sum()
        ##c=b.groupby(['day','hwEventType']).sum().reset_index()
        #b['Session_SR']=round(b['hwSuccessSessionCount']/b['hwSessionTotalNumber']*100,2)
        #if 'hwCAPS' not in b.columns:
        #    b['hwCAPS']=0
        ##b=b[['Date','hwEventType','Session_SR','hwFailedSessionCount','hwCAPS']]
        #b['hwEventType'].replace(['1105','11000','11002','11006','12005'],['BWControlling','OnlineVoice','OnlineSMS','OnlineGRPS','RTBPChargingRT'],inplace=True)
        #b=b[b['hwEventType'].isin(['BWControlling','OnlineVoice','OnlineSMS','OnlineGRPS','RTBPChargingRT'])]
        #b.insert(2,'MNO',mno)
        #dcc_session.append(b)

###################
    b_avail=pd.concat(b_avail)
    db_avail=pd.concat(db_avail)
    br_avail=pd.concat(br_avail)
    dbr_avail=pd.concat(dbr_avail)
    cpu=pd.concat(cpu)
    cpu2=pd.concat(cpu2)
    #dcc_session=pd.concat(dcc_session)    
    #for u in ['h', 'd']:
    #    if u == 'd':
    #       df_b_avail = b_avail.groupby(pd.DatetimeIndex(b_avail['Date']).strftime('%Y-%m-%d')).sum()
    #        df_b_avail.reset_index(inplace=True)
    #        df_b_avail['Date'] = pd.to_datetime(df_b_avail['Date'])
            
    #        df_db_avail = db_avail.groupby(pd.DatetimeIndex(db_avail['Date']).strftime('%Y-%m-%d')).sum()
    #        df_db_avail.reset_index(inplace=True)
    #        df_db_avail['Date'] = pd.to_datetime(df_db_avail['Date'])
     #   else:
     #       df_b_avail=b_avail
     #       df_db_avail=db_avail

            # traffic part
    
    return b_avail, db_avail,br_avail,dbr_avail,cpu,cpu2#, dcc_session

layout = {

    'template': 'plotly_dark',
    'margin': dict(l=2, r=2, t=5, b=0),
    'font': dict(size=8),
    'yaxis': {'title': '', 'showgrid': False},
    'xaxis': {'title': '', 'showgrid': False,
              'type': 'date'},
    'clickmode': 'event+select',
    'legend': {'itemclick': 'toggle'}
}
#df_b_avail, df_db_avail,br_avail,dbr_avail,cpu,cpu2 = prepare_data()
print('come here')

bb,aa=[],[]
def register_callback(dashapp):
    #@dashapp.callback(Output('whole', 'data'),
    #                  [Input('interval-component2', 'n_intervals')]
    #                  )
    #def refresh_data(n):
    #    global df_b_avail, df_db_avail
    #    df_b_avail, df_db_avail = prepare_data()
    #    print('why not enter here2222')

    @dashapp.callback(Output('table_elave_et','children'),
    Input('Data Traffic','clickData'),
    Input('BKC_alarm_count','clickData'),
    Input('AZF_alarm_count','clickData'))
    def add_table(selected,selected2,selected3):
        #global br_avail,dbr_avail,cpu2,aa,bb
        global aa, bb
        br_avail,dbr_avail=[],[]
        for mno in ['azf','bkc']:
            ddd=pd.read_csv('/disk2/support_files/archive/OSRuntime_'+mno+'.csv')
            ddd['mno']=mno
            br_avail.append(ddd)
        br_avail = pd.concat(br_avail)
        for mno in ['azf','bkc']:
            ddd=pd.read_csv('/disk2/support_files/archive/persistRunningTime_'+mno+'.csv')
            ddd['mno']=mno
            dbr_avail.append(ddd)
        dbr_avail = pd.concat(dbr_avail)
        ctx_msg = ctx.triggered
        reverse_map={'CPU, Memory or Storage':['CPU','LogicalCPU','Memory','serviceDatabaseInf','Storage','VirtualMemory'],
             'DCC related':['hwBillingDCCMsgNumber','hwBillingDCCSessionNumber','hwBillingDCCSeviceCallNumber'],
             'Diameter related':['hwForwardDiamMsgStat'],'Offline stats':['hwOfflineStat'],
            'Availability':['OSRuntime','persistRunningTime']
            }
        #print(ctx_msg)
        print(ctx_msg[0]['prop_id'])
        if ctx_msg[0]['prop_id']=="BKC_alarm_count.clickData":
            #print(df_b_avail.head())
            #print(selected)
            #print(selected['points'][0]['x'])
            #print(selected['points'][0]['curveNumber'])
            #br_avail['x']=br_avail['MNO'].map({'AZF':0,'BKC':1})
            needed_kpis=reverse_map[bb[selected2['points'][0]['curveNumber']]]
            needed_date=selected2['points'][0]['x']
            filted=pd.read_hdf('/disk2/support_files/archive/anomality/'+needed_date[:10]+'_anomalies.h5','it',where='Date=needed_date and file in needed_kpis')
            filted[['up_threshold','down_threshold']]=round(filted[['up_threshold','down_threshold']],2)
            #filted['threshold']=0
            #filted.loc[filted['status']==1,'threshold']=filted.loc[filted['status']==1,filted['up_threshold']]
            #filted.loc[filted['status']==2,'threshold']=filted.loc[filted['status']==2,filted['down_threshold']]
            filted['Date'] = [dt.strftime(filted['Date'].iloc[i].to_pydatetime(), '%d.%m.%Y %H:%M') for i in
                                range(len(filted['Date']))]
            filted = filted[['Date','period','file','UserLabel','hwNetDevice','hwEventType','variable','value','up_threshold','down_threshold','status']]
            #print(filted)
            tt=dash_table.DataTable(id='live-table',
                                            columns=[{'name': i, 'id': i} for i in filted.columns],
                                            data=filted.to_dict('records'),style_cell={'textAlign': 'center',
                                                                                    'color': 'black', 'fontSize': 11,
                                                                                    'height':'auto',
                                                                                    'whiteSpace':'normal',
                                                                                    "font-family": "Comic Sans MS",
                                                                                    'width':str(100 / (len(filted.columns))) + '%'},
                        style_header={
                            'backgroundColor': 'rgb(167, 171, 170)',
                            'fontWeight': 'bold', 'color': 'black',
                            'fontSize': 12},
                        virtualization=True,
    #filter_action='native',
            sort_action="native",
    sort_mode='multi',
    page_action="native",
                        fixed_rows={'headers': True,'data':0},
                        #fixed_columns={'headers': True, 'data': 0},
                        style_table={
                                    'overflowY': 'auto',
                                    'minWidth': 95,
                                    },export_format="xlsx", page_size=10)
        elif ctx_msg[0]['prop_id']=="AZF_alarm_count.clickData":
            #print(df_b_avail.head())
            #print(selected)
            #print(selected['points'][0]['x'])
            #print(selected['points'][0]['curveNumber'])
            needed_kpis=reverse_map[aa[selected3['points'][0]['curveNumber']]]
            needed_date=selected3['points'][0]['x']
            filted=pd.read_hdf('/disk2/support_files/archive/anomality/'+needed_date[:10]+'_anomalies.h5','it_azf',where='Date=needed_date and file in needed_kpis')
            filted[['up_threshold','down_threshold']]=round(filted[['up_threshold','down_threshold']],2)
            #filted['threshold']=0
            #filted.loc[filted['status']==1,'threshold']=round(filted.loc[filted['status']==1,filted['up_threshold']],2)
            #filted.loc[filted['status']==2,'threshold']=round(filted.loc[filted['status']==2,filted['down_threshold']],2)
            filted['Date'] = [dt.strftime(filted['Date'].iloc[i].to_pydatetime(), '%d.%m.%Y %H:%M') for i in
                                range(len(filted['Date']))]
            filted = filted[['Date','period','file','UserLabel','hwNetDevice','hwEventType','variable','value','up_threshold','down_threshold','status']]
            #print(filted)
            tt=dash_table.DataTable(id='live-table',
                                            columns=[{'name': i, 'id': i} for i in filted.columns],
                                            data=filted.to_dict('records'),style_cell={'textAlign': 'center',
                                                                                    'color': 'black', 'fontSize': 11,
                                                                                    'height':'auto',
                                                                                    'whiteSpace':'normal',
                                                                                    "font-family": "Comic Sans MS",
                                                                                    'width':str(100 / (len(filted.columns))) + '%'},
                        style_header={
                            'backgroundColor': 'rgb(167, 171, 170)',
                            'fontWeight': 'bold', 'color': 'black',
                            'fontSize': 12},
                        virtualization=True,
    #filter_action='native',
            sort_action="native",
    sort_mode='multi',
    page_action="native",
                        fixed_rows={'headers': True,'data':0},
                        #fixed_columns={'headers': True, 'data': 0},
                        style_table={
                                    'overflowY': 'auto',
                                    'minWidth': 95,
                                    },export_format="xlsx", page_size=10)
        
        else:
            tt=[]
        return tt


    @dashapp.callback(Output('Data Traffic', 'figure'),
                      #Output('Data Traffic_nese', 'children'),
                      Output('BKC_alarm_count', 'figure'),
                      #Output('Call Setup Time_nese', 'children'),
                      Output('AZF_alarm_count', 'figure'),
                      #Output('Paging SR_nese', 'children'),
                      #[Input('radio-items', 'value')],
                      [Input('interval-component2', 'n_intervals')],
                      #[Input('radio-items2', 'value')],
                      [Input('interval-component2', 'n_intervals')]
                      )
    def update_traffic(n,int): # value, cli
        #global df_b_avail, df_db_avail
        #df_b_avail, df_db_avail,br_avail,dbr_avail,cpu,cpu2 = prepare_data()
        print('why not enter here2222')
        trace = []
        trace_db= []
        trace_cpu = []
        trace_ses= []
        br_avail,dbr_avail=[],[]
        for mno in ['azf','bkc']:
            ddd=pd.read_csv('/disk2/support_files/archive/OSRuntime_'+mno+'.csv')
            ddd['mno']=mno
            br_avail.append(ddd)
        br_avail = pd.concat(br_avail)
        for mno in ['azf','bkc']:
            ddd=pd.read_csv('/disk2/support_files/archive/persistRunningTime_'+mno+'.csv')
            ddd['mno']=mno
            dbr_avail.append(ddd)
        dbr_avail = pd.concat(dbr_avail)
       
        df=br_avail
        df_db=dbr_avail
        df['Date']=pd.to_datetime(df['Date'])
        df_db['Date']=pd.to_datetime(df_db['Date'])
        #df = df_b_avail
        #df_db=df_db_avail
        #df_c = cpu

        df_bk=df[(df['mno'] == 'bkc')]
        df_az=df[(df['mno'] == 'azf')]
        df_db_bk=df_db[(df_db['mno'] == 'bkc')]
        df_db_az=df_db[(df_db['mno'] == 'azf')]
   ########### Gauge chart ###########
        fig = go.Figure()
        fig.add_trace(go.Indicator(
    #domain = {'x': [0, 1], 'y': [0, 1]},
    value = df_az[(df_az['Date']==df_az['Date'].unique()[-1])]['availability'].values[0],
    mode = "gauge+number+delta",
    title = {'text': "AZF Billing Board Availability_"+str(
                                 dt.strftime(dt.utcfromtimestamp(df_az['Date'].unique()[-1].tolist() / 1e9),
                                             '%d.%m.%Y %H:%M')),'font_size':12},
    #size=150,
    delta = {'reference': df_az[(df_az['Date']==df_az['Date'].unique()[-2])]['availability'].values[0]},
    gauge = {'axis': {'range': [None, 100]},
             'steps' : [
                 {'range': [0, 90], 'color': "lightgray"},
                 {'range': [90, 100], 'color': "gray"}],
             'threshold' : {'line': {'color': "red", 'width': 4}, 'thickness': 0.75, 'value': 99}},
             domain={'row': 0, 'column': 0})
             )
        fig.update_layout(
    paper_bgcolor="lightgray",
    #height=80,  # Added parameter
)
        fig.add_trace(go.Indicator(
    #domain = {'x': [0, 1], 'y': [0, 1]},
    value = df_bk[(df_bk['Date']==df_bk['Date'].unique()[-1])]['availability'].values[0],
    mode = "gauge+number+delta",
    title = {'text': "BKC Billing Board Availability_"+str(
                                 dt.strftime(dt.utcfromtimestamp(df_bk['Date'].unique()[-1].tolist() / 1e9),
                                             '%d.%m.%Y %H:%M')),'font_size':12},
    #size=150,
    delta = {'reference': df_bk[(df_bk['Date']==df_bk['Date'].unique()[-2])]['availability'].values[0]},
    gauge = {'axis': {'range': [None, 100]},
             'steps' : [
                 {'range': [0, 90], 'color': "lightgray"},
                 {'range': [90, 100], 'color': "gray"}],
             'threshold' : {'line': {'color': "red", 'width': 4}, 'thickness': 0.75, 'value': 99}},
             domain={'row': 0, 'column': 1})
             )
        #fig2.update_layout(
    #paper_bgcolor="lightgray",
    #height=300,  # Added parameter
#)
        fig.add_trace(go.Indicator(
    #domain = {'x': [0, 1], 'y': [0, 1]},
    value = df_db_az[(df_db_az['Date']==df_db_az['Date'].unique()[-1])]['availability'].values[0],
    mode = "gauge+number+delta",
    title = {'text': "AZF Billing Database Availability_"+str(
                                 dt.strftime(dt.utcfromtimestamp(df_db_az['Date'].unique()[-1].tolist() / 1e9),
                                             '%d.%m.%Y %H:%M')),'font_size':12},
    #size=150,
    delta = {'reference': df_db_az[(df_db_az['Date']==df_db_az['Date'].unique()[-2])]['availability'].values[0]},
    gauge = {'axis': {'range': [None, 100]},
             'steps' : [
                 {'range': [0, 90], 'color': "lightgray"},
                 {'range': [90, 100], 'color': "gray"}],
             'threshold' : {'line': {'color': "red", 'width': 4}, 'thickness': 0.75, 'value': 99}},
             domain={'row': 0, 'column': 2})
             )
        fig.add_trace(go.Indicator(
    #domain = {'x': [0, 1], 'y': [0, 1]},
    value = df_db_bk[(df_db_bk['Date']==df_db_bk['Date'].unique()[-1])]['availability'].values[0],
    mode = "gauge+number+delta",
    title = {'text': "BKC Billing Database Availability_"+str(
                                 dt.strftime(dt.utcfromtimestamp(df_db_bk['Date'].unique()[-1].tolist() / 1e9),
                                             '%d.%m.%Y %H:%M')),'font_size':12},
    #size=150,
    delta = {'reference': df_db_bk[(df_db_bk['Date']==df_db_bk['Date'].unique()[-2])]['availability'].values[0]},
    gauge = {'axis': {'range': [None, 100]},
             'steps' : [
                 {'range': [0, 90], 'color': "lightgray"},
                 {'range': [90, 100], 'color': "gray"}],
             'threshold' : {'line': {'color': "red", 'width': 4}, 'thickness': 0.75, 'value': 99}},
             domain={'row': 0, 'column': 3})
             )
        fig.update_layout(
    grid = {'rows': 1, 'columns': 4, 'pattern': "independent"},
    margin= dict(l=2, r=2, t=50, b=10))
        anom_bk=pd.read_csv('/disk2/support_files/archive/bkc_it_anom_trend.csv')
        needed_period = datetime.datetime.strftime(datetime.datetime.now() - datetime.timedelta(1),'%Y-%m-%d 00:00')
        anom_bk=anom_bk[pd.to_datetime(anom_bk['Date'])>=needed_period]
        global aa,bb
        map={'CPU':'CPU, Memory or Storage','hwBillingDCCMsgNumber':'DCC related','hwBillingDCCSessionNumber':'DCC related',
            'hwBillingDCCSeviceCallNumber':'DCC related','hwForwardDiamMsgStat':'Diameter related','hwOfflineStat':'Offline stats',
            'LogicalCPU':'CPU, Memory or Storage','Memory':'CPU, Memory or Storage','OSRuntime':'Availability','persistRunningTime':'Availability',
            'serviceDatabaseInf':'CPU, Memory or Storage','Storage':'CPU, Memory or Storage','VirtualMemory':'CPU, Memory or Storage'}
        anom_bk.replace(map,inplace=True)
        anom_bk=anom_bk.groupby(['Date','file']).sum().reset_index()
        for k in anom_bk['file'].unique():
            bb.append(k)
            trace.append(go.Scatter(x=anom_bk[anom_bk['file']==k]['Date'],
                                        y=anom_bk[anom_bk['file']==k]['value'],
                                        mode='lines',
                                        opacity=0.7,
                                        name='BKC_'+k,
                                        textposition='bottom center'))
        #        nese3 = ''

        traces = [trace]
        data = [val for sublist in traces for val in sublist]
        figure = go.Figure(data=data, layout=layout)
        figure.update_yaxes(automargin=True)

        anom_az=pd.read_csv('/disk2/support_files/archive/azf_it_anom_trend.csv')
        anom_az=anom_az[pd.to_datetime(anom_az['Date'])>=needed_period]
        anom_az.replace(map,inplace=True)
        anom_az=anom_az.groupby(['Date','file']).sum().reset_index()
        for k in anom_az['file'].unique():
            aa.append(k)
            trace_db.append(go.Scatter(x=anom_az[anom_az['file']==k]['Date'],
                                        y=anom_az[anom_az['file']==k]['value'],
                                        mode='lines',
                                        opacity=0.7,
                                        name='AZF_'+k,
                                        textposition='bottom center'))
        #        nese3 = ''

        traces_az = [trace_db]
        data_az = [val for sublist in traces_az for val in sublist]
        figure2 = go.Figure(data=data_az, layout=layout)
        figure2.update_yaxes(automargin=True)
        
    #    fig3.update_layout(
    #paper_bgcolor="lightgray",
    #height=300,  # Added parameter
#)
       

        #figure_ses = go.Figure(data=data_ses, layout=layout)
        #figure_ses.update_yaxes(automargin=True)

        return fig,figure, figure2#, fig2, fig3#, figure_ses, nese3
