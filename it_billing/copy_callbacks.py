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

yesterday = datetime.date.today() - datetime.timedelta(15)


def prepare_data():
    b_avail, db_avail, br_avail, dbr_avail, dcc_session,cpu,cpu2=[],[],[],[],[],[],[]
    needed_period =datetime.datetime.strftime(datetime.date.today() - datetime.timedelta(3),'%Y-%m-%d 00:00')
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
df_b_avail, df_db_avail,br_avail,dbr_avail,cpu,cpu2 = prepare_data()
print('come here')


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
    Input('Call Setup Time','clickData'),
    Input('Paging SR','clickData'))
    def add_table(selected,selected2,selected3):
        global br_avail,dbr_avail,cpu2
        ctx_msg = ctx.triggered

        #print(ctx_msg)
        print(ctx_msg[0]['prop_id'])
        if ctx_msg[0]['prop_id']=="Data Traffic.clickData":
            #print(df_b_avail.head())
            #print(selected)
            #print(selected['points'][0]['x'])
            #print(selected['points'][0]['curveNumber'])
            br_avail['x']=br_avail['MNO'].map({'AZF':0,'BKC':1})
            filted=br_avail.loc[(br_avail['Date']==selected['points'][0]['x']) & (br_avail['x']==selected['points'][0]['curveNumber'])]
            filted['Availability']=round(filted['Availability'].astype(float),2)
            filted=filted[filted['Availability']<100]
            filted['Date'] = [dt.strftime(filted['Date'].iloc[i].to_pydatetime(), '%d.%m.%Y %H:%M') for i in
                                range(len(filted['Date']))]
            filted = filted[['Date','MNO','Period','LocalDn','UserLabel','Availability']]
            #print(filted)
            tt=dash_table.DataTable(id='live-table',
                                            columns=[{'name': i, 'id': i} for i in filted.columns],
                                            data=filted.sort_values(by='Availability').to_dict('records'),style_cell={'textAlign': 'center',
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
        elif ctx_msg[0]['prop_id']=="Call Setup Time.clickData":
            #print(df_b_avail.head())
            #print(selected)
            #print(selected['points'][0]['x'])
            #print(selected['points'][0]['curveNumber'])
            dbr_avail['x']=dbr_avail['MNO'].map({'AZF':0,'BKC':1})
            filted=dbr_avail.loc[(dbr_avail['Date']==selected2['points'][0]['x']) & (dbr_avail['x']==selected2['points'][0]['curveNumber'])]
            filted['Availability']=round(filted['Availability'].astype(float),2)
            #filted=filted[filted['Availability']<100]
            filted['Date'] = [dt.strftime(filted['Date'].iloc[i].to_pydatetime(), '%d.%m.%Y %H:%M') for i in
                                range(len(filted['Date']))]
            filted = filted[['Date','MNO','Period','LocalDn','UserLabel','Availability']]
            #print(filted)
            tt=dash_table.DataTable(id='live-table',
                                            columns=[{'name': i, 'id': i} for i in filted.columns],
                                            data=filted.sort_values(by='Availability').to_dict('records'),style_cell={'textAlign': 'center',
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
        elif ctx_msg[0]['prop_id']=="Paging SR.clickData":
            #print(df_b_avail.head())
            #print(selected)
            #print(selected['points'][0]['x'])
            #print(selected['points'][0]['curveNumber'])
            cpu2['x']=cpu2['MNO'].map({'AZF':0,'BKC':1})
            filted=cpu2.loc[(cpu2['Date']==selected3['points'][0]['x']) & (cpu2['x']==selected3['points'][0]['curveNumber'])]
            #filted['Date']=pd.to_datetime(filted['Date'])
            filted=filted[filted['CpuUsage']>85]
            filted['Date'] = [dt.strftime(filted['Date'].iloc[i].to_pydatetime(), '%d.%m.%Y %H:%M') for i in
                                range(len(filted['Date']))]
            filted = filted[['Date','MNO','Period','LocalDn','UserLabel','LogicalCPUName','CpuUsage']]
            
            #filted=filted.reset_index().iloc[:5]
            #print(filted)
            tt=dash_table.DataTable(id='live-table',
                                            columns=[{'name': i, 'id': i} for i in filted.columns],
                                            data=filted.sort_values(by='CpuUsage').to_dict('records'),style_cell={'textAlign': 'center',
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
                      Output('Data Traffic_nese', 'children'),
                      Output('Call Setup Time', 'figure'),
                      Output('Call Setup Time_nese', 'children'),
                      Output('Paging SR', 'figure'),
                      Output('Paging SR_nese', 'children'),
                      #[Input('radio-items', 'value')],
                      [Input('interval-component2', 'n_intervals')],
                      #[Input('radio-items2', 'value')],
                      [Input('interval-component2', 'n_intervals')]
                      )
    def update_traffic(n,int): # value, cli
        global df_b_avail, df_db_avail
        df_b_avail, df_db_avail,br_avail,dbr_avail,cpu,cpu2 = prepare_data()
        print('why not enter here2222')
        trace = []
        trace_db= []
        trace_cpu = []
        trace_ses= []
        #if value == 'D':
        #    df = df_b_avail.groupby([pd.DatetimeIndex(df_b_avail['Date']).strftime('%Y-%m-%d'), 'MNO']).sum()
        #    pd.DataFrame(df).reset_index(inplace=True)
        #    df['Date'] = pd.to_datetime(df['Date'])
        #    df=df[df['Date'].dt.date != datetime.date.today()]
        #    df['Availability']=df['avail_num']/df['avail_den']*100

        #    df_db = df_db_avail.groupby([pd.DatetimeIndex(df_db_avail['Date']).strftime('%Y-%m-%d'), 'MNO']).sum()
        #    pd.DataFrame(df_db).reset_index(inplace=True)
        #    df_db['Date'] = pd.to_datetime(df_db['Date'])
        #    df_db=df_db[df_db['Date'].dt.date != datetime.date.today()]
        #    df_db['Availability']=df_db['avail_num']/df_db['avail_den']*100

        #    #df_ses=dcc_session.groupby([pd.DatetimeIndex(dcc_session['Date']).strftime('%Y-%m-%d'),'hwEventType','MNO']).sum()
        #    #df_ses.drop_duplicates(inplace=True)
        #    #pd.DataFrame(df_ses).reset_index(inplace=True)
        #    #df_ses['Date'] = pd.to_datetime(df_ses['Date'])
        #    #df_ses=df_ses[df_ses['Date'].dt.date != datetime.date.today()]
        #    #df_ses['Session_SR']=round(df_ses['hwSuccessSessionCount']/df_ses['hwSessionTotalNumber']*100,2)
        #elif value == 'H':
        df = df_b_avail
        df_db=df_db_avail
        df_c = cpu
        #    #df_ses=dcc_session

        #if cli == '1Q':
        #    filt = df['Date'].dt.date >= (datetime.date.today() - datetime.timedelta(90))
        #    filt_db = df_db['Date'].dt.date >= (datetime.date.today() - datetime.timedelta(90))
        #    #filt_ses = df_ses['Date'].dt.date >= (datetime.date.today() - datetime.timedelta(90))
        #elif cli == '1M':
        #    filt = df['Date'].dt.date >= (datetime.date.today() - datetime.timedelta(30))
        #    filt_db = df_db['Date'].dt.date >= (datetime.date.today() - datetime.timedelta(30))
        #    #filt_ses = df_ses['Date'].dt.date >= (datetime.date.today() - datetime.timedelta(30))
        #elif cli == '2W':
        #    filt = df['Date'].dt.date >= (datetime.date.today() - datetime.timedelta(14))
        #    filt_db = df_db['Date'].dt.date >= (datetime.date.today() - datetime.timedelta(14))
        #    #filt_ses = df_ses['Date'].dt.date >= (datetime.date.today() - datetime.timedelta(14))
        #elif cli == '1W':
        #    filt = df['Date'].dt.date >= (datetime.date.today() - datetime.timedelta(7))
        #    filt_db = df_db['Date'].dt.date >= (datetime.date.today() - datetime.timedelta(7))
        #    #filt_ses = df_ses['Date'].dt.date >= (datetime.date.today() - datetime.timedelta(7))
        #elif cli == '3D':
        #    filt = df['Date'].dt.date >= (datetime.date.today() - datetime.timedelta(3))
        #    filt_db = df_db['Date'].dt.date >= (datetime.date.today() - datetime.timedelta(3))
        #    #filt_ses = df_ses['Date'].dt.date >= (datetime.date.today() - datetime.timedelta(3))
        #df = df[filt]
        #df_db = df_db[filt_db]
        #df_ses = df_db[filt_ses]
        for mno in ['AZF','BKC']:
            trace.append(go.Scatter(x=df[df['MNO'] == mno]['Date'],
                                    y=df[df['MNO'] == mno]['Availability'],
                                    mode='lines',
                                    opacity=0.7,
                                    name=mno,
                                    textposition='bottom center'))
            nese = ''

        traces = [trace]
        data = [val for sublist in traces for val in sublist]


        for mno in ['AZF','BKC']:
            trace_db.append(go.Scatter(x=df_db[df_db['MNO'] == mno]['Date'],
                                    y=df_db[df_db['MNO'] == mno]['Availability'],
                                    mode='lines',
                                    opacity=0.7,
                                    name=mno,
                                    textposition='bottom center'))
            nese2 = ''

        traces_db = [trace_db]
        data_db = [val for sublist in traces_db for val in sublist]

        for mno in ['AZF','BKC']:
            for kpi in ['max']:
                trace_cpu.append(go.Scatter(x=df_c[df_c['MNO'] == mno]['Date'],
                                    y=df_c[df_c['MNO'] == mno][kpi],
                                    mode='lines',
                                    opacity=0.7,
                                    name=mno,
                                    textposition='bottom center'))
            nese_cpu = ''

        traces_cpu = [trace_cpu]
        data_cpu = [val for sublist in traces_cpu for val in sublist]

        #for mno in ['AZF','BKC']:
        #    for k in ['BWControlling','OnlineVoice','OnlineSMS','OnlineGRPS','RTBPChargingRT']:
        #        trace_ses.append(go.Scatter(x=df_ses[(df_ses['MNO'] == mno) & (df_ses['hwEventType']==k)]['Date'],
        #                                y=df_ses[(df_ses['MNO'] == mno) & (df_ses['hwEventType']==k)]['Session_SR'],
        #                                mode='lines',
        #                                opacity=0.7,
        #                                name=mno+'_'+k,
        #                                textposition='bottom center'))
        #        nese3 = ''

        #traces_ses = [trace_ses]
        #data_ses = [val for sublist in traces_ses for val in sublist]
        # Define Figure
        # STEP 4
   
   ########### Gauge chart ###########
#        fig = go.Figure(go.Indicator(
#    domain = {'x': [0, 1], 'y': [0, 1]},
#    value = df[(df['MNO'] == 'AZF') & (df['Date']==df['Date'].unique()[-1])]['Availability'].values[0],
#    mode = "gauge+number+delta",
#    title = {'text': "Availability"},
#    #size=150,
#    delta = {'reference': df[(df['MNO'] == 'AZF') & (df['Date']==df['Date'].unique()[-2])]['Availability'].values[0]},
#    gauge = {'axis': {'range': [None, 100]},
#             'steps' : [
#                 {'range': [0, 90], 'color': "lightgray"},
#                 {'range': [90, 100], 'color': "gray"}],
#             'threshold' : {'line': {'color': "red", 'width': 4}, 'thickness': 0.75, 'value': 99}}),
#             )
#        fig.update_layout(
#    paper_bgcolor="lightgray",
#    height=300,  # Added parameter
#)

        #figure=fig

    #######################################
        figure = go.Figure(data=data, layout=layout)
        figure.update_yaxes(automargin=True)

        figure_db = go.Figure(data=data_db, layout=layout)
        figure_db.update_yaxes(automargin=True)

        figure_cpu = go.Figure(data=data_cpu, layout=layout)
        figure_cpu.update_yaxes(automargin=True)

        #figure_ses = go.Figure(data=data_ses, layout=layout)
        #figure_ses.update_yaxes(automargin=True)

        return figure, nese, figure_db, nese2,figure_cpu,nese_cpu#, figure_ses, nese3
