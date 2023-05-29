import pandas as pd
from dash.dependencies import Input, Output, State
import plotly.graph_objs as go
import time, datetime
import os

KPIs = {  # 'Data Traffic':'Data Traffic, GB',
    'Call Setup Time': 'Call Setup Time',
    'Paging SR': 'Paging SR',
    'Attach SR': 'Attach SR',
    'Location Update SR': 'Location Update SR',
    'PDP_Ctx and Bearer Setup SR': 'PDP_Ctx and Bearer Setup SR',
    'SRVCC Handover SR':'SRVCC Handover SR',
    'Initial Registration':'Initial Registration',
    'VoLTE Call Setup time':'VoLTE Call Setup time'


    # 'Data':'Data Traffic, GB'
}

DRILLDOWN_FILTERS = [
    'Technology',
    'Region'
]

yesterday = datetime.date.today() - datetime.timedelta(15)


def prepare_data():

###################
    files = pd.date_range(start=yesterday,periods=16, freq='24H').strftime('%Y-%m-%d').tolist()
    pag_per_lac,mo_mt_ccr,usn_kpi,traf,scsf,srvcc,cem_cst=([] for i in range(7))
    for kpi in ['pag_per_lac','mo_mt_ccr','usn_kpi','traf','scsf', 'srvcc','cem_cst']:
        for i in files:
            if os.path.isfile(os.path.join('/disk2/support_files/archive/core','core_new_' + i + '.h5')):
                try:
                    if kpi=='traf':
                        eval(kpi).append(pd.read_hdf(os.path.join(r'/disk2/support_files/archive/core','core_new_' + i + '.h5'),kpi))
                    elif kpi=='usn_kpi':
                        eval(kpi).append(pd.read_hdf(os.path.join(r'/disk2/support_files/archive/core','core_new_' + i + '.h5'),kpi).\
                                         groupby(['Date','mode']).sum().reset_index())
                    else :
                        eval(kpi).append(pd.read_hdf(os.path.join(r'/disk2/support_files/archive/core','core_new_' + i + '.h5'),kpi).\
                                         groupby('Date').sum().reset_index())
                except:
                    1
#####################
    #df = pd.read_hdf('/disk2/support_files/archive/core_inputs.h5', '/cst', where='Date>=yesterday')
    usn=pd.concat(usn_kpi)
    iki=usn[usn['mode']=='Gb mode']
    uc=usn[usn['mode']=='Iu mode']
    dord=usn[usn['mode']=='S1 mode']
    df_usn=iki.merge(uc,on='Date',suffixes={'_2G','_3G'}).merge(dord,on='Date')

    df_cst = pd.concat(pag_per_lac)
    df_cst = df_cst.merge(df_usn, on='Date')
    df_cst = df_cst.merge(pd.concat(mo_mt_ccr), on='Date')
    df_cst = df_cst.merge(pd.concat(scsf),on='Date')
    df_cst = df_cst.merge(pd.concat(srvcc),on='Date')
    df_cst = df_cst.merge(pd.concat(cem_cst),on='Date',how='left')
    df_cst['V2V_Call_Setup_time']=df_cst['V2V_MO_Connection_Delay']

    g={}
    for i in df_cst.columns[:-1]: 
        g[i]='sum'
    g['V2V_Call_Setup_time']='mean'
    del g['Date']
    for u in ['h', 'd']:            
        if u == 'd':
            df_cstt = df_cst.groupby(pd.DatetimeIndex(df_cst['Date']).strftime('%Y-%m-%d')).agg(g)
            df_cstt.reset_index(inplace=True)
            df_cstt['Date'] = pd.to_datetime(df_cstt['Date'])
        else:
            df_cstt = df_cst

        df_cstt['2G_CST'] = df_cstt['TwoG Call Setup time'] / df_cstt['TwoG Call Completion']
        df_cstt['3G_CST'] = df_cstt['ThreeG Call Setup time'] / df_cstt['ThreeG Call Completion']
        df_cstt['2G_Pag_SR'] = df_cstt['TwoG_CS Paging SR num'] / df_cstt['TwoG_CS Paging SR den'] * 100
        df_cstt['3G_Pag_SR'] = df_cstt['ThreeG_CS Paging SR num'] / df_cstt['ThreeG_CS Paging SR den'] * 100

        df_cstt['2G_Attach_SR'] = df_cstt['Data accept_2G']/df_cstt['Data attach_2G']*100
        df_cstt['3G_Attach_SR'] = df_cstt['Data accept_3G']/df_cstt['Data attach_3G']*100
        df_cstt['4G_Attach_SR'] = df_cstt['Data accept']/df_cstt['Data attach']*100
        df_cstt['2G-3G'] = (df_cstt['MS init PDP_bear context act suc_2G']+ df_cstt['MS init PDP_bear context act suc_3G']) / (
                    df_cstt['MS init PDP_bear context act_2G']+df_cstt['MS init PDP_bear context act_3G']) * 100
        df_cstt['4G'] = df_cstt['MS init PDP_bear context act suc'] / df_cstt['MS init PDP_bear context act']* 100
        
        df_cstt['VLR'] = df_cstt['VLR Location Update Success'] / df_cstt['VLR Location Update Requests'] * 100
        df_cstt['Roaming'] = df_cstt['Roaming Location Update Success'] / df_cstt['Roaming Location Update Requests'] * 100
        df_cstt['V2V_Call_Setup_time']=df_cstt['V2V_Call_Setup_time']/1000
        df_cstt['Initial_Registration_SR']=df_cstt["Initial register success"]/df_cstt["Initial register attempt"]*100
        df_cstt["LTE_to_UMTS_eSRVCC_Handover_SR"]=df_cstt["LTE_to_UMTS SRVCC num"]/df_cstt["LTE_to_UMTS SRVCC den"]*100

        df_cstt = df_cstt[
            ['Date', '2G_CST', '3G_CST', '2G_Pag_SR', '3G_Pag_SR', '2G_Attach_SR', '3G_Attach_SR', 'VLR', 'Roaming',
             '2G-3G', '4G','V2V_Call_Setup_time','Initial_Registration_SR','LTE_to_UMTS_eSRVCC_Handover_SR']]
        kpi = pd.DataFrame(
            df_cstt[['2G_CST', '3G_CST', '2G_Pag_SR', '3G_Pag_SR', '2G_Attach_SR', '3G_Attach_SR', 'VLR', 'Roaming',
                     '2G-3G', '4G','V2V_Call_Setup_time','Initial_Registration_SR','LTE_to_UMTS_eSRVCC_Handover_SR']].unstack()).reset_index()
        dfd = pd.DataFrame(df_cstt['Date'])
        dfd = dfd.append(dfd)
        dfd['Technology'] = kpi[kpi['level_0'].str.contains('CST')]['level_0'].replace(
            {'2G_CST': '2G', '3G_CST': '3G'}).values
        dfd['Call Setup Time'] = kpi[kpi['level_0'].str.contains('CST')][0].values
        dfd.loc[dfd['Technology'] == '2G', 'Paging SR'] = kpi[kpi['level_0'] == '2G_Pag_SR'][0].values
        dfd.loc[dfd['Technology'] == '3G', 'Paging SR'] = kpi[kpi['level_0'] == '3G_Pag_SR'][0].values
        dfd.loc[dfd['Technology'] == '2G', 'Attach SR'] = kpi[kpi['level_0'] == '2G_Attach_SR'][0].values
        dfd.loc[dfd['Technology'] == '3G', 'Attach SR'] = kpi[kpi['level_0'] == '3G_Attach_SR'][0].values
        dfd.loc[dfd['Technology'] == '4G', 'Attach SR'] = kpi[kpi['level_0'] == '4G_Attach_SR'][0].values
        dfd = dfd.append(pd.DataFrame(dfd['Date']))
        dfd.loc[dfd['Technology'].isnull(), 'Technology'] = \
            kpi[(kpi['level_0'].str.contains('VLR')) | (kpi['level_0'].str.contains('Roaming'))]['level_0'].values
        dfd.loc[dfd['Technology'] == 'VLR', 'Location Update SR'] = kpi[kpi['level_0'] == 'VLR'][0].values
        dfd.loc[dfd['Technology'] == 'Roaming', 'Location Update SR'] = kpi[kpi['level_0'] == 'Roaming'][0].values
        dfd = dfd.append(pd.DataFrame(df_cstt['Date']))
        dfd = dfd.append(pd.DataFrame(df_cstt['Date']))
        dfd.loc[dfd['Technology'].isnull(), 'Technology'] = \
            kpi[(kpi['level_0'].str.contains('2G-3G')) | (kpi['level_0'].str.contains('4G'))]['level_0'].values
        dfd.loc[dfd['Technology'] == '2G-3G', 'PDP_Ctx and Bearer Setup SR'] = kpi[kpi['level_0'] == '2G-3G'][0].values
        dfd.loc[dfd['Technology'] == '4G', 'PDP_Ctx and Bearer Setup SR'] = kpi[kpi['level_0'] == '4G'][0].values

        dfd.loc[dfd['Technology'] == '4G', 'SRVCC Handover SR'] = kpi[kpi['level_0'] == 'LTE_to_UMTS_eSRVCC_Handover_SR'][0].values
        dfd.loc[dfd['Technology'] == '4G', 'Initial Registration'] = kpi[kpi['level_0'] == 'Initial_Registration_SR'][0].values
        dfd.loc[dfd['Technology'] == '4G', 'VoLTE Call Setup time'] = kpi[kpi['level_0'] == 'V2V_Call_Setup_time'][0].values
        
        dfd.reset_index(inplace=True)
        dfd.sort_values(by='Date', inplace=True)
        if u == 'h':
            df_h = dfd
        else:
            df_d = dfd
    # traffic part
    #azrc = pd.read_hdf('/disk2/support_files/archive/core_inputs.h5', '/data', where='Date>=yesterday')
    azrc = pd.concat(traf)
    #azrc_total = azrc.groupby('Date').sum()
    azrc_total=azrc.copy()
    azrc_total.reset_index(inplace=True)
    azrc_total['Date'] = pd.to_datetime(azrc_total['Date'], format='%d.%m.%Y %H:%M')
    #azrc_total.insert(1, 'MNO', 'AZRC')
    azrc_total['MNO'] = 'Azerconnect'
    df_traf = pd.concat([azrc, azrc_total])
    df_traf['Total data traffic, TB'] = (df_traf['2G/3G DL'] + df_traf['2G/3G UL'] + df_traf['4G UL'] + df_traf['4G DL']) / 1024 / 1024 / 1024
    df_traf = df_traf.groupby(['Date', 'MNO']).sum()
    df_traf.reset_index(inplace=True)
    df_traf = df_traf[['Date', 'MNO', 'Total data traffic, TB']]
    df_traf.sort_values(by='Date', inplace=True)
    df_traf.iloc[:, 2:] = df_traf.iloc[:, 2:].apply(lambda x: round(x, 2))
    return df_h.drop_duplicates(), df_d.drop_duplicates(), df_traf.drop_duplicates()

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
df_h, df_d, df_traf = prepare_data()
print('come here')


def register_callback(dashapp):
    @dashapp.callback(Output('whole', 'data'),
                      [Input('interval-component', 'n_intervals')]
                      )
    def refresh_data(n):
        global df_h, df_d, df_traf
        df_h, df_d, df_traf = prepare_data()
        print('why not enter here2222')

    @dashapp.callback(Output('Data Traffic', 'figure'),
                      Output('Data Traffic_nese', 'children'),
                      [Input('radio-items', 'value')],
                      [Input('interval-component2', 'n_intervals')],
                      [Input('radio-items2', 'value')]
                      )
    def update_traffic(value, n, cli):
        global df_h, df_d, df_traf
        df_h, df_d, df_traf = prepare_data()
        print('why not enter here2222')
        trace = []
        if value == 'D':
            df = df_traf.groupby([pd.DatetimeIndex(df_traf['Date']).strftime('%Y-%m-%d'), 'MNO']).sum()
            pd.DataFrame(df).reset_index(inplace=True)
            df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')
            df=df[df['Date'].dt.date != datetime.date.today()]
        elif value == 'H':
            df = df_traf

        if cli == '1Q':
            filt = df['Date'].dt.date >= (datetime.date.today() - datetime.timedelta(90))
        elif cli == '1M':
            filt = df['Date'].dt.date >= (datetime.date.today() - datetime.timedelta(30))
        elif cli == '2W':
            filt = df['Date'].dt.date >= (datetime.date.today() - datetime.timedelta(14))
        elif cli == '1W':
            filt = df['Date'].dt.date >= (datetime.date.today() - datetime.timedelta(7))
        elif cli == '3D':
            filt = df['Date'].dt.date >= (datetime.date.today() - datetime.timedelta(3))
        df = df[filt]
        for mno in ['Azerfon', 'Bakcell', 'Azerconnect']:
            trace.append(go.Scatter(x=df[df['MNO'] == mno]['Date'],
                                    y=df[df['MNO'] == mno]['Total data traffic, TB'],
                                    mode='lines',
                                    opacity=0.7,
                                    name=mno,
                                    textposition='bottom center'))
            nese = ''

        traces = [trace]
        data = [val for sublist in traces for val in sublist]
        # Define Figure
        # STEP 4

        figure = go.Figure(data=data, layout=layout)
        figure.update_yaxes(automargin=True)

        return figure, nese

    def update_graph(kpi):
        @dashapp.callback(Output(kpi, 'figure'),
                          Output(kpi + '_nese', 'children'),
                          [Input('radio-items', 'value')],
                          [Input('interval-component2', 'n_intervals')],
                          [Input('radio-items2', 'value')]
                          )
        def update_figure(value, n, cli):

            global df_h, df_d

            print('why not enter here')

            if value == 'D':
                df = df_d
                df=df[df['Date'].dt.date != datetime.date.today()]
            elif value == 'H':
                df = df_h
            if cli == '1Q':
                filt = df['Date'].dt.date >= (datetime.date.today() - datetime.timedelta(90))
            elif cli == '1M':
                filt = df['Date'].dt.date >= (datetime.date.today() - datetime.timedelta(30))
            elif cli == '2W':
                filt = df['Date'].dt.date >= (datetime.date.today() - datetime.timedelta(14))
            elif cli == '1W':
                filt = df['Date'].dt.date >= (datetime.date.today() - datetime.timedelta(7))
            elif cli == '3D':
                filt = df['Date'].dt.date >= (datetime.date.today() - datetime.timedelta(3))

            trace = []

            if kpi in ['Call Setup Time','Paging SR']:
                techs = ['2G', '3G']
                numb = {0: '2G', 1: '3G'}
            elif kpi in ['Location Update SR']:
                techs = ['VLR', 'Roaming']
                numb = {0: 'VLR', 1: 'Roaming'}
            elif kpi in ['Attach SR']:
                techs = ['2G', '3G','4G']
                numb = {0: '2G', 1: '3G',2:'4G'}
            elif kpi in ['PDP_Ctx and Bearer Setup SR']:
                techs = ['2G-3G', '4G']
                numb = {0: '2G-3G', 1: '4G'}
            elif kpi in ['SRVCC Handover SR','Initial Registration','VoLTE Call Setup time']:
                techs = ['4G']
                numb = {0:'4G'}

                

            df = df[filt]
            for tech in techs:
                trace.append(go.Scatter(x=df[df['Technology'] == tech]['Date'],
                                        y=round(df[df['Technology'] == tech][KPIs[kpi]], 2),
                                        mode='lines',
                                        opacity=0.7,
                                        name=tech,
                                        textposition='bottom center'))

                nese = ''
                # STEP 3
            traces = [trace]
            data = [val for sublist in traces for val in sublist]
            # Define Figure
            # STEP 4

            figure = go.Figure(data=data, layout=layout)
            figure.update_yaxes(automargin=True)

            return figure, nese

    for kpi in KPIs.keys():
        update_graph(kpi)