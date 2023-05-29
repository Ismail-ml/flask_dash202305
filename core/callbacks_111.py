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
    'PDP_Ctx and Bearer Setup SR': 'PDP_Ctx and Bearer Setup SR'
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
    lu_sr,pag,cst,pdp,s1,traf=([] for i in range(6))
    for kpi in ['lu_sr','pag','cst','pdp','s1','traf']:
        for i in files:
            if os.path.isfile(os.path.join('/disk2/support_files/archive/core','core_' + i + '.h5')):
                try:
                    eval(kpi).append(pd.read_hdf(os.path.join(r'/disk2/support_files/archive/core','core_' + i + '.h5'),kpi))
                except:
                    1
#####################
    #df = pd.read_hdf('/disk2/support_files/archive/core_inputs.h5', '/cst', where='Date>=yesterday')
    df_cst = pd.concat(cst)
    #df = pd.read_hdf('/disk2/support_files/archive/core_inputs.h5', '/pag', where='Date>=yesterday')
    df_cst = df_cst.merge(pd.concat(pag), on='Date')
    #df = pd.read_hdf('/disk2/support_files/archive/core_inputs.h5', '/pdp', where='Date>=yesterday')
    df_cst = df_cst.merge(pd.concat(pdp), on='Date')
    #df = pd.read_hdf('/disk2/support_files/archive/core_inputs.h5', '/lu_sr', where='Date>=yesterday')
    df_cst = df_cst.merge(pd.concat(lu_sr), on='Date')
    #df = pd.read_hdf('/disk2/support_files/archive/core_inputs.h5', '/s1', where='Date>=yesterday')
    df_cst = df_cst.merge(pd.concat(s1), on='Date')
    for u in ['h', 'd']:
        if u == 'd':
            df_cstt = df_cst.groupby(pd.DatetimeIndex(df_cst['Date']).strftime('%Y-%m-%d')).sum()
            df_cstt.reset_index(inplace=True)
            df_cstt['Date'] = pd.to_datetime(df_cstt['Date'])
        else:
            df_cstt = df_cst

        df_cstt['2G_CST'] = df_cstt['84148248'] / df_cstt['84148235']
        df_cstt['3G_CST'] = df_cstt['84148278'] / df_cstt['84148265']
        df_cstt['2G_Pag_SR'] = (df_cstt['84152186'] + df_cstt['84152188']) / df_cstt['84152185'] * 100
        df_cstt['3G_Pag_SR'] = (df_cstt['84152192'] + df_cstt['84152190']) / df_cstt['84152189'] * 100
        df_cstt['2G_Attach_SR'] = df_cstt['117454514'] / df_cstt['117454513'] * 100
        df_cstt['3G_Attach_SR'] = df_cstt['117456614'] / df_cstt['117456613'] * 100
        df_cstt['VLR'] = df_cstt['84151990'] / df_cstt['84151989'] * 100
        df_cstt['Roaming'] = df_cstt['84151992'] / df_cstt['84151991'] * 100
        df_cstt['2G-3G'] = (df_cstt['117458514'] + df_cstt['117459414']) / (
                    df_cstt['117458513'] + df_cstt['117459413']) * 100
        df_cstt['4G'] = df_cstt['117495953'] / df_cstt['117495952'] * 100

        df_cstt = df_cstt[
            ['Date', '2G_CST', '3G_CST', '2G_Pag_SR', '3G_Pag_SR', '2G_Attach_SR', '3G_Attach_SR', 'VLR', 'Roaming',
             '2G-3G', '4G']]
        kpi = pd.DataFrame(
            df_cstt[['2G_CST', '3G_CST', '2G_Pag_SR', '3G_Pag_SR', '2G_Attach_SR', '3G_Attach_SR', 'VLR', 'Roaming',
                     '2G-3G', '4G']].unstack()).reset_index()
        dfd = pd.DataFrame(df_cstt['Date'])
        dfd = dfd.append(dfd)
        dfd['Technology'] = kpi[kpi['level_0'].str.contains('CST')]['level_0'].replace(
            {'2G_CST': '2G', '3G_CST': '3G'}).values
        dfd['Call Setup Time'] = kpi[kpi['level_0'].str.contains('CST')][0].values
        dfd.loc[dfd['Technology'] == '2G', 'Paging SR'] = kpi[kpi['level_0'] == '2G_Pag_SR'][0].values
        dfd.loc[dfd['Technology'] == '3G', 'Paging SR'] = kpi[kpi['level_0'] == '3G_Pag_SR'][0].values
        dfd.loc[dfd['Technology'] == '2G', 'Attach SR'] = kpi[kpi['level_0'] == '2G_Attach_SR'][0].values
        dfd.loc[dfd['Technology'] == '3G', 'Attach SR'] = kpi[kpi['level_0'] == '3G_Attach_SR'][0].values
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
    return df_h, df_d, df_traf

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

            if kpi in ['Call Setup Time', 'Attach SR', 'Paging SR']:
                techs = ['2G', '3G']
                numb = {0: '2G', 1: '3G'}
            elif kpi in ['Location Update SR']:
                techs = ['VLR', 'Roaming']
                numb = {0: 'VLR', 1: 'Roaming'}
            elif kpi in ['PDP_Ctx and Bearer Setup SR']:
                techs = ['2G-3G', '4G']
                numb = {0: '2G-3G', 1: '4G'}

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