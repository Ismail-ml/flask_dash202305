import pandas as pd
from dash.dependencies import Input, Output, State
import plotly.graph_objs as go
import numpy as np
import time,datetime

KPIs={'Availability':'Cell_Availability',
      'CS Traffic':'CS_Traffic(Kerl)',
      'PS traffic':'PS_Traffic(TB)',
      'Call_setup_sr':'Call_Setup_SR',
      'Call_dr':'Call_Drop_Rate',
      'Data SR':'Data_Setup_SR',
      'Data DR':'Data_Drop_Rate',
      'DL Thrp':'DL_Average_user_thrp(kbps)',
      'CSFB SR':'4G_CSFB_SR'}

DRILLDOWN_FILTERS = [
    'Technology',
    'Region'

]

def prepare_data(df_2G,df_3G,df_4G):
    df_2G.reset_index(inplace=True)
    df_2G['Call_Setup_SR'] = 100 * (1 - df_2G['cssr_num1'] / df_2G['cssr_den1']) * df_2G['cssr_num2'] / df_2G[
        'cssr_den2'] * df_2G['cssr_num3'] / df_2G['cssr_den3']
    df_2G['Call_Drop_Rate'] = df_2G['drop_rate_num'] / df_2G['drop_rate_den'] * 100
    df_2G['Cell_Availability'] = (df_2G['cell_avail_num'] + df_2G['cell_avail_blck_num']) / (
            df_2G['cell_avail_den'] - df_2G['cell_avail_blck_den']) * 100
    df_2G['CS_Traffic(Kerl)'] = df_2G['cs_traffic_erl'] / 1000
    df_2G['PS_Traffic(TB)'] = df_2G['ps_traffic_mb'] / 1024 / 1024
    df_2G['Technology'] = '2G'

    df_3G.reset_index(inplace=True)
    df_3G['Call_Setup_SR'] = 100 * df_3G['voice_sr_num1'] / df_3G['voice_sr_den1'] * df_3G['voice_sr_num2'] / df_3G[
        'voice_sr_den2']
    df_3G['Call_Drop_Rate'] = 100 * df_3G['voice_dr_num'] / df_3G['voice_dr_den']
    df_3G['Cell_Availability'] = 100 * (df_3G['cell_avail_num'] + df_3G['cell_avail_blck_num']) / (
            df_3G['cell_avail_den'] - df_3G['cell_avail_blck_den'])
    df_3G['CS_Traffic(Kerl)'] = df_3G['cs_traf'] / 1000
    df_3G['PS_Traffic(TB)'] = df_3G['ps_traf'] / 1024 / 1024
    df_3G['Data_Setup_SR'] = 100 * df_3G['hsdpa_sr_num'] / df_3G['hsdpa_sr_den']
    df_3G['Data_Drop_Rate'] = 100 * df_3G['hsdpa_dr_num'] / df_3G['hsdpa_dr_den']
    df_3G['DL_Average_user_thrp(kbps)'] = df_3G['hsdpa_thrp_num'] / df_3G['hsdpa_thrp_den']
    df_3G['Technology'] = '3G'

    df_4G.reset_index(inplace=True)
    df_4G['Data_Setup_SR'] = 100 * df_4G['rrc_sr_num'] / df_4G['rrc_sr_den'] * df_4G['rab_sr_num'] / df_4G[
        'rab_sr_den']
    df_4G['4G_CSFB_SR'] = 100 * df_4G['csfb_sr_num'] / df_4G['csfb_sr_den']
    df_4G['4G_CSFB_SR'] = 100 * df_4G['csfb_sr_num'] / df_4G['csfb_sr_den']
    df_4G['Cell_Availability'] = 100 * (df_4G['cell_avail_num'] + df_4G['cell_avail_blck_num']) / (
            df_4G['cell_avail_den'] - df_4G['cell_avail_blck_den'])
    df_4G['PS_Traffic(TB)'] = (df_4G['dl_ps_traf'] + df_4G['ul_ps_traf']) / 1024 / 1024
    df_4G['DL_Average_user_thrp(kbps)'] = df_4G['dl_thrp_num'] / df_4G['dl_thrp_den']
    df_4G['Data_Drop_Rate'] = 100 * df_4G['dcr_num'] / df_4G['dcr_den']
    df_4G['4G_UL_Average_user_thrp(kbps)'] = df_4G['ul_thrp_num'] / df_4G['ul_thrp_den']
    df_4G['Call_Setup_SR'] = 100 * df_4G['volte_sr_num'] / df_4G['volte_sr_den']
    df_4G['Call_Drop_Rate'] = 100 * df_4G['volte_dr_num'] / df_4G['volte_dr_den']
    #df_4G['Volte_PS_Traffic(TB)'] = (df_4G['volte_dl_ps_traf'] + df_4G['volte_ul_ps_traf'])/ 1024 / 1024
    df_4G['CS_Traffic(Kerl)'] = df_4G['volte_cs_traf']/1000
    df_4G['Technology'] = '4G'

    df = pd.concat([df_2G, df_3G, df_4G])
    df.reset_index(inplace=True)
    df['Date'] = pd.to_datetime(df['Date'], format="%d.%m.%Y")
    df.sort_values(by='Date', inplace=True)
    return df.to_json()
yesterday = datetime.date.today() - datetime.timedelta(31)
def read_files():
    df_2G_raw = pd.read_hdf(r'/disk2/support_files/archive/combined_bsc.h5',
                            '/twoG',where='Date>=yesterday')
    df_3G_raw = pd.read_hdf(r'/disk2/support_files/archive/combined_bsc.h5',
                            '/threeG',where='Date>=yesterday')
    df_4G_raw = pd.read_hdf(r'/disk2/support_files/archive/combined_bsc.h5',
                            '/fourG',where='Date>=yesterday')
    df_4G_raw2 = pd.read_hdf(r'/disk2/support_files/archive/combined_bsc.h5',
                            '/fourGn', where='Date>=yesterday')
    df_4G_raw=df_4G_raw.merge(df_4G_raw2,on=['Date','Vendor','Region'],how='left').drop_duplicates()

    for i in ['D', 'H']:
        if i == 'D':
            df_2G = df_2G_raw[df_2G_raw['Date'].dt.date<datetime.date.today()].groupby(pd.DatetimeIndex(df_2G_raw[df_2G_raw['Date'].dt.date<datetime.date.today()]['Date']).strftime('%d.%m.%Y')).sum()
            df_3G = df_3G_raw[df_3G_raw['Date'].dt.date<datetime.date.today()].groupby(pd.DatetimeIndex(df_3G_raw[df_3G_raw['Date'].dt.date<datetime.date.today()]['Date']).strftime('%d.%m.%Y')).sum()
            df_4G = df_4G_raw[df_4G_raw['Date'].dt.date<datetime.date.today()].groupby(pd.DatetimeIndex(df_4G_raw[df_4G_raw['Date'].dt.date<datetime.date.today()]['Date']).strftime('%d.%m.%Y')).sum()
            df_D = prepare_data(df_2G, df_3G, df_4G)
            df_2G_reg = df_2G_raw[df_2G_raw['Date'].dt.date<datetime.date.today()].groupby(['Region', pd.DatetimeIndex(df_2G_raw[df_2G_raw['Date'].dt.date<datetime.date.today()]['Date']).strftime('%d.%m.%Y')]).sum()
            df_3G_reg = df_3G_raw[df_3G_raw['Date'].dt.date<datetime.date.today()].groupby(['Region', pd.DatetimeIndex(df_3G_raw[df_3G_raw['Date'].dt.date<datetime.date.today()]['Date']).strftime('%d.%m.%Y')]).sum()
            df_4G_reg = df_4G_raw[df_4G_raw['Date'].dt.date<datetime.date.today()].groupby(['Region', pd.DatetimeIndex(df_4G_raw[df_4G_raw['Date'].dt.date<datetime.date.today()]['Date']).strftime('%d.%m.%Y')]).sum()
            df_D_reg = prepare_data(df_2G_reg, df_3G_reg, df_4G_reg)
        else:
            df_2G = df_2G_raw.groupby('Date').sum()
            df_3G = df_3G_raw.groupby('Date').sum()
            df_4G = df_4G_raw.groupby('Date').sum()
            df_H = prepare_data(df_2G, df_3G, df_4G)
            df_2G_reg = df_2G_raw.groupby(['Region', 'Date']).sum()
            df_3G_reg = df_3G_raw.groupby(['Region', 'Date']).sum()
            df_4G_reg = df_4G_raw.groupby(['Region', 'Date']).sum()
            df_H_reg = prepare_data(df_2G_reg, df_3G_reg, df_4G_reg)
    return df_D,df_D_reg,df_H,df_H_reg

y={'Availability':None,
      'CS Traffic':None,
      'PS traffic':None,
      'Call_setup_sr':None,
      'Call_dr':None,
      'Data SR':None,
      'Data DR':None,
      'DL Thrp':None,
      'CSFB SR':None,
      'Availability_int':None,
      'CS Traffic_int':None,
      'PS traffic_int':None,
      'Call_setup_sr_int':None,
      'Call_dr_int':None,
      'Data SR_int':None,
      'Data DR_int':None,
      'DL Thrp_int':None,
      'CSFB SR_int':None}
z=0
layout = {
          'template':'plotly_dark',
          'margin':dict(l=2, r=2, t=5, b=0),
            'font':dict(size=8),
            'yaxis': {'title': '','showgrid': False},
            'xaxis': {'title': '','showgrid': False,
                      'type':'date'
                      },
          'clickmode':'event+select',
          'legend':{'itemclick':'toggle'}
            }
df_D, df_D_reg, df_H, df_H_reg = read_files()

def register_callback(dashapp):
    @dashapp.callback(Output('whole', 'data'),
                      [Input('interval-component', 'n_intervals')]
                      )
    def refresh_data(n):
        global df_D, df_D_reg, df_H, df_H_reg
        df_D, df_D_reg, df_H, df_H_reg = read_files()

    def update_graph(kpi):
        @dashapp.callback(Output(kpi, 'figure'),
                          Output(kpi + '_nese', 'children'),
                          [Input(kpi, 'clickData')],
                          [Input('radio-items', 'value')],
                          [Input('interval-component2', 'n_intervals')],
                          [Input('radio-items2', 'value')]
                          )
        def update_figure(restyle_data,value, n,cli):
            global y, z, df_D, df_D_reg, df_H, df_H_reg
            #if n!=None:
             #   print(n,'n')
             #   print(z,'z')
            #if n > z and kpi == 'Availability':
             #   df_D, df_D_reg, df_H, df_H_reg = read_files()
              #  for i in y.keys():
              #      y[i] = None
              #  z = n
            if value == 'D':
                df = pd.read_json(df_D)
                print('df assigned')
            elif value == 'H':
                df = pd.read_json(df_H)
            if cli == '1M': filt= df['Date'].dt.date>=(datetime.date.today()-datetime.timedelta(30))
            elif cli == '2W': filt= df['Date'].dt.date>=(datetime.date.today()-datetime.timedelta(14))
            elif cli == '1W': filt = df['Date'].dt.date >= (datetime.date.today() - datetime.timedelta(7))
            elif cli == '3D': filt = df['Date'].dt.date >= (datetime.date.today() - datetime.timedelta(3))
            trace = []

            #if kpi in ['CS Traffic', 'Call_setup_sr', 'Call_dr']:
            #    techs = ['2G', '3G']
            #    numb = {0: '2G', 1: '3G'}
            #elif
            if kpi in ['Data SR', 'Data DR', 'DL Thrp']:
                techs = ['3G', '4G']
                numb = {0: '3G', 1: '4G'}
            #elif kpi in ['Availability', 'PS traffic']:
            elif kpi in ['Availability', 'PS traffic', 'CS Traffic', 'Call_setup_sr', 'Call_dr']:
                techs = ['2G', '3G', '4G']
                numb = {0: '2G', 1: '3G', 2: '4G'}
            elif kpi in ['CSFB SR']:
                techs = ['4G']
                numb = {0: '4G'}
            print('restyle = ', restyle_data, 'and clicks=', kpi)
            print('y interval=', 'value = ', value)
            df = df[filt]
            if restyle_data == None :
                for tech in techs:
                    trace.append(go.Scatter(x=df[df['Technology'] == tech]['Date'],
                                            y=round(df[df['Technology'] == tech][KPIs[kpi]],2),
                                            mode='lines',
                                            opacity=0.7,
                                            name=tech,
                                            textposition='bottom center'))

                nese = ''
                # STEP 3
            elif restyle_data != None :
                # print('bura geldi',DRILLDOWN_FILTERS[0],drilldown_values[0])
                kk = restyle_data['points'][0]['curveNumber']
                # numb = {0: '2G', 1: '3G', 2: '4G'}
                if value == 'D':
                    df = pd.read_json(df_D_reg)
                elif value == 'H':
                    df = pd.read_json(df_H_reg)
                if cli == '1M':
                    filt = df['Date'].dt.date >= (datetime.date.today() - datetime.timedelta(30))
                elif cli == '2W':
                    filt = df['Date'].dt.date >= (datetime.date.today() - datetime.timedelta(14))
                elif cli == '1W':
                    filt = df['Date'].dt.date >= (datetime.date.today() - datetime.timedelta(7))
                elif cli == '3D':
                    filt = df['Date'].dt.date >= (datetime.date.today() - datetime.timedelta(3))
                df = df[filt]
                df = df[df[DRILLDOWN_FILTERS[0]] == numb[kk]]

                for r in np.sort(df[DRILLDOWN_FILTERS[1]].unique()):
                    trace.append(go.Scatter(x=df[df[DRILLDOWN_FILTERS[1]] == r]['Date'],
                                            y=round(df[df[DRILLDOWN_FILTERS[1]] == r][KPIs[kpi]],2),
                                            mode='lines',
                                            opacity=0.7,
                                            name=r,
                                            textposition='bottom center'))
                nese = '_' + numb[kk]
            traces = [trace]
            data = [val for sublist in traces for val in sublist]
            # Define Figure
            # STEP 4

            figure = go.Figure(data=data, layout=layout)
            figure.update_yaxes(automargin=True)


            return figure, nese

    for kpi in KPIs.keys():
        update_graph(kpi)
