import pandas as pd
import numpy as np
from dash.dependencies import Input, Output, State
from plotly import tools
from plotly.subplots import make_subplots
import dash
from dash import Dash, dcc, html,dash_table
import plotly.express as px
import plotly.graph_objs as go
from datetime import datetime as dt
import datetime
import numpy as np
#from dateutil.relativedelta import relativedelta
from datetime import datetime as dt
import os

Formula = {'2G': {
    'Call Setup SR': "100*(1-df['cssr_num1']/df['cssr_den1'])*df['cssr_num2']/df['cssr_den2']*df['cssr_num3']/df['cssr_den3']",
    'Call Drop Rate': "df['drop_rate_num']/df['drop_rate_den']*100",
    'Call Block Rate': "df['call_block_rate_num']/df['call_block_rate_den']*100",
    'SDCCH Drop Rate': "df['sdcch_drop_rate_num']/df['sdcch_drop_rate_den']*100",
    'SDCCH Block Rate': "df['sdcch_block_rate_num']/df['sdcch_block_rate_den']*100",
    'Handover SR': "df['hosr_num']/df['hosr_den']*100",
    'Cell Availability excl blck': "(df['cell_avail_num']+df['cell_avail_blck_num'])/(df['cell_avail_den']-df['cell_avail_blck_den'])*100",
    'TCH Availability': "df['tch_avail_num']/df['tch_avail_den']*100",
    'SDCCH Availability': "df['sdcch_avail_num']/df['sdcch_avail_den']*100",
    'TBF Est SR': "df['tbf_est_sr_num']/df['tbf_est_sr_den']*100",
    'TBF Drop Rate': "df['tbf_drop_rate_num']/df['tbf_drop_rate_den']*100",
    'CS Traffic, Erl': "df['cs_traffic_erl']",
    'PS Traffic, MB': "df['ps_traffic_mb']",
    'Combined Thrp, Kbps': "df['comb_thrp_num']/df['comb_thrp_den']",
    'Call Setup Fails': "df['cssr_num1']+df['cssr_den2']+df['cssr_den3']-df['cssr_num2']-df['cssr_num3']",
    'Call Setup Den': "df['cssr_den1']+df['cssr_den2']+df['cssr_den3']",
    'Call Drops': "df['drop_rate_num']",
    'Call Blocks': "df['call_block_rate_num']",
    'SDCCH Drops': "df['sdcch_drop_rate_num']",
    'SDCCH Blocks': "df['sdcch_block_rate_num']",
    'TBF Drops': "df['tbf_drop_rate_num']",
    'TBF Est fails': "df['tbf_est_sr_den']-df['tbf_est_sr_num']",
    'Handover fails': "df['hosr_den']-df['hosr_num']", 'Handover attempts': "df['hosr_den']"
},
    '3G': {
        'Voice Call Setup SR': "100*df['voice_sr_num1']/df['voice_sr_den1']*df['voice_sr_num2']/df['voice_sr_den2']",
        'Voice Call DR': "100*df['voice_dr_num']/df['voice_dr_den']",
        'HSDPA RAB SR': "100*df['hsdpa_sr_num']/df['hsdpa_sr_den']",
        'HSUPA RAB SR': "100*df['hsupa_sr_num']/df['hsupa_sr_den']",
        'HSDPA Drop Rate': "100*df['hsdpa_dr_num']/df['hsdpa_dr_den']",
        'HSUPA Drop Rate': "100*df['hsupa_dr_num']/df['hsupa_dr_den']",
        'CS Soft HOSR': "100*df['cs_sho_ho_num']/df['cs_sho_ho_den']",
        'PS Soft HOSR': "100*df['ps_sho_ho_num']/df['ps_sho_ho_den']",
        'CS IRAT HOSR': "100*df['cs_inter_rat_ho_num']/df['cs_inter_rat_ho_den']",
        'CS InterFreq HOSR': "100*df['cs_inter_freq_ho_num']/df['cs_inter_freq_ho_den']",
        'PS InterFreq HOSR': "100*df['ps_inter_freq_ho_num']/df['ps_inter_freq_ho_den']",
        'Cell Availability excl blck': "100*(df['cell_avail_num']+df['cell_avail_blck_num'])/(df['cell_avail_den']-df['cell_avail_blck_den'])",
        'Average HSDPA user thrp, Kbps': "df['hsdpa_thrp_num']/df['hsdpa_thrp_den']",
        'CS Traffic, Erl': "df['cs_traf']",
        'PS Traffic, MB': "df['ps_traf']",
 #       'Interference':"10*np.log10(df['rtwp_num']/df['rtwp_den'])",
        'Call Setup fails': "df['voice_sr_den1']+df['voice_sr_den2']-df['voice_sr_num1']-df['voice_sr_num2']",
        'Call drops': "df['voice_dr_num']",
        'Call Setup attempts': "df['voice_sr_den1']+df['voice_sr_den2']",
        'HSDPA Setup fails': "df['hsdpa_sr_den']-df['hsdpa_sr_num']",
        'HSDPA Setup attempts': "df['hsdpa_sr_den']",
        'HSDPA drops': "df['hsdpa_dr_num']",
        'HSUPA Setup fails': "df['hsupa_sr_den']-df['hsupa_sr_num']",
        'HSUPA Setup attempts': "df['hsupa_sr_den']",
        'HSUPA drops': "df['hsupa_dr_num']",
        'CS Soft HO fails': "df['cs_sho_ho_den']-df['cs_sho_ho_num']",
        'CS Soft HO attempts': "df['cs_sho_ho_den']",
        'PS Soft HO fails': "df['ps_sho_ho_den']-df['ps_sho_ho_num']",
        'PS Soft HO attempts': "df['ps_sho_ho_den']",
        'CS IRAT HO attempts': "df['cs_inter_rat_ho_den']",
        'CS IRAT HO fails': "df['cs_inter_rat_ho_den']-df['cs_inter_rat_ho_num']",
        'CS InterFreq HO attempts': "df['cs_inter_freq_ho_den']",
        'CS InterFreq HO fails': "df['cs_inter_freq_ho_den']-df['cs_inter_freq_ho_num']",
        'PS InterFreq HO attempts': "df['ps_inter_freq_ho_den']",
        'PS InterFreq HO fails': "df['ps_inter_freq_ho_den']-df['ps_inter_freq_ho_num']"},
    '4G': {'RRC Setup SR': "100*df['rrc_sr_num']/df['rrc_sr_den']",
           'RAB Setup SR': "100*df['rab_sr_num']/df['rab_sr_den']",
           'Session Setup SR': "100*df['rrc_sr_num']/df['rrc_sr_den']*df['rab_sr_num']/df['rab_sr_den']",
           'CSFB SR': "100*df['csfb_sr_num']/df['csfb_sr_den']",
           'Session Drop Rate': "100*df['dcr_num']/df['dcr_den']",
           'Cell Availability excl blck': "100*(df['cell_avail_num']+df['cell_avail_blck_num'])/(df['cell_avail_den']-df['cell_avail_blck_den'])",
           'IntraFreq HOSR': "100*df['intra_freq_ho_num']/df['intra_freq_ho_den']",
           '4G-3G IRAT HOSR': "100*df['irat_ho_num']/df['irat_ho_den']",
           'DL Traffic, MB': "df['dl_ps_traf']",
           'UL Traffic, MB': "df['ul_ps_traf']",
           'Total Traffic, MB': "df['dl_ps_traf']+df['ul_ps_traf']",
#           'Interference':"df['rtwp']",
           'DL Throughput, Kbps': "df['dl_thrp_num']/df['dl_thrp_den']",
           'UL Throughput, Kbps': "df['ul_thrp_num']/df['ul_thrp_den']",
           'RRC Setup fails': "df['rrc_sr_den']-df['rrc_sr_num']",
           'RRC Setup attempts': "df['rrc_sr_den']",
           'RAB Setup fails': "df['rab_sr_den']-df['rab_sr_num']",
           'RAB Setup attempts': "df['rab_sr_den']",
           'Session drops': "df['dcr_num']",
           'Session Setup fails': "df['rrc_sr_den']-df['rrc_sr_num']+df['rab_sr_den']-df['rab_sr_num']",
           'CSFB fails': "df['csfb_sr_den']-df['csfb_sr_num']",
           'CSFB attempts': "df['csfb_sr_den']",
           'IntraFreq HO fails': "df['intra_freq_ho_den']-df['intra_freq_ho_num']",
           'IntraFreq HO attempts': "df['intra_freq_ho_den']",
           '4G-3G IRAT HO fails': "df['irat_ho_den']-df['irat_ho_num']",
           '4G-3G IRAT HO attempts': "df['irat_ho_den']"}}


today=datetime.date.today()-datetime.timedelta(15)
needed=dt.strptime(dt.strftime(dt.now()-datetime.timedelta(hours=169),'%d.%m.%y %H:00'),'%d.%m.%y %H:00')
def read_files():
    site = pd.read_csv('/home/ismayil/flask_dash/support_files/anomality_detection/site_ongoing_anomalies.csv')
    higher = pd.read_csv('/home/ismayil/flask_dash/support_files/anomality_detection/higher_ongoing_anomalies.csv')
    traffic = pd.read_csv('/home/ismayil/flask_dash/support_files/anomality_detection/traffic_ongoing_anomalies.csv')
    tx=pd.read_csv('/home/ismayil/flask_dash/support_files/anomality_detection/tx_ongoing_anomalies.csv')
    tx['CollectionTime']=pd.to_datetime(tx['CollectionTime'])
    mapping={'IG27': 'Basic Traffic Statistics', 'IG30014':'PTN Ethernet-Port Performance','IG30024':'Laser SDH Performance',
            'IG30029':'IF Port SDH Performance','IG41022':'PFM_RMONG_PRISTAT', 'IGMSTP':'MSTP Performance','IGRTN':'RTN MW Link'}

    site['Date']=pd.to_datetime(site['Date'])
    traffic['Date']=pd.to_datetime(traffic['Date'])
    higher['Date']=pd.to_datetime(higher['Date'])
    tx['file'].replace(mapping,inplace=True)
    #site_trend=pd.read_hdf(r'D:\disk_E\Desktop\Export\Python\1\Anomality\anomalies.h5', '/trend')
    #higher_trend=pd.read_hdf(r'D:\disk_E\Desktop\Export\Python\1\Anomality\anomalies.h5', '/trend_higher')
    

    site=site.rename(columns={'Fails_and_drops_average_15_day':'Fails and drops average 15 day',
                                    'Fails_and_drops_last_hour':'Fails and drops last hour'})[['Date', 'tech', 'Site_name',
                                    'Vendor', 'NE', 'Region','variable','value', 'threshold','Fails and drops average 15 day',
                                    'Fails and drops last hour','Cell Availability', 'c_days','status']]
    higher = higher[higher['variable'].isin(
        ['Call Setup SR', 'Call Drop Rate', 'Call Block Rate', 'SDCCH Drop Rate', 'SDCCH Block Rate', 'Handover SR',
         'TBF Est SR', 'TBF Drop Rate',
         'Voice Call Setup SR', 'Voice Call DR', 'HSDPA RAB SR', 'HSUPA RAB SR', 'HSDPA Drop Rate', 'HSUPA Drop Rate',
         'CS Soft HOSR', 'PS Soft HOSR', 'CS IRAT HOSR', 'CS InterFreq HOSR', 'PS InterFreq HOSR',
         'Average HSDPA user thrp, Kbps',
         'Session Setup SR', 'CSFB SR', 'Session Drop Rate', 'IntraFreq HOSR', '4G-3G IRAT HOSR', 'DL Throughput, Kbps',
         'UL Throughput, Kbps'])]
    higher = higher.rename(columns={'Fails_and_drops_average_15_day': 'Fails and drops average 15 day',
                                        'Fails_and_drops_last_hour': 'Fails and drops last hour'})[
        ['Date', 'tech', 'Vendor',
         'NE', 'variable', 'value', 'threshold', 'Fails and drops average 15 day',
         'Fails and drops last hour', 'Cell Availability', 'c_days','status']]

    #del temporary_df
    return site, higher, traffic, tx
site, higher, traffic,tx =read_files()

layout = {

          'template':'plotly_dark',
          'margin':dict(l=2, r=2, t=2, b=10),
          'font':dict(size=12),
          'yaxis': {'title': '','showgrid': False},
          'xaxis': {'title': '','showgrid': False},
          'clickmode':'event+select',
          'legend':dict(
                    yanchor="top",
                    orientation="h",
                    y=1.1,
                    xanchor="center",
                    x=0.50
                        )
            }
click_state=None
click_state2=None
ns=None
def register_callback(dashapp):
    @dashapp.callback(
                    Output('table', 'columns'),
                    Output('table','data'),
                    Output('table2', 'columns'),
                    Output('table2', 'data'),
                    Output('live-table', 'export_format'),
                    Output('live-table', 'columns'),
                    Output('live-table', 'data'),
                    Output('graph', 'figure'),
                    Output('graph2', 'figure'),
                    Output("live-table", "style_cell"),
                    Output('table2_title', 'children'),
                    Output('table3_title', 'children'),
                    Output('neighbor-table', 'export_format'),
                    Output('neighbor-table', 'columns'),
                    Output('neighbor-table', 'data'),
                    Output("neighbor-table", "style_cell"),
                    Output('table4_title', 'children'),
                    Input('table','selected_cells'),
                    Input('live-table', 'selected_cells'),
                    Input('live-table', 'derived_virtual_row_ids'),
                    Input('live-table', 'active_cell'),
                    Input('live-table', 'selected_row_ids'),
                    Input('live-table', 'filter_query'),
                    Input('interval-component', 'n_intervals')
                  )
    def plot_map_graph(cell,cell_live,row_ids,active_cell,selected_row_ids,filt_stg,n):
        ''' Draw traces of the feature 'Value' based one the currently selected stocks '''
        # STEP 1
        # Load data and sort/filter
        mapping_back = {'Basic Traffic Statistics': 'IG27', 'PTN Ethernet-Port Performance': 'IG30014','Laser SDH Performance': 'IG30024',
                        'IF Port SDH Performance': 'IG30029', 'PFM_RMONG_PRISTAT': 'IG41022','MSTP Performance': 'IGMSTP', 'RTN MW Link': 'IGRTN'}
        global click_state,click_state2,ns,site, higher, traffic,tx
        if n!=ns:
            site, higher, traffic, tx = read_files()
            cell=None
            cell_live=None
            active_row_id=None
            #table2 = []
            #table3 = []
            ns=n

        figure2=go.Figure(data=[])
        print(filt_stg)

        style_cell={'textAlign': 'center',
                                                                                  'color': 'black', 'fontSize': 11,
                                                                                  'height':'auto',
                                                                                  'whiteSpace':'normal',
                                                                                  "font-family": "Comic Sans MS"}
        style_cell2 = {'textAlign': 'center',
                      'color': 'black', 'fontSize': 11,
                      'height': 'auto',
                      'whiteSpace': 'normal',
                      "font-family": "Comic Sans MS"}
        if cell!=None:
            if (click_state!=cell[0]['column']) | (click_state2!=cell[0]['row']):
            #active_row_id==None
                cell_live=None
                filt_stg = ""
            print(cell)
            print(cell[0]['row'])
            print('click_state=',click_state)
            click_state=cell[0]['column']
            click_state2=cell[0]['row']
            if cell[0]['row']==0:
                filt=cell[0]['column_id']

                if filt=='>5':
                    new = site[site['c_days'] > 5]
                else: new=site[site['c_days']==filt]
                new = new[new['variable'].isin(
                    ['Call Setup SR', 'Call Drop Rate', 'Call Block Rate', 'SDCCH Drop Rate', 'SDCCH Block Rate',
                     'Handover SR', 'TBF Est SR','TBF Drop Rate','Voice Call Setup SR', 'Voice Call DR', 'HSDPA RAB SR', 'HSUPA RAB SR', 'HSDPA Drop Rate',
                     'HSUPA Drop Rate','CS Soft HOSR','PS Soft HOSR', 'CS IRAT HOSR', 'CS InterFreq HOSR', 'PS InterFreq HOSR',
                     'Average HSDPA user thrp, Kbps','Session Setup SR','CSFB SR', 'Session Drop Rate', 'IntraFreq HOSR', '4G-3G IRAT HOSR', 'DL Throughput, Kbps',
                     'UL Throughput, Kbps'])] #,'Interference'
                new2=new.pivot_table(index=['tech','variable'], columns='Region', values='c_days', aggfunc='count').reset_index()
                new2['Total'] = new2.iloc[:, 1:].sum(axis=1)
                col2=[{'name': i, 'id': i} for i in new2.columns]
                table2=new2.sort_values(by='Total',ascending=False).to_dict('records')
                new['Date'] = [dt.strftime(new['Date'].iloc[i].to_pydatetime(), '%d.%m.%Y %H:%M') for i in
                                  range(len(new['Date']))]
                new.rename(columns={'tech': 'Tech', 'variable': 'KPI', 'value': 'Last hour', 'threshold': 'Threshold',
                                    'c_days': 'Ongoing periods, hours'}, inplace=True)
                new['Delta from threshold,%']=round(abs(new['Last hour']-new['Threshold'])/new['Threshold']*100,2)
                new=new[['Date','Tech','Vendor','Site_name','NE','Region','KPI','Last hour','Threshold','Delta from threshold,%','Fails and drops last hour',
                         'Fails and drops average 15 day', 'Cell Availability','Ongoing periods, hours']]
                new['id'] = new['Site_name'] + '&' + new['KPI']
                row_ids = new['id']
                export="csv"
                export_2='csv'
                tblcols=[{'name': i, 'id': i} for i in new.columns if (i != 'id') & (i != 'status')]
                style_cell['width']=str(100/(len(new.columns)-1))+'%'
                table3 = round(new,2).to_dict('rows')
                t2_title='Summary of anomalies for RAN sites'
                t3_title='Details of RAN site level anomalies'
            elif cell[0]['row']==2:

                filt = cell[0]['column_id']
                if filt == '>5':
                    new = traffic[traffic['c_days'] > 5]
                    #new = traffic[traffic['c_days'] > 5]
                else:
                    new = traffic[traffic['c_days'] == filt]
                    #new = traffic[traffic['c_days'] == filt]
                new2=new.pivot_table(index='tech', columns='Region', values='c_days', aggfunc='count').reset_index()
                new2['Total'] = new2.iloc[:, 1:].sum(axis=1)
                col2=[{'name': i, 'id': i} for i in new2.columns]
                table2 = new2.sort_values(by='Total',ascending=False).to_dict('records')
                #new['id'] = new['region']+'&'+new['kpi']+'&'+new['tech']
                new['id'] = new['Region'] + '&' + new['variable'] + '&' + new['tech']
                new.rename(columns={'tech':'Tech','variable':'KPI','value':'Last hour','c_days':'Ongoing periods','mean':'Average value'},inplace=True)
                new['Date'] = [dt.strftime(new['Date'].iloc[i].to_pydatetime(), '%d.%m.%Y %H:%M')
                                         for i in
                                         range(len(new['Date']))]
                row_ids = new['id']
                export = "csv"
                new=new[['Date','Region','Tech','KPI','Last hour','Average value','Ongoing periods','id']]
                tblcols = [{'name': i, 'id': i} for i in new.columns if i != 'id']
                style_cell['width'] = str(100 / (len(new.columns) - 1)) + '%'
                table3 = round(new,2).to_dict('rows')
                t2_title = 'Summary of anomalies for RAN Region level traffic'
                t3_title = 'Details of RAN Region level traffic anomalies'
            elif cell[0]['row']==3:
                filt = cell[0]['column_id']
                if filt == '>5':
                    new = tx[tx['c_days'] > 5]
                else:
                    new = tx[tx['c_days'] == filt]
                new2 = new.pivot_table(index='variable', columns='file', values='c_days', aggfunc='count').reset_index()
                new2['Total'] = new2.iloc[:, 1:].sum(axis=1)
                col2 = [{'name': i, 'id': i} for i in new2.columns]
                table2 = new2.sort_values(by='Total', ascending=False).to_dict('records')
                new['tech']='hecne'
                new['CollectionTime'] = [dt.strftime(new['CollectionTime'].iloc[i].to_pydatetime(), '%d.%m.%Y %H:%M') for i in
                               range(len(new['CollectionTime']))]
                new.rename(columns={'CollectionTime':'Date','Q1':'Average 14 days','delta':'Delta from average','c_days':'Ongoing periods','variable':'KPI',
                                    'value':'Last 15 minute'},inplace=True)
                new=new[['file','Date','DeviceName','ResourceName','KPI','Last 15 minute','Average 14 days','Delta from average','Ongoing periods']]
                new['id'] = new['file'] + '&' + new['ResourceName'] + '&' + new['KPI']
                row_ids = new['id']
                export = "csv"
                tblcols = [{'name': i, 'id': i} for i in new.columns if (i != 'id') & (i != 'file')]
                style_cell['width'] = str(100 / (len(new.columns) - 1)) + '%'
                table3 = round(new, 2).to_dict('rows')
                t2_title = 'Summary of anomalies for TX network'
                t3_title = 'Details of TX network anomalies'
            else:
                filt = cell[0]['column_id']
                if filt == '>5':
                    new = higher[higher['c_days'] > 5]
                else:
                    new = higher[higher['c_days'] == filt]
                new2 = new.pivot_table(index=['tech','variable'], columns='NE', values='c_days',
                                       aggfunc='count').reset_index()
                new2['Total']=new2.iloc[:,1:].sum(axis=1)
                col2 = [{'name': i, 'id': i} for i in new2.columns]
                table2 = new2.sort_values(by='Total',ascending=False).to_dict('records')
                new['Date'] = [dt.strftime(new['Date'].iloc[i].to_pydatetime(), '%d.%m.%Y %H:%M') for i in
                               range(len(new['Date']))]
                new.rename(columns={'tech': 'Tech', 'variable': 'KPI', 'value': 'Last hour', 'threshold': 'Threshold',
                                    'c_days': 'Ongoing periods'}, inplace=True)
                new['Delta from threshold'] = round(abs(new['Last hour'] - new['Threshold']) / new['Threshold'] * 100,
                                                    2)
                new = new[['Date', 'Tech', 'Vendor', 'NE', 'KPI', 'Last hour', 'Threshold',
                           'Delta from threshold','Fails and drops last hour', 'Fails and drops average 15 day',
                            'Cell Availability', 'Ongoing periods']]
                new['id'] = new['NE']+'&'+new['KPI']
                row_ids = new['id']
                export = "csv"
                tblcols = [{'name': i, 'id': i} for i in new.columns if i != 'id']
                style_cell['width'] = str(100 / (len(new.columns) - 1)) + '%'
                table3 = round(new, 2).to_dict('rows')
                t2_title = 'Summary of anomalies for RAN BSC/RNC/Regions'
                t3_title = 'Details of RAN BSC/RNC/Region level anomalies'
            ###########################
            if cell_live!=None:
                selected_id_set = set(selected_row_ids or [])
                new.set_index('id', inplace=True, drop=False)
                if row_ids is None:
                    dff = df
                    # pandas Series works enough like a list for this to be OK
                    row_ids = new['id']
                else:
                    dff = new.loc[row_ids]
                active_row_id = active_cell['row_id'] if active_cell else None
                print(active_row_id, ' active row id')
                site_n=active_row_id.split('&')[0]
                variable=active_row_id.split('&')[1]
                if cell[0]['row']!=3:
                    technology = new.loc[active_row_id, 'Tech']
                mapping={'2G':['twoG','BSC_name'],'3G':['threeG','RNC_name'],'4G':['fourG','Region']}
                if cell[0]['row']==0:
                    trend=pd.read_hdf('/disk2/support_files/archive/anomality/anomalies.h5', '/trend',
                                      where='Site_name==site_n and variable==variable')
                    kpi = 'value'
                    threshold = new.loc[active_row_id, 'Threshold']
                    files = pd.date_range(end=site['Date'].iloc[-1], periods=3,
                                           freq='24H').strftime("%Y-%m-%d").tolist()
                    mapping={'2G':'/twoG','3G':'/threeG','4G':'/fourG'}

                    h = []
                    for i in files:
                        try:
                            if os.path.isfile('/disk2/support_files/archive/ran/'+i + '.h5'):
                                h.append(pd.read_hdf('/disk2/support_files/archive/ran/'+i + '.h5',
                                                    mapping[new.loc[active_row_id, 'Tech']],
                                                    where='Site_name==site_n'))
                        except: 
                            1
                    df = pd.concat(h)
                    df = df.groupby(['Date', 'Site_name']).sum().reset_index()
                    df['Date'] = [dt.strftime(df['Date'].iloc[i].to_pydatetime(), '%d.%m.%Y %H:%M') for i in
                                  range(len(df['Date']))]
                    for i in Formula[new.loc[active_row_id, 'Tech']].keys():
                        df[i] = eval(Formula[new.loc[active_row_id, 'Tech']][i])
                    figure2 = make_subplots(specs=[[{"secondary_y": False}]])
                    figure2.add_trace(go.Scatter(x=df.loc[:, 'Date'], y=df.loc[:, variable],  # Main KPI
                                                 mode='lines', opacity=0.7, name=active_row_id), secondary_y=False)

                    #########################################
                    ### add neighboring site information
                    tracker=pd.read_csv('/home/ismayil/flask_dash/support_files/tracker.csv')
                    new = tracker[['SITE_ID', 'Old Site ID']]
                    import numpy as np

                    def haversine(lon1, lat1, lon2, lat2):
                        """
                        Calculate the great circle distance between two points
                        on the earth (specified in decimal degrees)
                        """

                        # Convert decimal degrees to Radians:
                        lon1 = np.radians(lon1.values)
                        lat1 = np.radians(lat1.values)
                        lon2 = np.radians(lon2.values)
                        lat2 = np.radians(lat2.values)

                        # Implementing Haversine Formula:
                        dlon = np.subtract(lon2, lon1)
                        dlat = np.subtract(lat2, lat1)

                        a = np.add(np.power(np.sin(np.divide(dlat, 2)), 2),
                                   np.multiply(np.cos(lat1),
                                               np.multiply(np.cos(lat2),
                                                           np.power(np.sin(np.divide(dlon, 2)), 2))))
                        c = np.multiply(2, np.arcsin(np.sqrt(a)))
                        r = 6371

                        return c * r
                    an=site.copy()
                    try:
                        #tracker=tracker[pd.to_numeric(tracker['Long'], errors='coerce').notnull()]
                        #tracker=tracker[pd.to_numeric(tracker['Lat'], errors='coerce').notnull()]
                        #tracker=tracker[(tracker.Long.str.isnumeric()) & (tracker.Lat.str.isnumeric())]
                        #tracker=tracker[(tracker['Long']!='0') & (tracker['Lat']!='0')]
                        new['dist'] = haversine(tracker.loc[tracker['SITE_ID'] == site_n[1:8], 'Long'],
                                                tracker.loc[tracker['SITE_ID'] == site_n[1:8], 'Lat'], tracker['Long'],tracker['Lat'])
                        new.sort_values(by='dist',inplace=True)
                        #new=new[new['dist']<10]
                        new=new.iloc[1:31]
                        #an['c_days']=['Ongoing '+str(round(int(i),0))+' periods' for i in an['c_days']]
                        #an=an[an['Site_name'].apply(lambda x: x[1:8]).isin(new['SITE_ID'])][:30].pivot_table(index=['Site_name','Cell Availability'],
                                                                                            #                columns=['variable','c_days'],values='value').reset_index()
                        #an.columns=an.columns.map(lambda x: list(x)[0] + ',' + str(list(x)[1]))
                        an = an[an['Site_name'].apply(lambda x: x[1:8]).isin(new['SITE_ID'])]
                        an['SITE_ID'] = an['Site_name'].apply(lambda x: x[1:8])
                        an = an.merge(new[['SITE_ID', 'dist']], on='SITE_ID')
                        an.drop(columns='SITE_ID',inplace=True)
                        an.rename(columns={'tech': 'Tech', 'variable': 'KPI', 'value': 'Last hour', 'threshold': 'Threshold',
                                    'c_days': 'Ongoing periods, hours','dist':'Distance from site,km'}, inplace=True)
                        an['Delta from threshold,%'] = round(
                            abs(an['Last hour'] - an['Threshold']) / an['Threshold'] * 100, 2)
                        an = an[['Date', 'Tech', 'Site_name', 'NE', 'Distance from site,km','KPI', 'Last hour', 'Threshold',
                                'Delta from threshold,%', 'Fails and drops last hour',
                                'Fails and drops average 15 day', 'Cell Availability', 'Ongoing periods, hours']]
                        an['Date'] = [dt.strftime(an['Date'].iloc[i].to_pydatetime(), '%d.%m.%Y %H:%M') for i in
                                    range(len(an['Date']))]
                        an=an[an['Site_name']!=site_n]

                        export2 = "csv"
                        tblcols_neighbor = [{'name': i, 'id': i} for i in an.columns if (i != 'id') & (i != 'status')]
                        style_cell2['width'] = str(100 / (len(an.columns))) + '%'
                    
                        table_neighbor = round(an, 2).to_dict('rows')
                        t4_title = 'Anomalies for neighbours of '+site_n
                    except Exception as e:
                        tblcols_neighbor=[]
                        table_neighbor=[]
                        t4_title=''
                        export2=''
                        print(e)
                        1

                    ##########################
                    ##########################
                    
                    ##################################### Add alarm information ###############################
                    
                    ###########################
                    ###########################

                elif cell[0]['row']==1:
                    export2 = []
                    tblcols_neighbor=[]
                    table_neighbor=[]
                    t4_title=[]
                    trend = pd.read_hdf('/disk2/support_files/archive/anomality/anomalies.h5', '/trend_higher',
                                        where='NE==site_n and variable==variable')
                    trend=trend.iloc[-15:]
                    kpi='value'
                    threshold = new.loc[active_row_id, 'Threshold']
                    df= pd.read_hdf('/disk2/support_files/archive/combined_bsc.h5', mapping[technology][0],
                                        where=mapping[technology][1]+'==site_n and Date>"2021-08-20 23:00:00"')
                    df=df.groupby(['Date',mapping[technology][1]]).sum().reset_index()
                    df['Date'] = [dt.strftime(df['Date'].iloc[i].to_pydatetime(), '%d.%m.%Y %H:%M') for i in
                                   range(len(df['Date']))]
                    for i in Formula[new.loc[active_row_id, 'Tech']].keys():
                        df[i] = eval(Formula[new.loc[active_row_id, 'Tech']][i])
                    figure2=make_subplots(specs=[[{"secondary_y": False}]])
                    figure2.add_trace(go.Scatter(x=df.loc[:, 'Date'], y=df.loc[:, variable],  # Main KPI
                                                mode='lines', opacity=0.7, name=active_row_id), secondary_y=False)
                
                ################ Traffic part ##############
                elif cell[0]['row']==2:
                    export2 = []
                    tblcols_neighbor=[]
                    table_neighbor=[]
                    t4_title=[]
                    trend = pd.read_csv('/home/ismayil/flask_dash/support_files/anomality_detection/daily_traffic_check.csv')
                    trend.rename(columns={'date':'Date'},inplace=True)
                    trend['Date']=pd.to_datetime(trend['Date'])
                    trend=trend[(trend['region']==site_n) & (trend['tech']==new.loc[active_row_id, 'Tech']) & (trend['kpi']==variable)]
                    trend.sort_values(by='Date',inplace=True)
                    df=trend.copy()
                    trend=trend[trend['Date'].dt.hour==pd.to_datetime(new['Date']).dt.hour.iloc[-1]]
                    #df=trend.copy()
                    #trend=trend.iloc[-15:]
                    kpi='traf'
                    threshold = new.loc[active_row_id, 'Average value']
                    df['Date']=pd.to_datetime(df['Date'])
                    print(trend.columns)
                    df['Date'] = [dt.strftime(df['Date'].iloc[i].to_pydatetime(), '%d.%m.%Y %H:%M') for i in
                                   range(len(df['Date']))]
                    ##for i in Formula[new.loc[active_row_id, 'Tech']].keys():
                    ##    df[i] = eval(Formula[new.loc[active_row_id, 'Tech']][i])
                    figure2=make_subplots(specs=[[{"secondary_y": False}]])
                    figure2.add_trace(go.Scatter(x=df.loc[:, 'Date'], y=df.loc[:, 'traf'],  # Main KPI
                                                mode='lines', opacity=0.7, name=active_row_id), secondary_y=False)
                    figure2.add_trace(go.Scatter(x=df.loc[:, 'Date'], y=df.loc[:, 'predict'],  # Main KPI
                                                mode='lines', line = dict(color='red', dash='dot'),opacity=0.8, name='prediction'), secondary_y=False)
                ######################################################
                elif cell[0]['row']==3:
                    export2 = []
                    tblcols_neighbor = []
                    table_neighbor = []
                    t4_title = []
                    print(active_row_id)
                    files = pd.date_range(end=tx['CollectionTime'].iloc[-1], periods=15,
                                          freq='24H').strftime("%d.%m.%Y").tolist()
                    files2 = pd.date_range(end=tx['CollectionTime'].iloc[-1], periods=3,
                                          freq='24H').strftime("%d.%m.%Y").tolist()
                    hour = tx['CollectionTime'].iloc[-1].strftime("%H:%M")
                    kpi=active_row_id.split('&')[2]
                    threshold=0
                    #threshold = new.loc[active_row_id, 'Q1']+3*new.loc[active_row_id, 'Q3']
                    h = []
                    for i in files:
                        if os.path.isfile('/disk2/support_files/archive/tx/tx_' + i + '.h5'):
                            t = datetime.datetime.strptime(i + " " + hour, "%d.%m.%Y %H:%M")
                            h.append(pd.read_hdf('/disk2/support_files/archive/tx/tx_' + i + '.h5',
                                                 mapping_back[site_n], where='CollectionTime==t and ResourceName==variable'))
                    trend = pd.concat(h)
                    tech='hecne'
                    trend.rename(columns={'CollectionTime':'Date'},inplace=True)
                    h2 = []
                    for i in files2:
                        if os.path.isfile('/disk2/support_files/archive/tx/tx_' + i + '.h5'):
                            t = datetime.datetime.strptime(i + " " + hour, "%d.%m.%Y %H:%M")
                            h2.append(pd.read_hdf('/disk2/support_files/archive/tx/tx_' + i + '.h5',
                                                 mapping_back[site_n], where='ResourceName==variable'))
                    trend2 = pd.concat(h2)
                    trend2.sort_values(by='CollectionTime',inplace=True)
                    trend2['CollectionTime'] = [
                        dt.strftime(trend2['CollectionTime'].iloc[i].to_pydatetime(), '%d.%m.%Y %H:%M') for i in
                        range(len(trend2['CollectionTime']))]
                    print(trend2.sample(5))
                    print(kpi)
                    figure2 = make_subplots(specs=[[{"secondary_y": False}]])
                    figure2.add_trace(go.Scatter(x=trend2.loc[:, 'CollectionTime'], y=trend2.loc[:, kpi],  # Main KPI
                                                 mode='lines', opacity=0.7, name=active_row_id), secondary_y=False)
                else:
                    export2 = []
                    tblcols_neighbor = []
                    table_neighbor = []
                    t4_title = []
                    trend= pd.read_csv('/home/ismayil/flask_dash/support_files/anomality_detection/daily_traffic_check.csv')
                    trend.rename(columns={'date':'Date'},inplace=True)
                    trend['Date']=pd.to_datetime(trend['Date'])
                    trend.sort_values(by='Date',inplace=True)
                    new['Date'] = pd.to_datetime(new['Date'])
                    tech = active_row_id.split('&')[2]
                    trend2=trend.loc[(trend['region']==site_n) & (trend['kpi']==variable) & (trend['Tech']==tech)].copy()
                    trend=trend.loc[(trend['region']==site_n) & (trend['kpi']==variable) & (trend['Tech']==tech) & (trend['Date'].dt.hour==new.loc[active_row_id,'Date'].dt.hour)]
                    kpi = 'traf'
                    threshold = trend.iloc[-1]['predict']
                    #trend2=trend2.iloc[-72:]
                    figure2 = make_subplots(specs=[[{"secondary_y": False}]])
                    figure2.add_trace(go.Scatter(x=trend2.loc[:, 'Date'], y=trend2.loc[:, kpi],  # Main KPI
                                                 mode='lines', opacity=0.7, name=active_row_id), secondary_y=False)
                    figure2.add_trace(
                        go.Scatter(x=trend2.loc[:, 'Date'], y=trend2.loc[:, 'predict'],
                                   # Main KPI
                                   mode='lines', opacity=0.7, line=dict(color='firebrick', width=4, dash='dot')
                                   , name='predicted'),
                        secondary_y=False)
                    #figure2.add_trace(
                     #   go.Scatter(x=trend2.loc[:, 'Date'], y=trend2.loc[:, 'upper'],
                      #             # Main KPI
                       #            mode='lines', opacity=0.7, line=dict(color='grey', width=0, dash='dot')
                        #           , name='upper limit',
        #showlegend=False),
         #               secondary_y=False)
          #          figure2.add_trace(
           #             go.Scatter(x=trend2.loc[:, 'Date'], y=trend2.loc[:, 'lower'],
            #                       # Main KPI
             #                      mode='lines', opacity=0.7, line=dict(color='grey', width=0, dash='dot')
              #                     , name='lower limit',fill='tonexty',
        #showlegend=False),
         #               secondary_y=False)
                    figure2.add_trace(go.Scatter(x=trend2.loc[trend2['status']>0, 'Date'],
                                                y=trend2.loc[trend2['status']>0, kpi],  # Main KPI
                                                mode='markers', marker=dict(size=10,color='red'), opacity=0.7,
                                                name='anomality'), secondary_y=False)


                trend['Date'] = [dt.strftime(trend['Date'].iloc[i].to_pydatetime(), '%d.%m.%Y %H:%M') for i in
                               range(len(trend['Date']))]
                #threshold=new.loc[active_row_id,'threshold']
                figure = make_subplots(specs=[[{"secondary_y": False}]])
                figure.add_trace(go.Scatter(x=trend.loc[:, 'Date'], y=trend.loc[:, kpi],  # Main KPI
                                            mode='lines+markers', opacity=0.7, name=active_row_id), secondary_y=False)

                #if kpi!='traf' :
                #    if ('SR' in variable) | ('Kbps' in variable) | ('CUR' in variable) | ('RSL' in variable) | ('TSL' in variable):
                #        abnormal_point=trend[kpi]<threshold
                #    else: abnormal_point=trend[kpi]>threshold
                figure.add_trace(go.Scatter(x=[trend.iloc[-1]['Date']], y=[trend.iloc[-1][kpi]],  # Main KPI
                                            mode='markers',marker=dict(size=40,
                color='red'), opacity=0.7, name='anomality'), secondary_y=False)
                #figure.add_trace(
                #    go.Scatter(x=trend.loc[:, 'Date'], y=[threshold for i in range(len(trend.loc[:, 'Date']))],
                #               # Main KPI
                 #              mode='lines', opacity=0.7, line=dict(color='firebrick', width=4, dash='dot')
                 #              ,name='threshold'),
                 #   secondary_y=False)
                figure.update_layout(layout)
                figure2.update_layout(layout)
            else:
                figure = go.Figure(data=[])
                export2=[]
                tblcols_neighbor = []
                table_neighbor = []
                t4_title=[]
            ################################
        else:
            col2=[]
            table2=[]
            table3=[]
            tblcols=[]
            export=[]
            t2_title=[]
            t3_title=[]
            t4_title=[]
            export2=[]
            tblcols_neighbor=[]
            table_neighbor=[]
            figure = go.Figure(data=[])
        #if cell_live!=None:
        #    print(cell_live)
        #    print('Site name=',new.iloc[cell_live[0]['row']+ num * 10,2])
        #    cell4324=cell_live[0]['row']

        #df_sub=final
        #if click:
        #    trend2 = pd.read_hdf(r'D:\disk_E\Desktop\Export\Python\1\Anomality\August_2021.h5', 'twoG',
        #                        where='Site_name==site_n and Date>"2021-08-13"')
        #    trend2['Date'] = [dt.strftime(trend2['Date'].iloc[i].to_pydatetime(), '%d.%m.%Y %H:%M') for i in
        #                     range(len(trend2['Date']))]
        #    trend2['Call_Drop_Rate']=trend2['drop_rate_num']/trend2['drop_rate_den']*100
        #    figure2 = make_subplots(specs=[[{"secondary_y": False}]])
        #    figure2.add_trace(go.Scatter(x=trend2.loc[:, 'Date'], y=trend2.loc[:, 'Call_Drop_Rate'],  # Main KPI
        #                                mode='lines', opacity=0.7, name=active_row_id), secondary_y=False)
        temporary_df = pd.DataFrame(columns=['Period',1, 2, 3, 4, 5, '>5'])

        site = site[site['variable'].isin(
            ['Call Setup SR', 'Call Drop Rate', 'Call Block Rate', 'SDCCH Drop Rate', 'SDCCH Block Rate',
             'Handover SR', 'TBF Est SR', 'TBF Drop Rate', 'Voice Call Setup SR', 'Voice Call DR', 'HSDPA RAB SR',
             'HSUPA RAB SR', 'HSDPA Drop Rate',
             'HSUPA Drop Rate', 'CS Soft HOSR', 'PS Soft HOSR', 'CS IRAT HOSR', 'CS InterFreq HOSR',
             'PS InterFreq HOSR',
             'Average HSDPA user thrp, Kbps', 'Session Setup SR', 'CSFB SR', 'Session Drop Rate', 'IntraFreq HOSR',
             '4G-3G IRAT HOSR', 'DL Throughput, Kbps',
             'UL Throughput, Kbps'])] #,'Interference'
        combined = pd.DataFrame([site.groupby('c_days').count()['status'], higher.groupby('c_days').count()['status'],
                                 traffic.groupby('c_days').count()['status'],tx.groupby('c_days').count()['status']],
                                index=['site', 'higher', 'traffic','tx']).rename_axis(
            'ongoing_hours', axis='columns').rename_axis('ongoing_hours', axis='columns')
        combined.columns = combined.columns.astype('int')
        temp = combined.loc[:, combined.columns > 5].columns
        combined['>5'] = combined.loc[:, temp].sum(axis=1)
        combined.drop(columns=temp, inplace=True)
        combined.insert(1,'Period',['1 hour','1 hour', '1 hour','15 minute'])
        final = temporary_df.append(combined)
        final = final.reset_index().rename(columns={'index': 'category'})
        #final.rename_axis('ongoing hours',axis='columns',inplace=True)
        final.fillna(0,inplace=True)
        final.category=['RAN sites','RAN BSC/RNC/Region','RAN Region level traffic','MW, SDH, MSTP tx']
        #'tech', 'Absheron', 'Aran', 'Baku', 'Ganja', 'Lankaran', 'Naxchivan', 'Qarabag', 'Quba', 'Sheki'

        col1=[{"name": ["", "Category"], "id": "category"},{"name": ["", "Period"], "id": "Period"},
        *[{'name': ['Ongoing Periods',i], 'id': i} for i in final.columns[2:]]
            ]
        table1=final.to_dict('records')


        return col1,table1,col2,table2,export,tblcols,table3,figure,figure2,style_cell,t2_title,t3_title,export2,tblcols_neighbor,table_neighbor,style_cell2,t4_title
