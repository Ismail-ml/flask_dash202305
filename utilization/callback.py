import pandas as pd
from dash.dependencies import Input, Output, State
from plotly import tools
from plotly.subplots import make_subplots
import dash
import dash_html_components as html
import dash_core_components as dcc
import plotly.express as px
import plotly.graph_objs as go
import dash_table
from datetime import datetime as dt
import datetime
import numpy as np
from dateutil.relativedelta import relativedelta
from datetime import datetime as dt
import os
#'df_rrc': 'RRC',

names_dict = {
    'df_power':'power', 'df_tcp': 'TCP', 'df_prb': 'PRB',  'df_ce': 'CE', 'df_iub': 'Huawei_3Gframe_loss',
'df_ippm': 'Huawei_lte_ippm', 'df_bbu': 'BB_SU', 'df_dch_crc': 'DCH_Discarded_crc_error',
'df_dch_delay': 'DCH_Discarded_high_delay', 'df_dpdcp': 'DL_PDCP_SDU_loss', 'df_drlc': 'DL_RLC_PDU', 'df_hsdsch': 'HS_DSCH_loss',
'df_dthrp': 'HSDPA_thrp_util', 'df_duser': 'HSDPA_User_util', 'df_uthrp': 'HSUPA_thrp_util', 'df_uuser': 'HSUPA_User_util',
'df_iub_loss': 'threeG_IUB_drop_num', 'df_updcp': 'UL_PDCP_SDU_loss', 'df_urlc': 'UL_RLC_PDU', 'df_csfb': 'CSFB_fails',
'df_dpad': 'HSDPA_drops', 'df_dpaf': 'HSDPA_fails', 'df_upad': 'HSUPA_drops', 'df_upaf': 'HSUPA_fails',
'df_sesd': 'Session_drops', 'df_favail': 'fourG_Availability', 'df_frab': 'fourG_RAB_fails', 'df_frrc': 'fourG_RRC_fails',
'df_thavail': 'threeG_Availability', 'df_thvd': 'threeG_voice_drops', 'df_thvf': 'threeG_voice_fails',
'df_twavail': 'twoG_Availability', 'df_twbl': 'twoG_call_blocks', 'df_twvd': 'twoG_call_drops', 'df_sdbl': 'twoG_sdcch_blocks'}
names = ['df_power', 'df_tcp', 'df_prb', 'df_ce', 'df_iub', 'df_ippm', 'df_bbu', 'df_dch_crc',
         'df_dch_delay', 'df_dpdcp', 'df_drlc', 'df_hsdsch', 'df_dthrp', 'df_duser', 'df_uthrp', 'df_uuser',
         'df_iub_loss', 'df_updcp', 'df_urlc', 'df_csfb', 'df_dpad', 'df_dpaf', 'df_upad', 'df_upaf',
         'df_sesd', 'df_favail', 'df_frab', 'df_frrc', 'df_thavail', 'df_thvd', 'df_thvf', 'df_twavail', 'df_twbl',
         'df_twvd', 'df_sdbl']

today=datetime.date.today()-datetime.timedelta(15)
def read_files():

    files = pd.date_range(start=today,periods=15, freq='24H').strftime('%Y-%m-%d').tolist()
    for name in names:
        to_concat = []
        for i in files:
            if os.path.isfile(os.path.join('/disk2/support_files/archive/util', i + '_util.h5')):
                to_concat.append(pd.read_hdf(os.path.join(r'/disk2/support_files/archive/util', i + '_util.h5'),names_dict[name]))
        if name=='df_power':
            df_power = pd.concat(to_concat)
            df_power.rename(columns={'Value': 'Power Utilization'}, inplace=True)
        elif name=='df_tcp':
            df_tcp = pd.concat(to_concat)
            df_tcp.rename(columns={'Value': 'TCP Utilization'}, inplace=True)
        elif name=='df_prb':
            df_prb = pd.concat(to_concat)
            df_prb.rename(columns={'Value': 'PRB Utilization'}, inplace=True)
        elif name=='df_rrc':
            df_rrc = pd.concat(to_concat)
            df_rrc.rename(columns={'Value': 'RRC Utilization'}, inplace=True)
        elif name=='df_ce':
            df_ce = pd.concat(to_concat)
            df_ce.rename(columns={'Value': 'CE Utilization'}, inplace=True)
        elif name=='df_iub':
            df_iub = pd.concat(to_concat)
            df_iub.rename(columns={'Value': '3G Huawei IUB Drops'}, inplace=True)
        elif name=='df_ippm':
            df_ippm = pd.concat(to_concat)
            df_ippm.rename(columns={'Value': '4G IPPM Drops'}, inplace=True)
        elif name=='df_bbu':
            df_bbu = pd.concat(to_concat)
            df_bbu.rename(columns={'Value': 'BB Subunit Utilization'}, inplace=True)
        elif name=='df_dch_crc':
            df_dch_crc = pd.concat(to_concat)
            df_dch_crc.rename(columns={'Value': 'DCH Discarded due to CRC error'}, inplace=True)
        elif name=='df_dch_delay':
            df_dch_delay = pd.concat(to_concat)
            df_dch_delay.rename(columns={'Value': 'DCH Discarded due to high delay'}, inplace=True)
        elif name=='df_dpdcp':
            df_dpdcp = pd.concat(to_concat)
            df_dpdcp.rename(columns={'Value': 'DL PDCP SDU loss'}, inplace=True)
        elif name=='df_drlc':
            df_drlc = pd.concat(to_concat)
            df_drlc.rename(columns={'Value': 'DL RLC PDU loss'}, inplace=True)
        elif name=='df_hsdsch':
            df_hsdsch = pd.concat(to_concat)
            df_hsdsch.rename(columns={'Value': 'HS_DSCH loss'}, inplace=True)
        elif name=='df_dthrp':
            df_dthrp = pd.concat(to_concat)
            df_dthrp.rename(columns={'Value': 'HSDPA Thrp utilization'}, inplace=True)
        elif name=='df_duser':
            df_duser = pd.concat(to_concat)
            df_duser.rename(columns={'Value': 'HSDPA User utilization'}, inplace=True)
        elif name=='df_uthrp':
            df_uthrp = pd.concat(to_concat)
            df_uthrp.rename(columns={'Value': 'HSUPA Thrp utilization'}, inplace=True)
        elif name=='df_uuser':
            df_uuser = pd.concat(to_concat)
            df_uuser.rename(columns={'Value': 'HSUPA User utilization'}, inplace=True)
        elif name=='df_iub_loss':
            df_iub_loss = pd.concat(to_concat)
            df_iub_loss.rename(columns={'Value': '3G Nokia IUB Loss'}, inplace=True)
        elif name=='df_updcp':
            df_updcp = pd.concat(to_concat)
            df_updcp.rename(columns={'Value': 'UL PDCP SDU loss'}, inplace=True)
        elif name=='df_urlc':
            df_urlc = pd.concat(to_concat)
            df_urlc.rename(columns={'Value': 'UL RLC PDU loss'}, inplace=True)
        elif name=='df_csfb':
            df_csfb = pd.concat(to_concat)
            df_csfb.rename(columns={'Value': 'CSFB Fails'}, inplace=True)
        elif name=='df_dpad':
            df_dpad = pd.concat(to_concat)
            df_dpad.rename(columns={'Value': 'HSDPA drops'}, inplace=True)
        elif name=='df_dpaf':
            df_dpaf = pd.concat(to_concat)
            df_dpaf.rename(columns={'Value': 'HSDPA fails'}, inplace=True)
        elif name=='df_upad':
            df_upad = pd.concat(to_concat)
            df_upad.rename(columns={'Value': 'HSUPA drops'}, inplace=True)
        elif name=='df_upaf':
            df_upaf = pd.concat(to_concat)
            df_upaf.rename(columns={'Value': 'HSUPA fails'}, inplace=True)
        elif name=='df_sesd':
            df_sesd = pd.concat(to_concat)
            df_sesd.rename(columns={'Value': 'LTE session drops'}, inplace=True)
        elif name=='df_favail':
            df_favail = pd.concat(to_concat)
            df_favail.rename(columns={'Value': '4G Availability'}, inplace=True)
        elif name=='df_frab':
            df_frab = pd.concat(to_concat)
            df_frab.rename(columns={'Value': '4G RAB fails'}, inplace=True)
        elif name=='df_frrc':
            df_frrc = pd.concat(to_concat)
            df_frrc.rename(columns={'Value': '4G RRC fails'}, inplace=True)
        elif name=='df_thavail':
            df_thavail = pd.concat(to_concat)
            df_thavail.rename(columns={'Value': '3G Availability'}, inplace=True)
        elif name=='df_thvd':
            df_thvd = pd.concat(to_concat)
            df_thvd.rename(columns={'Value': '3G Voice drops'}, inplace=True)
        elif name=='df_thvf':
            df_thvf = pd.concat(to_concat)
            df_thvf.rename(columns={'Value': '3G Voice fails'}, inplace=True)
        elif name=='df_twavail':
            df_twavail = pd.concat(to_concat)
            df_twavail.rename(columns={'Value': '2G Availability'}, inplace=True)
        elif name=='df_twbl':
            df_twbl = pd.concat(to_concat)
            df_twbl.rename(columns={'Value': '2G Call blocks'}, inplace=True)
        elif name=='df_twvd':
            df_twvd = pd.concat(to_concat)
            df_twvd.rename(columns={'Value': '2G Call drops'}, inplace=True)
        elif name=='df_sdbl':
            df_sdbl = pd.concat(to_concat)
            df_sdbl.rename(columns={'Value': '2G SDCCH blocks'}, inplace=True)
    # df_power.reset_index(inplace=True)
    # df_tcp.set_index('Date',inplace=True)
    df_tcpg = df_tcp[df_tcp['TCP Utilization'] > 80].groupby('Date').count()
    df_powerg = df_power[df_power['Power Utilization'] > 60].groupby('Date').count()
    df_prbg = df_prb[df_prb['PRB Utilization'] > 90].groupby('Date').count()
    #df_rrcg = df_rrc[df_rrc['RRC Utilization'] > 90].groupby('Date').count()
    df_ceg = df_ce[df_ce['CE Utilization'] > 90].groupby('Date').count()
    df_iubg = df_iub[df_iub['3G Huawei IUB Drops'] > 300].groupby('Date').count()
    df_ippmg = df_ippm[df_ippm['4G IPPM Drops'] > 300].groupby('Date').count()
    df_bbug = df_bbu[df_bbu['BB Subunit Utilization'] > 90].groupby('Date').count()
    df_dch_crcg = df_dch_crc[df_dch_crc['DCH Discarded due to CRC error'] > 300].groupby('Date').count()
    df_dch_delayg = df_dch_delay[df_dch_delay['DCH Discarded due to high delay'] > 300].groupby('Date').count()
    df_dpdcpg = df_dpdcp[df_dpdcp['DL PDCP SDU loss'] > 300].groupby('Date').count()
    df_drlcg = df_drlc[df_drlc['DL RLC PDU loss'] > 300].groupby('Date').count()
    df_updcpg = df_updcp[df_updcp['UL PDCP SDU loss'] > 300].groupby('Date').count()
    df_urlcg = df_urlc[df_urlc['UL RLC PDU loss'] > 300].groupby('Date').count()
    df_hsdschg = df_hsdsch[df_hsdsch['HS_DSCH loss'] > 300].groupby('Date').count()
    df_dthrpg = df_dthrp[df_dthrp['HSDPA Thrp utilization'] > 90].groupby('Date').count()
    df_duserg = df_duser[df_duser['HSDPA User utilization'] > 90].groupby('Date').count()
    df_uthrpg = df_uthrp[df_uthrp['HSUPA Thrp utilization'] > 95].groupby('Date').count()
    df_uuserg = df_uuser[df_uuser['HSUPA User utilization'] > 90].groupby('Date').count()
    df_iub_lossg = df_iub_loss[df_iub_loss['3G Nokia IUB Loss'] > 300].groupby('Date').count()
    df_csfbg = df_csfb[df_csfb['CSFB Fails'] > 50].groupby('Date').count()
    df_dpadg = df_dpad[df_dpad['HSDPA drops'] > 500].groupby('Date').count()
    df_dpafg = df_dpaf[df_dpaf['HSDPA fails'] > 500].groupby('Date').count()
    df_upadg = df_upad[df_upad['HSUPA drops'] > 500].groupby('Date').count()
    df_upafg = df_upaf[df_upaf['HSUPA fails'] > 500].groupby('Date').count()
    df_sesdg = df_sesd[df_sesd['LTE session drops'] > 500].groupby('Date').count()
    df_favailg = df_favail[df_favail['4G Availability'] < 100].groupby('Date').count()
    df_thavailg = df_thavail[df_thavail['3G Availability'] < 100].groupby('Date').count()
    df_twavailg = df_twavail[df_twavail['2G Availability'] < 100].groupby('Date').count()
    df_frabg = df_frab[df_frab['4G RAB fails'] > 500].groupby('Date').count()
    df_frrcg = df_frrc[df_frrc['4G RRC fails'] > 500].groupby('Date').count()
    df_thvdg = df_thvd[df_thvd['3G Voice drops'] > 50].groupby('Date').count()
    df_thvfg = df_thvf[df_thvf['3G Voice fails'] > 50].groupby('Date').count()
    df_twvdg = df_twvd[df_twvd['2G Call drops'] > 50].groupby('Date').count()
    df_twblg = df_twbl[df_twbl['2G Call blocks'] > 10].groupby('Date').count()
    df_sdblg = df_sdbl[df_sdbl['2G SDCCH blocks'] > 10].groupby('Date').count()
    #df_rrcg,
    final_df = pd.concat([df_twavailg, df_thavailg, df_favailg, df_tcpg, df_powerg, df_prbg,  df_ceg, df_iubg,
                          df_ippmg, df_bbug, df_dch_crcg, df_dch_delayg, df_dpdcpg, df_drlcg,
                          df_updcpg, df_urlcg, df_hsdschg, df_dthrpg, df_duserg, df_uthrpg,
                          df_uuserg, df_iub_lossg, df_csfbg, df_thvfg, df_thvdg, df_twvdg, df_twblg,
                          df_sdblg, df_dpafg, df_upafg, df_frabg, df_frrcg, df_dpadg, df_upadg, df_sesdg], axis=1,
                         join='outer').sort_index()
    final_df.reset_index(inplace=True)
    # print(df_tcpg)
    # print(final_df.head())
    final_df['Date'] = pd.to_datetime(final_df['Date'], format='%d.%m.%Y')
    final_df.sort_values(by='Date', inplace=True)
    final_df = final_df[-15:]
    final_df['Date'] = [dt.strftime(final_df['Date'].iloc[i].to_pydatetime(), '%d.%m.%Y') for i in
                        range(len(final_df['Date']))]
    final_df.set_index('Date', inplace=True)
    #'RRC Utilization',
    final_df = final_df[
        ['2G Availability', '3G Availability', '4G Availability', 'Power Utilization', 'TCP Utilization',
         'PRB Utilization',     'CE Utilization', '3G Huawei IUB Drops', '4G IPPM Drops',
         'BB Subunit Utilization', 'DCH Discarded due to CRC error', 'DCH Discarded due to high delay',
         'DL PDCP SDU loss', 'DL RLC PDU loss', 'UL PDCP SDU loss', 'UL RLC PDU loss', 'HS_DSCH loss',
         'HSDPA Thrp utilization', 'HSDPA User utilization', 'HSUPA Thrp utilization', 'HSUPA User utilization',
         '3G Nokia IUB Loss', 'CSFB Fails', '3G Voice fails', '3G Voice drops', '2G Call drops',
         '2G Call blocks', '2G SDCCH blocks', 'HSDPA fails', 'HSUPA fails', '4G RAB fails', '4G RRC fails',
         'HSDPA drops', 'HSUPA drops', 'LTE session drops']].T
    final_df.fillna(0, inplace=True)
    final_df.reset_index(inplace=True)
    #'# of Sites',
    final_df.insert(1, 'Measurement Unit', ['# of Cells', '# of Cells', '# of Cells', '# of Cells', '# of Cells',
                                            '# of Cells',       '# of Sites', '# of Sites', '# of Sites',
                                            '# of Site-Subunit', '# of Sites', '# of Sites',
                                            '# of Sites', '# of Sites', '# of Sites', '# of Sites', '# of Sites',
                                            '# of Sites', '# of Sites', '# of Sites', '# of Site-Subunit',
                                            '# of Sites', '# of Cells', '# of Cells', '# of Cells', '# of Cells',
                                            '# of Cells', '# of Cells', '# of Cells', '# of Cells', '# of Cells',
                                            '# of Cells',
                                            '# of Cells', '# of Cells', '# of Cells'])
    #'>90%',
    final_df.insert(2, 'Threshold',
                    ['<100', '<100', '<100', '>60%', '>70%', '>90%',      '>90%', '>300', '>300', '>90%',
                     '>300', '>300', '>300', '>300', '>300', '>300', '>300', '>90%', '>90%', '>95%', '>90%', '>300',
                     '>50', '>50', '>50', '>50', '>10', '>10', '>500', '>500', '>500', '>500', '>500', '>500', '>500'])
    final_df.rename(columns={'index':'KPI'},inplace=True)
    second_table_cols = final_df.columns[-16:].values.copy()
    second_table_cols[0] = 'NE'
    return final_df, second_table_cols
#90,
targets=[100,100,100,60,70,90,      90,300,300,90,300,300,300,300,300,300,300,90,90,95,90,300,50,50,50,50,
         10,10,500,500,500,500,500,500,500]

table={0:'twoG_Availability', 1:'threeG_Availability', 2:'fourG_Availability',3:'power',4:'TCP',5:'PRB',
          6:'RRC',7:'CE',8:'Huawei_3Gframe_loss',9:'Huawei_lte_ippm',10:'BB_SU',11:'DCH_Discarded_crc_error',
       12:'DCH_Discarded_high_delay',13:'DL_PDCP_SDU_loss',14:'DL_RLC_PDU',15:'UL_PDCP_SDU_loss',16:'UL_RLC_PDU',
       17:'HS_DSCH_loss',18:'HSDPA_thrp_util',19:'HSDPA_User_util',20:'HSUPA_thrp_util',21:'HSUPA_User_util',
       22:'threeG_IUB_drop_num',23:'CSFB_fails',24:'threeG_voice_fails',25:'threeG_voice_drops',26:'twoG_call_drops',
       27:'twoG_call_blocks',28:'twoG_sdcch_blocks',29:'HSDPA_fails',30:'HSUPA_fails',31:'fourG_RAB_fails',
       32:'fourG_RRC_fails',33:'HSDPA_drops',34:'HSUPA_drops',35:'Session_drops'}
KPI_name=['2G Availability', '3G Availability', '4G Availability', 'Power Utilization', 'TCP Utilization',
         'PRB Utilization', 'RRC Utilization', 'CE Utilization', '3G Huawei IUB Drops', '4G IPPM Drops',
         'BB Subunit Utilization', 'DCH Discarded due to CRC error', 'DCH Discarded due to high delay',
         'DL PDCP SDU loss', 'DL RLC PDU loss', 'UL PDCP SDU loss', 'UL RLC PDU loss', 'HS_DSCH loss',
         'HSDPA Thrp utilization', 'HSDPA User utilization', 'HSUPA Thrp utilization', 'HSUPA User utilization',
         '3G Nokia IUB Loss', 'CSFB Fails', '3G Voice fails', '3G Voice drops', '2G Call drops',
         '2G Call blocks', '2G SDCCH blocks', 'HSDPA fails', 'HSUPA fails', '4G RAB fails', '4G RRC fails',
         'HSDPA drops', 'HSUPA drops', 'LTE session drops']
final_df, second_table_cols=read_files()

layout = {

          'template':'plotly_dark',
          'margin':dict(l=2, r=2, t=5, b=0),
          'font':dict(size=10),
            'yaxis': {'title': ''},
            'xaxis': {'title': '','type':'date'},
          'clickmode':'event+select',
          'legend':{'itemclick':'toggle'}
            }

def register_callback(dashapp):
    @dashapp.callback(
                  Output('table','data'),
                  Output('table_cell','data'),
                  Output('graph','figure'),
                  Output('table1_title','children'),
                  Output('graph_title','children'),
                  [Input('table','selected_cells')],
                  [Input('table_cell','selected_cells')])
    def plot_map_graph(cell,second_cell):
        ''' Draw traces of the feature 'Value' based one the currently selected stocks '''
        # STEP 1
        # Load data and sort/filter
        df_sub=final_df
        a=''
        if cell != None:
            print(cell,'selected cell with index ',cell[0]['row'])
            print()
            a = KPI_name[cell[0]['row']]
            today = datetime.date.today() - datetime.timedelta(15)
            files = pd.date_range(start=today,periods=15, freq='24H').strftime('%Y-%m-%d').tolist()
            to_concat = []
            for i in files:
                if os.path.isfile(os.path.join('/disk2/support_files/archive/util', i + '_util.h5')):
                    to_concat.append(pd.read_hdf(os.path.join(r'/disk2/support_files/archive/util', i + '_util.h5'),
                                                 table[cell[0]['row']]))
            selected_df=pd.concat(to_concat)
            #selected_df=pd.read_hdf('/disk2/support_files/archive/util.h5', table[cell[0]['row']],where='Date>=today')
            selected_df.rename(columns={'Value': a}, inplace=True)
            selected_df.dropna(inplace=True)
            #print(selected_df.head(),'selected  df')
            #print(cell[0]['column_id'], 'columns')
            selected_df['Date']=pd.to_datetime(selected_df['Date'])
            selected_df.sort_values(by='Date',inplace=True)
            #print(selected_df.tail())
           # print(cell[0]['column_id'])
            #print(dt.strptime(cell[0]['column_id'],'%d.%m.%Y'))
            #print('target = ',targets[cell[0]['row']])
            if cell[0]['row']<=2:
                filt=(selected_df['Date'] == dt.strptime(cell[0]['column_id'],'%d.%m.%Y')) & (selected_df[a]<targets[cell[0]['row']])
                to_sort=True
            else:
                filt = (selected_df['Date'] == dt.strptime(cell[0]['column_id'], '%d.%m.%Y')) & (
                            selected_df[a] > targets[cell[0]['row']])
                to_sort=False
            cells=selected_df[filt]['Cell'].unique()
            #print('filtered df')
            #print(selected_df[filt].sample(10))
            df_celt=pd.pivot_table(selected_df[selected_df['Cell'].isin(cells)],values=a,index='Cell',columns='Date')
            #print(df_celt.sample(10),'filtered table')
            df_celt=round(df_celt.iloc[:,-15:],2)
            df_celt.columns = [dt.strftime(df_celt.columns[i], '%d.%m.%Y') for i in
                               range(len(df_celt.columns))]
            df_celt.reset_index(inplace=True)
            df_celt.sort_values(by=df_celt.columns[-1],ascending=to_sort,inplace=True)
            df_celt.rename(columns={'Cell':'NE'},inplace=True)
            table2 = df_celt.to_dict('records')
        else:
            table2 = []

        if second_cell !=None:
            print(second_cell)
            filt=(selected_df['Cell']==df_celt['NE'].iloc[second_cell[0]['row']])
            df_cel2=selected_df[filt]
            df_cel2.rename(columns={a:'Value'},inplace=True)
            df_cel2.sort_values(by='Date',inplace=True)
            df_cel2 = df_cel2[-15:]
            df_cel2['Threshold']=targets[cell[0]['row']]
            print(df_cel2.head())
            figure = make_subplots(specs=[[{"secondary_y": False}]])
            figure.add_trace(go.Bar(x=df_cel2['Date'], y=df_cel2['Value'], # Main KPI
                                         opacity=0.7,
                                         name=a), secondary_y=False)
            nese_qraf= 'Graph of '+ df_celt['NE'].iloc[second_cell[0]['row']],' for ' + a
        else:
            figure=go.Figure(data=[])
            nese_qraf=''
        table1 = df_sub.to_dict('records')
        if cell!=None:
            nese=a+' - table of worst NEs which are out of target on '+ cell[0]['column_id']
        else: nese=''
        figure.layout=layout
        return table1,table2,figure,nese,nese_qraf
