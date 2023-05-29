import pandas as pd
from dash.dependencies import Input, Output, State
import plotly.graph_objs as go
from dash import ctx, dash_table,dcc,html
import datetime
import numpy as np
import plotly.express as px
import dash_bootstrap_components as dbc

lr = {'margin':dict(l=5, r=5, t=30, b=15),'height':350,'template': 'simple_white'}
lb = {'margin':dict(l=5, r=10, t=30, b=15),'height':350,'template': 'simple_white'}
lt = {'margin':dict(l=5, r=5, t=30, b=15),'height':30,'template': 'simple_white'}
dd=pd.DataFrame()

def register_callback(dashapp):

    @dashapp.callback(Output("download", "data"), [Input("btn", "n_clicks")], prevent_initial_call=True)
    def generate_xlsx(n_nlicks):
        global dd

        def to_xlsx(bytes_io):
            xslx_writer = pd.ExcelWriter(bytes_io, engine="xlsxwriter")  # requires the xlsxwriter package
            dd[['SITE_ID','Site Name','Economical Region','District/Community','2G down','3G down','LTE down',\
                'MPF start','SG start','PG start','Unique down','time']].to_excel(xslx_writer, index=False, sheet_name="sheet1")
            xslx_writer.save()

        return dcc.send_bytes(to_xlsx, "site_level_alarm_info.xlsx")

    @dashapp.callback(
        Output('dropdown', 'options'),
        Output('dropdown','value'),
        [Input('interval-component2', 'n_intervals')],
        #[Input('radio-items2', 'value')],
        [Input('interval-component2', 'n_intervals')],
        )
    def add_row(int1,int2):

        df=pd.read_csv(r'/home/ismayil/flask_dash/data/active_alarms/active_alarms.csv')
            
        return df['time'].unique(),df['time'].unique()[-1]

    @dashapp.callback(
        Output('map','children'),
        Output('table', 'data'),
        Output('availability', 'figure'),
        Output('down_graph','figure'),
        Output('mpf_graph','figure'),
        Output('Down_card', 'children'),
        Output('Mpf_card', 'children'),
        Output('Generator_card', 'children'),
        Output('PG_card', 'children'),
        Output('table2','data'),
        Output('table3','data'),
        Input('dropdown', 'value'),
        Input('dropdown_region','value'))
        
    def display_output(val,reg):

        df=pd.read_csv('/home/ismayil/flask_dash/data/active_alarms/active_alarms.csv')#.drop_duplicates()
        df2=df[df['time']==val]
        #print(df2.loc[df2['location']=='Total','Generator'].values)
        #print(df2)
        df=df[(df['time']==val) & (df['location']!='Total')]
        if reg!='All Regions':
            df=df[df['location']==reg]   
        down_sorted=df[['location','Unique down','MPF start','PG start','SG start']].groupby('location',as_index=False).sum().sort_values(by='location')
        down_sorted=down_sorted.append(pd.DataFrame(data=[['Absheron',61,30],['Aran',108,54],['Baku',125,63],['Ganja',73,40],['Lankaran',40,20],\
                            ['Naxchivan',28,14],['Qarabag',71,28],['Quba',46,23],['Sheki',45,30]],columns=['location','mpf_thr','down_thr']),ignore_index=True)
        down_sorted=down_sorted[['location','Unique down','MPF start','SG start','PG start','mpf_thr','down_thr']].groupby('location',as_index=False).sum().sort_values(by='location')
        down_sorted2=df[['location','admin_region','Unique down','MPF start','SG start','PG start']].sort_values(by='location')
        down_sorted['status'] = '🟢'
        down_sorted.loc[(down_sorted['Unique down']>=down_sorted['down_thr']) | (down_sorted['MPF start']>=down_sorted['mpf_thr']),'status']='🔥'
        
        #                           down_sorted['Unique down'].apply(lambda x: '🔥' if x > 30 else 
                                    #                                            ('🚒' if x > 20 else (
                                    #                                                '⚠️' if x > 10 else '🟢')))
        down_sorted.drop(columns=['mpf_thr','down_thr'],inplace=True)
        fig1 =down_sorted.to_dict('records')
        fig9 =down_sorted2.to_dict('records')
        
        #fig2 = go.Figure(data=[go.Bar(y=down_sorted['MPF start'], 
        #    x=down_sorted['location'], text=down_sorted['MPF start'],
        #    textfont=dict(size=12),
        #    name='Active MPF alarms',
        #    marker=dict(
        #        color='rgba(50, 171, 96,0.6)',
        #        line=dict(color='rgba(50, 171, 96, 1.0)', width=3))
        #    )])
        #fig2.update_layout(lr)
        #fig2.update_layout({'title':'Active MPF alarms'})

        fig3 = go.Figure(data=[go.Bar(y=down_sorted['Unique down'], 
            x=down_sorted['location'], text=down_sorted['Unique down'],
            textfont=dict(size=12),
            name='Active Down alarms',
            marker=dict(
                color='rgba(50, 171, 96,0.6)',
                line=dict(color='rgba(50, 171, 96, 1.0)', width=3))
            )])
        fig3.update_layout(lb)
        fig3.update_layout({'title':'Active Down alarms'})
        fig4 = go.Figure(data=[go.Bar(y=down_sorted['MPF start'], 
            x=down_sorted['location'], text=down_sorted['MPF start'],
            textfont=dict(size=12),
            name='Active MPF alarms',
            marker=dict(
                color='rgba(50, 171, 96,0.6)',
                line=dict(color='rgba(50, 171, 96, 1.0)', width=3))
            )])
        fig4.update_layout(lb)
        fig4.update_layout({'title':'Active MPF alarms'})


        down_sorted=df[['location','Unique down','MPF start','SG start','PG start']].groupby('location',as_index=False).sum().sort_values(by='location',ascending=False)
        
        df_site=pd.read_csv('/home/ismayil/flask_dash/data/active_alarms/site_level.csv')
        tracker=pd.read_csv('/home/ismayil/flask_dash/support_files/tracker.csv')
        df_site=df_site[df_site['time']==val]
        df_site.rename(columns={'sitecode':'site'},inplace=True)
        global dd

        df_site=df_site.merge(tracker[['SITE_ID','Lat','Long','Site Name']],on='SITE_ID',how='left')

        if reg!='All Regions':
            df_site=df_site[df_site['Economical Region']==reg]
            print('site level filtered')
        dd=df_site.drop(columns='location').copy()
        # Define center values of the map
        a=datetime.datetime.now()
        
        # Step 2
        # Plot the map and update layout
        df_site=df_site[df_site['2G down'].notnull() | df_site['3G down'].notnull() | df_site['LTE down'].notnull()]
        df_site[['Lat','Long']]=df_site[['Lat','Long']].astype(float)
        
        if len(df_site)>0:
            df_site['Down_since']=0
            df_site.loc[df_site['2G down'].notnull(),'Down_since']=df_site.loc[df_site['2G down'].notnull(),'2G down']
            df_site.loc[df_site['3G down'].notnull(),'Down_since']=df_site.loc[df_site['3G down'].notnull(),'3G down']
            df_site.loc[df_site['LTE down'].notnull(),'Down_since']=df_site.loc[df_site['LTE down'].notnull(),'LTE down']
            df_site['Down_since']=pd.to_datetime(df_site['Down_since'])
            df_site['ddf2']=df_site['Down_since'].apply(lambda x: (a-x).total_seconds()/60)
            df_site['size']=df_site['ddf2']*100/df_site['ddf2'].max()
            df_site.loc[df_site['size']<20,'size']=20
            def convert(s):
                days=s//86400
                hour=(s%86400//3600)
                minute=(s%3600//60)
                return str(round(days))+'d, '+str(round(hour))+'h and '+str(round(minute))+'min'
            df_site2=df_site.copy()
            df_site2.sort_values(by='ddf2',ascending=False,inplace=True)
            df_site2['Down_duration']=df_site2['ddf2'].apply(lambda x: convert(x*60))
            link=pd.read_csv('/mnt/raw_counters/Shared Folder/python script/Link_info.csv')
            df_site2=df_site2.merge(link,left_on='SITE_ID',right_on='Site ID',how='left')
            df_site2.loc[df_site2['2G down'].notnull(),'2G down']='2G'
            df_site2.loc[df_site2['2G down'].isnull(),'2G down']=''
            df_site2.loc[df_site2['3G down'].notnull(),'3G down']='3G'
            df_site2.loc[df_site2['3G down'].isnull(),'3G down']=''
            df_site2.loc[df_site2['LTE down'].notnull(),'LTE down']='4G'
            df_site2.loc[df_site2['LTE down'].isnull(),'LTE down']=''
            df_site2.loc[df_site2['MPF start'].notnull(),'MPF start']='MPF'
            df_site2.loc[df_site2['MPF start'].isnull(),'MPF start']=''
            df_site2['Down_Technology']=df_site2['2G down']+"/"+df_site2['3G down']+"/"+df_site2['LTE down']+"/"+df_site2['MPF start']
            def clear(x):
                x=x.replace('//','/')
                if x.startswith('/'): x=x[1:]
                elif x.endswith('/'): x=x[:-1]
                return x
            #df_site2['Down_Technology'].replace({'//':'/'},inplace=True)
            df_site2['Down_Technology']=df_site2['Down_Technology'].apply(clear)
            df_site2['Down_Technology']=df_site2['Down_Technology'].apply(clear)
            
            fig10 =df_site2[['SITE_ID','Site Name','SG info','Dependent_sites','Far End Site ID','HUB End Site ID','Economical Region','Down_Technology','Down_duration']].\
                rename(columns={'Economical Region':'Region','Far End Site ID':'Far End','HUB End Site ID':'HUB End'}).to_dict('records')
            if reg=='All Regions':
                zoom=6.5
            else:
                zoom=8.5
            figure = go.Figure(px.scatter_mapbox(df_site, lat='Lat', lon='Long', hover_name='site', size='size',#size=size_value,
                                    zoom=zoom, text=df_site['site'],hover_data={'site':False,'Lat':False,'Long':False,'size':False}
                                            ))
            layout2={
                'height': 500,
                'template': 'seaborn',
                'mapbox_style': "open-street-map",
                'margin': dict(l=2, r=10, t=5, b=5),
                'yaxis': {'title': ''},
                'xaxis': {'title': ''},
                'clickmode': 'event+select',
                'legend': {'itemclick': 'toggleothers'}}
            figure.update_layout(layout2)
            
            figure.update_layout(title=
                                    go.layout.Title(text='<b>' + val + ' values for '+reg+# + str(
                                        #dt.strftime(dt.utcfromtimestamp(dates[period].tolist() / 1e9),
                                        '<b>',#            '%d.%m.%Y')) + '</b>',
                                                    font=dict(
                                                        family="Courier New, monospace",
                                                        size=22,
                                                        color="#0A2A52"
                                                    )),
                                    title_x=0.5,
                                    title_y=0.97
                                    )
            figure.update_traces(marker=dict(color='red', opacity=0.5))#,cluster=dict(enabled=True))
            figure=dcc.Graph(figure=figure,config={'displayModeBar': False})
        else:
            figure = html.H1("No down in "+reg, className="card-title",style={'height': 500})
            fig10=[]
                
        fig5=down_sorted['Unique down'].sum()
        fig6=down_sorted['MPF start'].sum()
        fig7=down_sorted['SG start'].sum()
        fig8=down_sorted['PG start'].sum()

        yesterday = datetime.date.today() - datetime.timedelta(3)
        df_2G_raw = pd.read_hdf(r'/disk2/support_files/archive/combined_bsc.h5',
                                '/twoG',where='Date>=yesterday')
        df_3G_raw = pd.read_hdf(r'/disk2/support_files/archive/combined_bsc.h5',
                                '/threeG',where='Date>=yesterday')
        df_4G_raw = pd.read_hdf(r'/disk2/support_files/archive/combined_bsc.h5',
                                '/fourG',where='Date>=yesterday')

        df_2G=df_2G_raw.groupby('Date').sum()
        df_2G.reset_index(inplace=True)
        df_2G_reg=df_2G_raw.groupby(['Region', 'Date']).sum()
        df_2G_reg.reset_index(inplace=True)
        df_2G['Cell_Availability'] = (df_2G['cell_avail_num'] + df_2G['cell_avail_blck_num']) / (
            df_2G['cell_avail_den'] - df_2G['cell_avail_blck_den']) * 100
        df_2G['Technology'] = '2G'
        df_2G_reg['Cell_Availability'] = (df_2G_reg['cell_avail_num'] + df_2G_reg['cell_avail_blck_num']) / (
            df_2G_reg['cell_avail_den'] - df_2G_reg['cell_avail_blck_den']) * 100
        df_2G_reg['Technology'] = '2G'

        df_3G=df_3G_raw.groupby('Date').sum()
        df_3G.reset_index(inplace=True)
        df_3G_reg=df_3G_raw.groupby(['Region', 'Date']).sum()
        df_3G_reg.reset_index(inplace=True)
        df_3G['Cell_Availability'] = 100 * (df_3G['cell_avail_num'] + df_3G['cell_avail_blck_num']) / (
                df_3G['cell_avail_den'] - df_3G['cell_avail_blck_den'])
        df_3G['Technology'] = '3G'
        df_3G_reg['Cell_Availability'] = 100 * (df_3G_reg['cell_avail_num'] + df_3G_reg['cell_avail_blck_num']) / (
                df_3G_reg['cell_avail_den'] - df_3G_reg['cell_avail_blck_den'])
        df_3G_reg['Technology'] = '3G'

        df_4G=df_4G_raw.groupby('Date').sum()
        df_4G.reset_index(inplace=True)
        df_4G_reg=df_4G_raw.groupby(['Region', 'Date']).sum()
        df_4G_reg.reset_index(inplace=True)
        df_4G['Cell_Availability'] = 100 * (df_4G['cell_avail_num'] + df_4G['cell_avail_blck_num']) / (
                df_4G['cell_avail_den'] - df_4G['cell_avail_blck_den'])
        df_4G['Technology'] = '4G'
        df_4G_reg['Cell_Availability'] = 100 * (df_4G_reg['cell_avail_num'] + df_4G_reg['cell_avail_blck_num']) / (
                df_4G_reg['cell_avail_den'] - df_4G_reg['cell_avail_blck_den'])
        df_4G_reg['Technology'] = '4G'

        df_avail = pd.concat([df_2G, df_3G, df_4G])
        df_avail.reset_index(inplace=True)
        df_avail['Date'] = pd.to_datetime(df_avail['Date'], format="%d.%m.%Y")
        df_avail.sort_values(by='Date', inplace=True)

        df_avail_reg = pd.concat([df_2G_reg, df_3G_reg, df_4G_reg])
        df_avail_reg.reset_index(inplace=True)
        df_avail_reg['Date'] = pd.to_datetime(df_avail_reg['Date'], format="%d.%m.%Y")
        df_avail_reg.sort_values(by='Date', inplace=True)
          

        trace=[]
        numb=['2G','3G','4G']
        if reg=='All Regions':
            for tech in numb:
                trace.append(go.Scatter(x=df_avail[df_avail['Technology'] == tech]['Date'],
                                        y=round(df_avail[df_avail['Technology'] == tech]['Cell_Availability'],2),
                                        mode='lines',
                                        opacity=0.7,
                                        name=tech,
                                        textposition='bottom center'))
        else:
            df_avail_reg = df_avail_reg[df_avail_reg['Region'] == reg]
            for r in np.sort(df_avail_reg['Technology'].unique()):
                trace.append(go.Scatter(x=df_avail_reg[df_avail_reg['Technology'] == r]['Date'],
                                        y=round(df_avail_reg[df_avail_reg['Technology'] == r]['Cell_Availability'],2),
                                        mode='lines',
                                        opacity=0.7,
                                        name=r,
                                        textposition='bottom center'))
        traces = [trace]
        data = [val for sublist in traces for val in sublist]
        fig2 = go.Figure(data=data, layout=lr)
        fig2.update_yaxes(automargin=True)
        fig2.update_layout({'title':reg+' - Cell Availability, %'})



        return figure, fig1, fig2, fig3, fig4, fig5, fig6, fig7,fig8, fig9,fig10
