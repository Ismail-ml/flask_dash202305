import pandas as pd
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import plotly.express as px
import datetime
import numpy as np

l = {
            'margin':dict(l=2, r=2, t=5, b=15),
            'font':dict(color='white')
            }
def register_callback(dashapp):


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
        Output('Down_pie', 'figure'),
        Output('Down_tree', 'figure'),
        #Output('Mpf_pie', 'figure'),
        Output('Mpf_tree', 'figure'),
        Output('Down_card', 'children'),
        Output('Mpf_card', 'children'),
        Output('Generator_card', 'children'),
        Output('PG_card','children'),
        Output('Availability','figure'),
        Input('dropdown', 'value'),
        Input('Availability', 'clickData'))
    def display_output(val,drilldown):
        df=pd.read_csv(r'/home/ismayil/flask_dash/data/active_alarms/active_alarms.csv')
        df2=df[df['time']==val]
        #print(df2.loc[df2['location']=='Total','Generator'].values)
        #print(df2)
        df=df[(df['time']==val) & (df['location']!='Total')]
        #fig1 = px.pie(df[['location','Down']].groupby('location',as_index=False).sum(), values='Down', names='location',
        #         title='Active Down Alarms',hole=.5)
                    #hover_data=['lifeExp'], labels={'lifeExp':'life expectancy'})
        down_sorted=df[['location','Unique down','MPF start','PG start','SG start']].groupby('location',as_index=False).sum().sort_values(by='location',ascending=False)
        fig1=go.Figure(data=[go.Bar(y=down_sorted['location'], 
        x=down_sorted['Unique down'], text=down_sorted['Unique down'],
        textfont=dict(color="white",size=14),
        orientation='h',name='Active Down alarms',
        marker=dict(
            color='rgba(58, 71, 80, 0.6)',
            line=dict(color='rgba(58, 71, 80, 1.0)', width=3))
        )],layout=l)

        for i in ['MPF','SG','PG']:
            if i=='MPF':
                gt='Active MPF'
                color='rgba(246, 78, 139,'
            elif i=='PG':
                gt=i+" Running"
                color='rgba(50, 171, 96,'
            else:
                gt=i+" Running"
                color='rgba(52, 45, 113,'
            fig1.add_trace(go.Bar(
            y=down_sorted['location'],
            x=down_sorted[i+' start'],
            name=gt,
            text=down_sorted[i+' start'],
            textfont=dict(color="white",size=14),
            orientation='h',
            marker=dict(
                color=color+' 0.6)',
                line=dict(color=color+' 1.0)', width=3)
            )
            ))
        #fig1.add_trace(go.Bar(
        #y=down_sorted['location'],
        #x=down_sorted['SG start'],
        #name='SG Running',
        #text=down_sorted['SG start'],
        #textfont=dict(color="white",size=14),
        #orientation='h',
        #marker=dict(
        #    color='rgba(50, 171, 96, 0.6)',
        #    line=dict(color='rgba(50, 171, 96, 1.0)',
        #        width=3)
        #)
        #))
        #fig1.add_trace(go.Bar(
        #y=down_sorted['location'],
        #x=down_sorted['PG start'],
        #name='PG Running',
        #text=down_sorted['PG start'],
        #textfont=dict(color="white",size=14),
        #orientation='h',
        #marker=dict(
        #    color='rgba(50, 171, 96, 0.6)',
        #    line=dict(color='rgba(50, 171, 96, 1.0)',
        #        width=3)
        #)
        #))
        #fig1.update_traces(textposition='outside')
        fig1.update_layout(barmode='stack',template='plotly_dark',xaxis ={'showgrid': False},
                                    yaxis = {'showgrid': False})
    # fig1.update_traces(textposition='inside', textinfo='value+label+percent',showlegend=False)

        fig2 = px.treemap(df, path=[px.Constant('Active Down alarms'),'location', 'admin_region'], values='Unique down')
        fig2.update_layout(margin = dict(t=0, l=2, r=2, b=0))
        fig2.update_traces(root_color="lightgrey",insidetextfont={'size':16},outsidetextfont={'size':24})
        fig2.data[0].textinfo = 'label+text+value'

    

        fig4 = px.treemap(df, path=[px.Constant('MPF Active alarms'),'location', 'admin_region'], values='MPF start')
        fig4.update_layout(margin = dict(t=4, l=2, r=2, b=0))
        fig4.update_traces(root_color="lightgrey",insidetextfont={'size':16},outsidetextfont={'size':24})
        fig4.data[0].textinfo = 'label+text+value'

        

        

        #df2=df2.append(pd.DataFrame([['Total','',down_sorted['Unique down'].sum(),a['MPF'].sum(),a['Generator'].sum()]],columns=['location','admin_region','Down','MPF','Generator']))

        fig5 = down_sorted['Unique down'].sum() #df2.loc[df2['location']=='Total','Unique down'].values[0]
        fig6 = down_sorted['MPF start'].sum() #df2.loc[df2['location']=='Total','MPF start'].values[0]
        fig7 = down_sorted['SG start'].sum()
        fig9 = down_sorted['PG start'].sum()  #df2.loc[df2['location']=='Total','SG start'].values[0]

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
        layout = {
          'template':'plotly_dark',
          'margin':dict(l=2, r=2, t=3, b=0),
            #'font':dict(size=8),
            'yaxis': {'title': '','showgrid': False},
            'xaxis': {'title': '','showgrid': False,
                      'type':'date'
                      },
          'clickmode':'event+select',
          'legend':{'itemclick':'toggle'}
            }
        numb=['2G','3G','4G']
        if drilldown==None:
            for tech in numb:
                trace.append(go.Scatter(x=df_avail[df_avail['Technology'] == tech]['Date'],
                                        y=round(df_avail[df_avail['Technology'] == tech]['Cell_Availability'],2),
                                        mode='lines',
                                        opacity=0.7,
                                        name=tech,
                                        textposition='bottom center'))
        elif drilldown!=None:
            kk = drilldown['points'][0]['curveNumber']
            df_avail_reg = df_avail_reg[df_avail_reg['Technology'] == numb[kk]]
            for r in np.sort(df_avail_reg['Region'].unique()):
                trace.append(go.Scatter(x=df_avail_reg[df_avail_reg['Region'] == r]['Date'],
                                        y=round(df_avail_reg[df_avail_reg['Region'] == r]['Cell_Availability'],2),
                                        mode='lines',
                                        opacity=0.7,
                                        name=r,
                                        textposition='bottom center'))
        traces = [trace]
        data = [val for sublist in traces for val in sublist]
        fig8 = go.Figure(data=data, layout=layout)
        fig8.update_yaxes(automargin=True)


        return fig1, fig2, fig4, fig5, fig6, fig7, fig9, fig8
