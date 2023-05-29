import pandas as pd
from dash.dependencies import Input, Output, State
from plotly.subplots import make_subplots
import plotly.express as px
import plotly.graph_objs as go
from datetime import datetime as dt
import numpy as np
from dashapp1.layout import layout as ly
import datetime
import time


selected_KPI = ''
date_period = 6
fff='2G Voice Fails'

layout = {
        'height': 630,
        'template': 'seaborn',
        'mapbox_style': "open-street-map",
        'margin': dict(l=2, r=2, t=5, b=0),
        'yaxis': {'title': ''},
        'xaxis': {'title': ''},
        'clickmode': 'event+select',
        'legend': {'itemclick': 'toggleothers'}}

def load_data():
    df = pd.read_csv(r'/home/ismayil/flask_dash/support_files/worst_sites.csv')
    df['Date'] = pd.to_datetime(df['Date'], format="%d.%m.%Y")

    
    return df
df= load_data()
def register_callback(dashapp):

    @dashapp.callback(Output('date_picker', 'marks'),
                      [Input('interval-component', 'n_intervals')])
    def update_marks(ticker):
        ''' Draw traces of the feature 'value' based one the currently selected stocks '''
        # STEP 1
        # Load data and sort/filte
        global df
        df = load_data()
        marks = {i: {'label': dt.strftime(dt.now() - datetime.timedelta(7 - i), '%d.%m.%Y')} for i in range(7)}
        return marks

    @dashapp.callback(Output('map', 'figure'),
                      Output('table', 'data'),
                      Output('graph', 'figure'),
                      #Output('date_picker','marks'),
                      [Input('radio_items', 'value')],
                      [Input('table', 'selected_cells')],
                      [Input('date_picker', 'value')],
                      [Input('interval-component2', 'n_intervals')])
    def plot_map_graph(selected_dropdown_value, cell, period,n):
        ''' Draw traces of the feature 'value' based one the currently selected stocks '''
        # STEP 1
        # Load data and sort/filte  
        #marks={i:{'label':dt.strftime(dt.now()-datetime.timedelta(7-i),'%d.%m.%Y')} for i in range(7)}
        #time.sleep(1)
        print(selected_dropdown_value,'Selected KPI')
        global df, fff
        if fff!=selected_dropdown_value :
            df = load_data()
            fff=selected_dropdown_value
        trace = []
        dates = np.sort(df['Date'].unique())
        if selected_dropdown_value not in ['2G Cell Availability', '3G Cell Availability', '4G Cell Availability']:
            df.sort_values(by='KPI value', inplace=True, ascending=False)
        else:
            df = df[df['KPI value'] < 100]
            df.sort_values(by='KPI value', inplace=True, ascending=True)
        df_sub = df[(df['KPI name'] == selected_dropdown_value) & (df['Date'] == dates[period])][:15]

        # Define size based as reverse order
        if selected_dropdown_value not in ['2G Cell Availability', '3G Cell Availability', '4G Cell Availability']:
            size_value = 'KPI value'
        else:
            size_value = [(100 - i) for i in df_sub['KPI value']]
        # Define center values of the map
        center_value = {'lat': 'None', 'lon': 'None'}
        if cell:
            center_value['lat'] = df_sub.iloc[cell[0]['row']][['Lat', 'Long']][0]
            center_value['lon'] = df_sub.iloc[cell[0]['row']][['Lat', 'Long']][1]

        global selected_KPI, date_period
        if selected_dropdown_value != selected_KPI or period != date_period:
            center_value = {'lat': 'None', 'lon': 'None'}
            selected_KPI = selected_dropdown_value
            date_period = period

        # Step 2
        # Plot the map and update layout
        if center_value['lat'] != 'None':
            figure = px.scatter_mapbox(df_sub, lat='Lat', lon='Long', hover_name='Site Name', size=size_value,
                                       center=center_value,
                                       hover_data=['KPI value'], zoom=15,
                                       text=df_sub['Site Id'])
        else:
            figure = px.scatter_mapbox(df_sub, lat='Lat', lon='Long', hover_name='Site Name', size=size_value,
                                       hover_data=['KPI value'], zoom=7, text=df_sub['Site Id']
                                       )

        figure.update_layout(layout)
        figure.update_layout(title=
                             go.layout.Title(text='<b>' + selected_dropdown_value + ' values for ' + str(
                                 dt.strftime(dt.utcfromtimestamp(dates[period].tolist() / 1e9),
                                             '%d.%m.%Y')) + '</b>',
                                             font=dict(
                                                 family="Courier New, monospace",
                                                 size=24,
                                                 color="#0A2A52"
                                             )),
                             title_x=0.5,
                             title_y=0.97
                             )
        figure.update_traces(marker=dict(color='red', opacity=0.5),
                             )

        # Step 3
        # Fill table and draw scatter plot
        df_sub['Date'] = [dt.strftime(df_sub['Date'].iloc[i].to_pydatetime(), '%d.%m.%Y') for i in
                          range(len(df_sub['Date']))]

        if cell != None:
            filt = (df['KPI name'] == selected_dropdown_value) & (
                    df['Site Id'] == df_sub.iloc[cell[0]['row']]['Site Id'])
            df_for_plot = df[filt]
            df_for_plot.sort_values(by='Date', inplace=True)
            graphic = make_subplots(specs=[[{"secondary_y": True}]])
            graphic.add_trace(go.Bar(x=df_for_plot['Date'], y=df_for_plot['KPI value'],  # Main KPI
                                     opacity=0.7,
                                     name=selected_dropdown_value), secondary_y=False)
            graphic.add_trace(
                go.Scatter(x=df_for_plot['Date'], y=df_for_plot['Cell Availability'],  # Secondary KPI(Availability)
                           mode='lines+markers',
                           line=dict(color='red'),
                           name='Cell Availability', connectgaps=False), secondary_y=True)
            graphic.update_yaxes(title_text=selected_dropdown_value, secondary_y=False)
            graphic.update_yaxes(title_text='Cell Availability', secondary_y=True)
            graphic.update_layout(go.Layout(
                margin=go.layout.Margin(
                    l=0,  # left margin
                    r=0,  # right margin
                    b=30,  # bottom margin
                    t=30,  # top margin
                )))
            graphic.update_layout(
                font=dict(
                    size=12
                ),
                legend=dict(
                    yanchor="bottom",
                    orientation="h",
                    y=1.02,
                    xanchor="center",
                    x=0.50
                )
            )

        else:
            graphic = go.Figure(data=[])

        table1 = df_sub[['Date', 'Site Id','Region', 'KPI name', 'KPI value', 'Cell Availability']][
                 :15].to_dict('records')

        return figure, table1, graphic


