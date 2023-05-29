from dash import Dash, dcc, html,dash_table
from datetime import datetime as dt
import datetime


layout = html.Div(children=[
html.Div(className="row",children=[
html.A(html.H1(children=['RAN Dashboard'],
             style={
                 'textAlign': 'center',
                 'color': '#435F82',
                 'font-weight': 'bold',
                 'display':'inline',
                 'line-height':1.5,
                 'font-family':"Apple Color Emoji",
                'letter-spacing':1,
                       'font-size':'1.7em',
             'margin-left':'1%'}), href='/dashboard'
             ),

html.A(html.H1(children=['Core Dashboard'],
             style={
                 'textAlign': 'center',
                 'color': '#435F82',
                 'font-weight': 'bold',
                 'display':'inline',
                 'line-height':1.5,
                 'font-family':"Apple Color Emoji",
                'letter-spacing':1,
                'font-size':'1.7em',
             'margin-left':'1%'}), href='/core_dashboard'
             ),
html.A(html.H1(children=['Anomalities'],
             style={
                 'textAlign': 'center',
                 'color': '#435F82',
                 'font-weight': 'bold',
                 'display':'inline',
                 'line-height':1.5,
                 'font-family':"Apple Color Emoji",
                'letter-spacing':1,
                'font-size':'1.7em',
             'margin-left':'1%'}), href='/anomality'
             )
        ],style={'margin-top':10,'color':'black',"font-size": "0.8em"}),
    html.Div(className='row',children=[
        dcc.RadioItems(id='radio_items',
                       options=[
                           {'label': '2G Voice Fails', 'value': '2G Call fails'},
                           {'label': '2G Voice Drops', 'value': '2G Call drops'},
                           {'label': '2G Availability', 'value': '2G Cell Availability'},
                           {'label': '3G Voice Fails', 'value': '3G Call fails'},
                           {'label': '3G Voice Drops', 'value': '3G Call drops'},
                           {'label': '3G HSDPA Fails', 'value': '3G HSDPA fails'},
                           {'label': '3G HSDPA Drops', 'value': '3G HSDPA drops'},
                           {'label': '3G Availability', 'value': '3G Cell Availability'},
                           {'label': '4G RRC Fails', 'value': '4G RRC fails'},
                           {'label': '4G RAB Fails', 'value': '4G RAB fails'},
                           {'label': '4G CSFB Fails', 'value': '4G CSFB fails'},
                           {'label': '4G Session Drops', 'value': '4G Session drops'},
                           {'label': '4G Availability', 'value': '4G Cell Availability'}
                       ],
                       value='2G Call fails',
                       style={'font-size': 12, 'color': 'black', "font-family": "Comic Sans MS"},
                       labelStyle={'display': 'inline-block', 'margin-left': 10}
                       )], style={'margin-top': 10, 'background-color': '#CAC7C7'}),
    html.Div(className='row',children=[html.Div(children=[
    html.Div(className='six columns',children=[dcc.Graph(id='map',config={'displayModeBar': False})],style={'margin-left':10,'margin-bottom':10}),
    html.Div(className='five columns',children=[dash_table.DataTable(id='table',columns=[
        {"name": i, "id": i} for i in ['Date','Site Id','Region','KPI name','KPI value','Cell Availability']],
        style_cell={'textAlign': 'center','color': 'black','fontSize':12},
        style_header={'backgroundColor': 'rgb(167, 171, 170)',
                'fontWeight': 'bold','color':'black','fontSize':12},
        style_data_conditional=[
        {
            'if': {'row_index': 'odd'},
            'backgroundColor': 'rgb(230, 234, 233)'
        }],
        virtualization=True,
                                                                     style_table={'height': '39vh',
                                                                                    'overflowY': 'auto'
                                                                                  }, page_size=20
                                                                     ),
        html.H1(children=['Trend of selected KPI'],style={'font-size': '1.3vw',
                         'textAlign': 'center',
                         'color': 'black',
                         'font-weight': 'bold',
                     'display':'inline',
                     'margin-left':200},
                     ),
        dcc.Graph(id='graph',figure={'layout':{'height':320}},config={'displayModeBar': False})
                                                ],style={'margin-right':10,'plot_bgcolor':'transparent','paper_bgcolor':'transparent'})
    ])]),
    html.Div(className='row five columns',children=[
               dcc.Slider(id='date_picker',
               min=0,
               max=4,
               marks={i:{'label':dt.strftime(datetime.datetime.strptime(datetime.datetime.strftime(datetime.datetime.now()-datetime.timedelta(hours=2),'%Y-%d-%m %H'),'%Y-%d-%m %H')-datetime.timedelta(hours=4-i),'%H')} for i in range(5)},
               included=False,
               step=1,
               value=4
               )],style={'background-color':'rgb(54, 54, 54)','font-weight':'bold',"font-size": "0.8em"}),dcc.Interval(
                        id='interval-component',
                        interval=10 * 60000,  # in milliseconds
                        n_intervals=0
                    ),dcc.Interval(
                        id='interval-component2',
                        interval=10 * 60000+10000,  # in milliseconds
                        n_intervals=0
                    )
        ], style={'background-color':'rgb(226, 222, 222)','margin-left':10,'margin-top':10,})