from dash import Dash, dcc, html

a=[['Cell Availability','Availability'],['Call Setup SR','Call_setup_sr'],['Data Setup SR','Data SR'],
   ['CS Traffic, KErl','CS Traffic'],['Call Drop Rate','Call_dr'],['Data Drop Rate','Data DR'],
   ['PS Traffic, TB','PS traffic'],['CSFB Success Rate','CSFB SR'],['Average User DL Throughput, Kbps','DL Thrp']]
b=[]
for text in a:
    b.append(html.Div(children=[
     html.H1(children=[text[0],html.Div(id=text[1]+'_nese',style={'display': 'inline'})],
             style={
                 'textAlign': 'center',
                 'color': 'black',
                 'font-weight': 'bold',
             'display':'inline',
                 'font-size': '1vw',
             'margin-left':'30%'},
             )]))
    b.append(dcc.Graph(id=text[1],style={'height': '27vh'},config={'displayModeBar': False}))


# Define the app
layout = html.Div(children=[dcc.Store(id='drilldown', data=[]),
                    html.Div(children=[
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

html.A(html.H1(children=['Worst Sites'],
             style={
                 'textAlign': 'center',
                 'color': '#435F82',
                 'font-weight': 'bold',
                 'display':'inline',
                 'line-height':1.5,
                 'font-family':"Apple Color Emoji",
                'letter-spacing':1,
                       'font-size':'1.7em',
             'margin-left':'1%'}), href='/worst_sites'
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
             ),
                        dcc.RadioItems(id='radio-items',
                        options=[
                            {'label': 'Daily', 'value': 'D'},
                            {'label': 'Hourly', 'value': 'H'},
                        ],
                        value='H',labelStyle={'display': 'inline-block','margin-left':5,'margin-right':5},
                                                      style={'display': 'inline-block',
                                                             'margin-left':100,'margin-top':10,'background-color':'#CAC7C7',
                                                             "font-family": "Comic Sans MS"}
                            ),
                                    dcc.RadioItems(id='radio-items2',
                                  options=[
                                      {'label': '1 Month', 'value': '1M'},
                                      {'label': '2 Week', 'value': '2W'},
                                      {'label': '1 Week', 'value': '1W'},
                                      {'label': '3 Day', 'value': '3D'}
                                  ],
                                  value='3D', labelStyle={'display': 'inline-block','margin-left':5,'margin-right':5},
                                   style={'display': 'inline-block','margin-left':50,'margin-top':10,'margin-bottom':5,'background-color':'#CAC7C7',
                                          "font-family": "Comic Sans MS"}
                                  )
                        
                        ,html.A(html.H1(children=['Logout'],
             style={
                 'textAlign': 'center','margin-right': 20,'margin-top':10,
                 'color': '#435F82',
                 'font-weight': 'bold',
                 'display':'inline',
                 'float':'right',
                 'line-height':1.5,
                 'font-family':"Apple Color Emoji",
                'letter-spacing':1,
                       'font-size':'1.4em',
             'margin-left':'1%'}), href='/logout'
             ),
                        html.A(html.H1(children=['Account'],
             style={
                 'textAlign': 'center','margin-top':10,
                 'color': '#435F82',
                 'font-weight': 'bold',
                 'float':'right',
                 'display':'inline',
                 'line-height':1.5,
                 'font-family':"Apple Color Emoji",
                'letter-spacing':1,
                       'font-size':'1.4em',
             'margin-left':'1%'}), href='/change_password'
             ),

                    html.A(html.Button('Reset Graphs',style={'display': 'inline-block', 'margin-right': 50,
                                                                 'color': 'black',
                                                                 'float':'right',
                                                                 "font-size": "0.8em",
                                                                 "font-family": "Comic Sans MS"
                                                                 }),href='/dashboard')],
                             style={'margin-left':30,'margin-top':10,'color':'black',"font-size": "0.8em","font-family": "Comic Sans MS"}),
                    html.Div(className='row',  # Define the row element
                        children=[

                            html.Div(className='four columns div-for-charts',
                                     children=b[:6]
                                    ,style={'margin-left':20}),

                            html.Div(className='four columns div-for-charts',
                                     children=b[6:12],style={'margin-left':20}),


                            html.Div(className='four columns div-for-charts',
                                     children=b[12:],style={'margin-left':20})

                        ]),
                    dcc.Interval(
                        id='interval-component',
                        interval=10 * 60000,  # in milliseconds
                        n_intervals=0
                    ),dcc.Interval(
                        id='interval-component2',
                        interval=10 * 60000+10000,  # in milliseconds
                        n_intervals=0
                    ),html.Div(id='whole')
                    ],style={'background-color':'rgb(226, 222, 222)'})

