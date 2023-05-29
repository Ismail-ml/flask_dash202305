from dash import Dash, dcc, html

cn = "mr-1 py-0 px-1"
st = {"font-size": "0.6em","color":"black"}
cl = 'secondary'

a=[['Data Traffic',''],['Call Setup Time',''],['Paging SR',''],['Attach SR','Attach SR, %'],
   ['Location Update SR','Location Update SR, %'],['PDP_Ctx and Bearer Setup SR','PDP Ctx. and Bearer activation SR, %']]
b=[]
for text in a[:3]:
    b.append(html.Div(children=[
     html.H1(children=[text[1],html.Div(id=text[0]+'_nese',style={'display': 'inline'})],
             style={
                 'textAlign': 'center',
                 'color': 'black',
                 'font-weight': 'bold',
             'display':'inline',
             'font-size':'1vw',
             'margin-left':'35%'},
             )]))
    b.append(dcc.Graph(id=text[0],style={'height': '27vh'},config={'displayModeBar': False}))


# Define the app
layout = html.Div(children=[
                        dcc.Store(id='drilldown', data=[]),
                    html.Div(children=[
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

                       # dcc.RadioItems(id='radio-items',
                       # options=[
                       #     {'label': 'Daily', 'value': 'D'},
                       #     {'label': 'Hourly', 'value': 'H'},
                       # ],
                       # value='H',labelStyle={'display': 'inline-block','margin-left':5,'margin-right':5},
                       #                               style={'display': 'inline-block',
                       #                                      'margin-left':100,'margin-top':10,'background-color':'#CAC7C7',
                       #                                      "font-family": "Comic Sans MS"}
                       #     ),
html.A(html.H1(children=['Logout'],
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
                    #dcc.RadioItems(id='radio-items2',
                    #              options=[
                    #                  {'label': '3 Month', 'value': '1Q'},
                    #                  {'label': '1 Month', 'value': '1M'},
                    #                  {'label': '2 Week', 'value': '2W'},
                    #                  {'label': '1 Week', 'value': '1W'},
                    #                  {'label': '3 Day', 'value': '3D'}
                    #              ],
                    #              value='3D', labelStyle={'display': 'inline-block','margin-left':5,'margin-right':5},
                    #               style={'display': 'inline-block','margin-left':50,'margin-top':10,'margin-bottom':5,'background-color':'#CAC7C7',
                    #                      "font-family": "Comic Sans MS"}
                    #              ),
                                       

                    ],style={'margin-left':10,'margin-top':10,'color':'black',"font-size": "0.8em","font-family": "Comic Sans MS"}),

                    html.Div(className='row',  # Define the row element
                        children=dcc.Graph(id='Data Traffic',style={'height': '17vh'},config={'displayModeBar': False})
                                    ,style={'margin-top':20}
                            #html.Div(className='six columns div-for-charts', id='table_elave_et',
                             #        children=b[6:],style={'margin-left':20})
                        ),
                        html.Div(className='row',children=[
                        html.Div(className='six columns div-for-charts',children=[
                                html.Div(className='row',children=[
                            html.H1(children=['Bakcell Billing Anomaly count',html.Div(id='BKC_nese',style={'display': 'inline'})],
             style={
                 'textAlign': 'center',
                 'color': 'black',
                 'font-weight': 'bold',
             'display':'inline',
             'font-size':'1vw',
             'margin-left':'35%'},
             ), dcc.Graph(id='BKC_alarm_count',style={'height': '32vh'},config={'displayModeBar': False})]
                                    ,style={'margin-top':20}),

                    html.Div(className='row',children=[
                            html.H1(children=['Azerfon Billing Anomaly count',html.Div(id='AZF_nese',style={'display': 'inline'})],
             style={
                 'textAlign': 'center',
                 'color': 'black',
                 'font-weight': 'bold',
             'display':'inline',
             'font-size':'1vw',
             'margin-left':'35%'},
             ), dcc.Graph(id='AZF_alarm_count',style={'height': '32vh'},config={'displayModeBar': False})]
                                    ,style={'margin-top':20})]),
            html.Div(className='six columns div-for-charts', id='table_elave_et',
                                     children=b[6:],style={'margin-left':20})]),
                    dcc.Interval(
                        id='interval-component',
                        interval=5 * 60000,  # in milliseconds
                        n_intervals=0
                    ),dcc.Interval(
                        id='interval-component2',
                        interval=5 * 60000+10000,  # in milliseconds
                        n_intervals=0
                    ),html.Div(id='whole')
                    ],style={'background-color':'rgb(226, 222, 222)'})