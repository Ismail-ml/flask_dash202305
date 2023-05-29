from dash import Dash, dcc, html
import dash_bootstrap_components as dbc

cn = "mr-1 py-0 px-1"
st = {"font-size": "0.6em","color":"black"}
cl = 'secondary'

#app = Dash(__name__,external_stylesheets=[dbc.themes.BOOTSTRAP])

# Define the app
layout = html.Div(children=[

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
           

                    ],style={'margin-left':10,'margin-top':10,'color':'black',"font-size": "0.8em","font-family": "Comic Sans MS"}),
# 1st row
                    dbc.Row([dbc.Col(dcc.Dropdown(id='dropdown',
                   options=['New York City', 'Montreal', 'San Francisco'],
   value='Montreal'
),width=2),dbc.Col(dbc.Card([dbc.CardHeader(html.H6("Total Unique downs")),dbc.CardBody([
    html.P(id='Down_card',className="card-text")]
    )],color="info"),width=2),
 dbc.Col(dbc.Card([dbc.CardHeader(html.H6("Total MPF Alarms")),dbc.CardBody([#html.H5("MPF Alarms", className="card-title"),
    html.P(id='Mpf_card',className="card-text")]
    )],color="info"),width=2,style={'height':'10%'}),
dbc.Col(dbc.Card([dbc.CardHeader(html.H6("Total Running SG")),dbc.CardBody([#html.H5("Running SG", className="card-title"),
    html.P(id='Generator_card',className="card-text")]
    )],color="info"),width=2,style={'height':'10%'}),
dbc.Col(dbc.Card([dbc.CardHeader(html.H6("Total Running PG")),dbc.CardBody([#html.H5("Running SG", className="card-title"),
    html.P(id='PG_card',className="card-text")]
    )],color="info"),width=2,style={'height':'10%'}),
    dbc.Col(
        html.A(html.Button('Reset Graphs',style={'display': 'inline-block', 'margin-right': 50,
                                                                 'color': 'black',
                                                                 'float':'right',
                                                                 "font-size": "0.8em",
                                                                 "font-family": "Comic Sans MS"
                                                                 }),href='/flm')
    )],#align='center',

#dbc.Col(dcc.Graph(id='Down_card')),dbc.Col(dcc.Graph(id='Mpf_card')),dbc.Col(dcc.Graph(id='Generator_card')) ],
style={'margin-left':10,'margin-top':10,'color':'black',"font-size": "0.8em","font-family": "Comic Sans MS",'height':"10%"},
className='h-10'), #"width": "30%"

# 2nd row
dbc.Row([
dbc.Row([dbc.Col([dcc.Graph(id='Down_pie',config={'displayModeBar': False})],style={'height':'40%'},width=4),
    dbc.Col([dcc.Graph(id='Down_tree',config={'displayModeBar': False})],style={'height':'40%'},width=7)
     ]
                                    ,style={'margin-top':20,'margin-left':10}),

dbc.Row([dbc.Col([
    html.H1('Cell Availability',style={
                 'textAlign': 'center',
                 'color': 'black',
                 'font-weight': 'bold',
             'display':'inline',
                 'font-size': '1vw',
             'margin-left':'30%'}),
    dcc.Graph(id='Availability',config={'displayModeBar': False})],style={'height':'40%'},width=4),#dcc.Graph(id='Mpf_pie',config={'displayModeBar': False})],style={'height':'40%'},width=4),
    dbc.Col([html.H1(""),dcc.Graph(id='Mpf_tree',config={'displayModeBar': False})],style={'height':'40%'},width=7)]
                                    ,style={'margin-top':10,'margin-left':10}) 
                                    ]),
                            #html.Div(className='six columns div-for-charts', id='table_elave_et',
                             #        children=b[6:],style={'margin-left':20})
                    dcc.Interval(
                        id='interval-component',
                        interval=5 * 60000,  # in milliseconds
                        n_intervals=0
                    ),
                    dcc.Interval(
                        id='interval-component2',
                        interval=5 * 60000+10000,  # in milliseconds
                        n_intervals=0
                    ),html.Div(id='whole')
                    ],style={'background-color':'rgb(226, 222, 222)'})
