from dash import ctx, dash_table,dcc,html,Dash
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
                    dbc.Col([dbc.Row([dbc.Col([dbc.Row(dcc.Dropdown(id='dropdown',
                   options=['A', 'B', 'C'],
   value='A'
)), dbc.Row(dcc.Dropdown(id='dropdown_region',
                   options=['All Regions','Absheron','Aran','Baku','Ganja','Lankaran','Naxchivan','Qarabag','Quba','Sheki'],
   value='All Regions'
)),
dbc.Row([html.Button('Download site level info', id='btn',style={'color':'rgba(0, 0, 0, 0.7)','background-color': 'transparent'}),dcc.Download(id="download")])],width=2),
    dbc.Col(dbc.Card([dbc.CardHeader(html.H6("Total Down Alarms")),dbc.CardBody([
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
    )],color="info"),width=2,style={'height':'10%'})],#align='center',

#dbc.Col(dcc.Graph(id='Down_card')),dbc.Col(dcc.Graph(id='Mpf_card')),dbc.Col(dcc.Graph(id='Generator_card')) ],
style={'margin-left':10,'margin-top':10,'color':'black',"font-size": "0.8em","font-family": "Comic Sans MS",'height':"10%"},className='h-5'), #"width": "30%"

dbc.Row([
    dbc.Col([dbc.Row(dash_table.DataTable(id='table',columns=[
        {"name": i, "id": i} for i in ['location','Unique down','MPF start','PG start','SG start','status']],
        style_table={'height': 342},
        style_cell={'textAlign': 'center','color': 'black','fontSize':13},
        style_header={'backgroundColor': 'rgb(167, 171, 170)',
                'fontWeight': 'bold','color':'black','fontSize':13})),
                dbc.Row([dbc.Col([html.P('🔥 - more than 30 down',style={"font-size":13}),
                         html.P('🚒 - more than 20 down',style={"font-size":13})]),
                         dbc.Col([html.P('⚠️ - more than 10 down',style={"font-size":13}),
                         html.P('🟢 - less than 10 down',style={"font-size":13})])]),
                dbc.Row(dcc.Graph(id='availability'))],width=4),
    dbc.Col([dbc.Row(html.Div(id='map')),
            dbc.Row([dbc.Col(dcc.Graph(id='down_graph'),width=6),dbc.Col(dcc.Graph(id='mpf_graph'),width=6)])],width=5),
    dbc.Col([dash_table.DataTable(id='table2',columns=[
        {"name": i, "id": i} for i in ['location','admin_region','Unique down','MPF start','PG start','SG start']],
        style_table={'height': 400,'overflowY': 'auto'},
        style_cell={'textAlign': 'center','color': 'black','fontSize':12},
        style_header={'backgroundColor': 'rgb(167, 171, 170)',
                'fontWeight': 'bold','color':'black','fontSize':12},
        filter_action='native',
        fixed_rows={'headers': True, 'data': 0},
        sort_action="native",
        sort_mode='multi',
        page_action="native",
        style_cell_conditional=[
        {'if': {'column_id': 'location'},
         'width': '15%'},
        {'if': {'column_id': 'admin_region'},
         'width': '20%'},{'if': {'column_id': 'Unique down'},
         'width': '20%'}
    ]#,export_format='xlsx'
        ),dash_table.DataTable(id='table3',columns=[
        {"name": i, "id": i} for i in ['SITE_ID','Site Name','Region','Down_duration']],
        style_table={'height': 350,'overflowY': 'auto'},
        style_cell={'textAlign': 'center','color': 'black','fontSize':12},
        style_header={'backgroundColor': 'rgb(167, 171, 170)',
                'fontWeight': 'bold','color':'black','fontSize':12},
        filter_action='native',
        fixed_rows={'headers': True, 'data': 0},
        sort_action="native",
        sort_mode='multi',
        page_action="native",#,export_format='xlsx'
        style_cell_conditional=[
        {'if': {'column_id': 'Site Name'},
         'width': '30%'},
        {'if': {'column_id': 'Region'},
         'width': '25%'},
    ]
        )],width=3)
],style={'margin-left':2,'margin-top':10,'color':'black',"font-size": "0.5em","font-family": "Comic Sans MS",'height':"70%"},className='h-50'),
],style={'height':'70%'}),


                            #html.Div(className='six columns div-for-charts', id='table_elave_et',
                             #        children=b[6:],style={'margin-left':20})
                    dcc.Interval(
                        id='interval-component',
                        interval=15 * 60000,  # in milliseconds
                        n_intervals=0
                    ),
                    dcc.Interval(
                        id='interval-component2',
                        interval=15 * 60000+10000,  # in milliseconds
                        n_intervals=0
                    ),html.Div(id='whole')
                    ],style={'background-color':'rgb(226, 222, 222)'})
