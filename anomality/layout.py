import datetime
#import dash_html_components as html
#import dash_core_components as dcc
from dash import dash_table,dcc,html
from datetime import datetime as dt
import pandas as pd

# for formatting of the table
#from dash_table.Format import Format, Group, Scheme, Symbol
#columns=[{"name": i, "id": i}'type': 'numeric','format': Format(
#    scheme=Scheme.fixed,
#    precision=2,
#    group=Group.yes,
#    groups=3,
#    group_delimiter='.',
#    decimal_delimiter=','
#)


temporary_df=pd.DataFrame(columns=['category',1,2,3,4,5,'>5'])


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
             )
        ],style={'margin-top':10,'color':'black',"font-size": "0.8em"}),
html.Div(className="row",children=[
    html.H1('Summary of anomalies grouped by ongoing periods',id='table1_title',style={'font-size':18,'font-color':'#0A2A52','padding-top':5,
                                             'font-weight':'bold',"font-family": "Comic Sans MS"}),
    html.Div(className='twelve columns', children=[dash_table.DataTable(id='table', columns=[],css=[
        { 'selector': '.first-page, .first-page, .previous-page, .next-page, .last-page, .export', 'rule': 'color: black;' }],merge_duplicate_headers=True,
                                                                      style_cell={'textAlign': 'center',
                                                                                  'color': 'black', 'fontSize': 11,
                                                                                  'height':'auto',
                                                                                  'width': '{}%'.format(100/8),
                                                                                  'whiteSpace':'normal',
                                                                                  "font-family": "Comic Sans MS"},
                                                                      style_cell_conditional=[{'if': {
                                                                              'filter_query': '{{{}}} > {}'.format(temporary_df.columns[-1],0)
                                                                              , 'column_id': temporary_df.columns[-1]
                                                                          },
                                                                              'backgroundColor': '#FF4136',
                                                                              'color': 'white','fontWeight': 'bold'
                                                                          },
{'if': {
                                                                              'filter_query': '{{{}}} > {}'.format(temporary_df.columns[-2],0)
                                                                              , 'column_id': temporary_df.columns[-2]
                                                                          },
                                                                              'backgroundColor': 'orange',
                                                                              'color': 'white','fontWeight': 'bold'
                                                                          }
                                                                          ],
                                                                      style_header={
                                                                          'backgroundColor': 'rgb(167, 171, 170)',
                                                                          'fontWeight': 'bold', 'color': 'black',
                                                                          'fontSize': 12},
                                                                     virtualization=True,
                                                                      fixed_rows={'headers': True},
                                                                      style_table={'height': '25vh',
                                                                                   'overflowY': 'auto'
                                                                                   },page_size=6),

html.Div(children=[html.H1(id='table2_title',children=[],style={'font-size':18,'font-color':'#0A2A52','padding-top':5,
                                             'font-weight':'bold',"font-family": "Comic Sans MS"}),
                   dash_table.DataTable(id='table2', columns=[],   style_cell={'textAlign': 'center',
                                                                                  'color': 'black', 'fontSize': 11,
                                                                                  'height':'auto',
                                                                                  'width':'auto',
                                                                                  'whiteSpace':'normal',
                                                                                  "font-family": "Comic Sans MS"},

                                                                      style_header={
                                                                          'backgroundColor': 'rgb(167, 171, 170)',
                                                                          'fontWeight': 'bold', 'color': 'black',
                                                                          'fontSize': 12},
                                                                     virtualization=True,

                                                                      fixed_columns={'headers': True, 'data': 1},
                                                                      #fixed_rows={'headers': True},
                                                                      style_table={'height': '20vh',
                                                                                   'overflowY': 'auto','minWidth': '100%',
                                                                                   },page_size=5)],
         style={'margin-bottom': 30} ),
html.H1(id='table3_title',children=[],style={'font-size':18,'font-color':'#0A2A52','padding-top':5,
                                             'font-weight':'bold',"font-family": "Comic Sans MS"}),
dash_table.DataTable(id='live-table',
                                        columns=[],style_cell={'textAlign': 'center',
                                                                                  'color': 'black', 'fontSize': 11,
                                                                                  'height':'auto',
                                                                                  'whiteSpace':'normal',
                                                                                  "font-family": "Comic Sans MS"},
                     style_header={
                         'backgroundColor': 'rgb(167, 171, 170)',
                         'fontWeight': 'bold', 'color': 'black',
                         'fontSize': 12},
                     virtualization=True,
filter_action='native',
        sort_action="native",
sort_mode='multi',
page_action="native",
                     fixed_rows={'headers': True,'data':0},
                     #fixed_columns={'headers': True, 'data': 0},
                     style_table={
                                  'overflowY': 'auto',
                                  'minWidth': 95,
                                  },export_format="", page_size=10),
html.Div(className='row', children=[html.Div(className='twelve columns',id='graph1',
                                               children=[dcc.Graph(id='graph',style={'height': '45vh','padding-top':10},
                                                           config={'displayModeBar': False})])
]),
html.Div(className='row',children=[html.Div(className='twelve columns', id='gragh2',children=[dcc.Graph(id='graph2',style={'height': '45vh','padding-top':10},
                                                           config={'displayModeBar': False})])]),
html.Div(children=[html.H1(id='table4_title',children=[],style={'font-size':18,'font-color':'#0A2A52','padding-top':5,'padding-top':10,
                                             'font-weight':'bold',"font-family": "Comic Sans MS"}),
                   dash_table.DataTable(id='neighbor-table',
                                        columns=[],style_cell={'textAlign': 'center',
                                                                                  'color': 'black', 'fontSize': 11,
                                                                                  'height':'auto',
                                                                                  'whiteSpace':'normal',
                                                                                  "font-family": "Comic Sans MS"},
                     style_header={
                         'backgroundColor': 'rgb(167, 171, 170)',
                         'fontWeight': 'bold', 'color': 'black',
                         'fontSize': 12},
                     virtualization=True,
filter_action='native',
        sort_action="native",
sort_mode='multi',
page_action="native",
                     fixed_rows={'headers': True,'data':0},
                     #fixed_columns={'headers': True, 'data': 0},
                     style_table={
                                  'overflowY': 'auto',
                                  'minWidth': 95
                                  },export_format="", page_size=10)])

                                                 ]),dcc.Interval(
                        id='interval-component',
                        interval=1000 * 60*15,  # in milliseconds (every 15 minutes)
                        n_intervals=0
                    )], style={'margin-top': 10, 'background-color': '#CAC7C7'}),html.Div(id='whole')],style={'background-color':'rgb(226, 222, 222)'})
#df=pd.read_csv(r'C:\Users\ismayilm\Downloads\Python\Practice\PRB_Utilization\UPMS\Power_util.csv')
#df_cell=pd.read_csv(r'C:\Users\ismayilm\Downloads\Python\Practice\PRB_Utilization\UPMS\Power_util.csv')
