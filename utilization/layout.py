import datetime
import dash_html_components as html
import dash_core_components as dcc
import dash_table
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



today=datetime.date.today()-datetime.timedelta(1)

a=pd.DataFrame(index=[*pd.date_range(start=today-datetime.timedelta(14),end=today)])
a.reset_index(inplace=True)
a['index'] = [dt.strftime(a['index'].iloc[i].to_pydatetime(), '%d.%m.%Y') for i in
                        range(len(a['index']))]
final_df=a.set_index('index').T
final_df.insert(0, 'KPI',0)
final_df.insert(1, 'Measurement Unit',0)
final_df.insert(2, 'Threshold',0)
second_table_cols = final_df.columns[-16:].values.copy()
second_table_cols[0] = 'NE'

layout = html.Div(children=[

    html.Div(className='twelve columns', children=[dash_table.DataTable(id='table', columns=[
        {"name": i, "id": i} for i in final_df.columns],
                                                                      style_cell={'textAlign': 'center',
                                                                                  'color': 'black', 'fontSize': 11,
                                                                                  'height':'auto',
                                                                                  'whiteSpace':'normal',
                                                                                  "font-family": "Comic Sans MS"},
                                                                      style_cell_conditional=[{'if': {'column_id': 'KPI'},
                                                                        'width': '12%'},
                                                                          {'if': {
                                                                              'column_id': 'Measurement Unit'},
                                                                           'width': '5.55%'},
                                                                          {'if': {'column_id': 'Threshold'},
                                                                            'width': '5%'}
                                                                          ],
                                                                      style_header={
                                                                          'backgroundColor': 'rgb(167, 171, 170)',
                                                                          'fontWeight': 'bold', 'color': 'black',
                                                                          'fontSize': 12},
                                                                      style_data_conditional= [{
                                                                              'if': {'row_index': 'odd'},
                                                                              'backgroundColor': 'rgb(230, 234, 233)'},
                                                                          {'if': {
                                                                                    'filter_query': '{{{}}} > {{{}}}'.format(final_df.columns[-1],final_df.columns[-2])
                                                                                   ,'column_id':final_df.columns[-1]
                                                                                },
                                                                                'backgroundColor': 'orange',
                                                                                'color': 'white'
                                                                          },
                                                                          {'if': {
                                                                              'filter_query': '{{{}}} < {{{}}}'.format(final_df.columns[-1],final_df.columns[-2])
                                                                              , 'column_id': final_df.columns[-1]
                                                                          },
                                                                              'backgroundColor': '#3D9970',
                                                                              'color': 'white'
                                                                          },
                                                                          *[{
                                                                              'if': {
                                                    'filter_query': '{{{}}}={} && {{{}}}>0'.format(final_df.columns[-1],
                                                    final_df.iloc[:,4:].max(axis=1)[i],final_df.columns[-1]),
                                                                                  'column_id': final_df.columns[-1],
                                                                                  'row_index':i
                                                                              },
                                                                              'backgroundColor': '#FF4136',
                                                                              'fontWeight': 'bold'
                                                                          } for i in range(len(final_df))]
                                                                        ],

                                                                      virtualization=True,
                                                                      fixed_rows={'headers': True},
                                                                      style_table={'height': '40vh',
                                                                                   'overflowY': 'auto'
                                                                                   },page_size=36),
html.H1(id='table1_title',children=[],style={'font-size':18,'font-color':'white','padding-top':5,
                                             'font-weight':'bold',"font-family": "Comic Sans MS"}),
                                                   dash_table.DataTable(id='table_cell', columns=[
        {"name": i, "id": i} for i in second_table_cols],
                                                                      style_cell={'textAlign': 'center',
                                                                                  'color': 'black', 'fontSize': 11,
                                                                                  "font-family": "Comic Sans MS"},
                                                                      style_header={
                                                                          'backgroundColor': 'rgb(167, 171, 170)',
                                                                          'fontWeight': 'bold', 'color': 'black',
                                                                          'fontSize': 12},
                                                                      style_data_conditional=[
                                                                          {
                                                                              'if': {'row_index': 'odd'},
                                                                              'backgroundColor': 'rgb(230, 234, 233)'
                                                                          }],
                                                                      virtualization=True,
                                                                      fixed_rows={'headers': True},
                                                                      style_table={'height': '40vh',
                                                                                   'overflowY': 'auto'
                                                                                },export_format="csv",page_size=20,

),
html.H1(id='graph_title',children=[],style={'font-size':18,'font-color':'white','padding-top':5,
                                             'font-weight':'bold',"font-family": "Comic Sans MS"}),
dcc.Graph(id='graph',style={'height': '45vh','padding-top':25},config={'displayModeBar': False})
                                                 ])],style={'background-color': 'rgb(94, 92, 92)'})
#df=pd.read_csv(r'C:\Users\ismayilm\Downloads\Python\Practice\PRB_Utilization\UPMS\Power_util.csv')
#df_cell=pd.read_csv(r'C:\Users\ismayilm\Downloads\Python\Practice\PRB_Utilization\UPMS\Power_util.csv')
