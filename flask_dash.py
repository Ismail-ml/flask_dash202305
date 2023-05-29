from flask import Flask, render_template, url_for, request, redirect, send_file, send_from_directory,session,flash, Response
import numpy as np
import pandas as pd
import pickle, subprocess
import datetime
import os
from dash import Dash
from werkzeug.middleware.dispatcher import DispatcherMiddleware
import flask
import dash_bootstrap_components as dbc
from werkzeug.serving import run_simple
#from dashapp1.layout import layout as layout1
#from dashapp1.callbacks import register_callback as callback1
from dashapp2.layout import layout as layout2
from dashapp2.callbacks import register_callback as callback2
from dashapp3.layout import layout as layout3
from dashapp3.callbacks import register_callback as callback3
from core.layout import layout as layout_core
from core.callbacks import register_callback as callback_core
from anomality.layout import layout as layout_anom
from anomality.callback import register_callback as callback_anom
from it_billing.layout import layout as layout_bill
from it_billing.callbacks import register_callback as callback_bill
#from flm3.layout import layout as layout_flm
#from flm3.callback import register_callback as callback_flm
from flm3.layout import layout as layout_flm2
from flm3.callback import register_callback as callback_flm2
#from utilization.layout import layout as layout_util
#from utilization.callback import register_callback as callback_util
from flask.helpers import get_root_path

server = Flask(__name__)
server.secret_key="nese"

#dashapp1 = Dash(__name__, server=server, url_base_pathname='/worst_sites_daily/',
#               assets_folder=get_root_path(__name__)+'/dashapp1/assets/')
dashapp2 = Dash(__name__, server = server, url_base_pathname='/dashboard/',
                assets_folder=get_root_path(__name__)+'/dashapp2/assets/')
dashapp3 = Dash(__name__, server = server, url_base_pathname='/worst_sites/',
                assets_folder=get_root_path(__name__)+'/dashapp3/assets/')
core = Dash(__name__, server = server, url_base_pathname='/core_dashboard/',
                assets_folder=get_root_path(__name__)+'/dashapp2/assets/')
anomality = Dash(__name__, server=server, url_base_pathname='/anomality/',
               assets_folder=get_root_path(__name__)+'/dashapp2/assets/')
billing = Dash(__name__, server = server, url_base_pathname='/billing_dashboard/',
                assets_folder=get_root_path(__name__)+'/it_billing/assets/')
#flm = Dash(__name__, server = server, url_base_pathname='/flm2/',external_stylesheets=[dbc.themes.BOOTSTRAP],
#                assets_folder=get_root_path(__name__)+'/it_billing/assets/')
flm2 = Dash(__name__, server = server, url_base_pathname='/flm_mobile/',external_stylesheets=[dbc.themes.BOOTSTRAP],
                assets_folder=get_root_path(__name__)+'/it_billing/assets/')
#util = Dash(__name__, server = server, url_base_pathname='/utilization/',
#            assets_folder=get_root_path(__name__)+'/utilization/assets/')

#dashapp1.layout = layout1
#dashapp1.title='Worst Sites'
dashapp2.layout = layout2
dashapp2.title='RAN Dashboard'
dashapp3.layout = layout3
dashapp3.title='Worst Sites'
core.layout= layout_core
core.title='Core Dashboard'
anomality.layout=layout_anom
anomality.title = "Anomality Detection"
billing.layout=layout_bill
billing.title='Billing Dashboard'
#flm.layout=layout_flm
#flm.title='FLM Dashboard'
flm2.layout=layout_flm2
flm2.title='FLM Mobile Dashboard'
#util.layout= layout_util
#util.title='Utilization Summary'
#callback1(dashapp1)
callback2(dashapp2)
callback3(dashapp3)
callback_core(core)
callback_anom(anomality)
callback_bill(billing)
#callback_flm(flm)
callback_flm2(flm2)
#callback_util(util)

BSCs = ['NBSC01', 'NBSC02', 'NBSC03', 'NBSC04', 'NBSC05', 'NBSC06', 'NBSC08', 'NBSC09', 'NBSC10', 'NBSC11',
    'NBSC13', 'NBSC14','NBSC15', 'NBSC18','mcBSC12','mcBSC16','HBSC01', 'HBSC02', 'HBSC03', 'HBSC04', 'HBSC05', 'HBSC06', 'HBSC07', 'HBSC08', 'HBSC09'
    , 'HBSC10', 'HBSC11', 'HBSC12','ASBSC21','ASBSC22']
RNCs = ['RNC01', 'RNC02', 'RNC03', 'RNC04', 'RNC05', 'RNC06', 'RNC07','mcRNC08','mcRNC09','ARNC21',
        'HRNC01', 'HRNC02', 'HRNC03', 'HRNC04','HRNC05', 'HRNC07','HRNC08']
Vendors = ['Huawei', 'Nokia']
#Sites = ['BBK0004', 'BBK0084', 'BBK0654', 'BBK0045']
Regions = ['Absheron', 'Aran', 'Baku', 'Ganja', 'Naxchivan', 'Sheki', 'Lankaran', 'Quba', 'Qarabag']
KPIs={'2G':{'Call Setup SR':"'cssr_den1','cssr_den2','cssr_den3','cssr_num1','cssr_num2','cssr_num3'",
           'Call Drop Rate':"'drop_rate_num','drop_rate_den'",
           'Call Block Rate':"'call_block_rate_num','call_block_rate_den'",
           'SDCCH Drop Rate':"'sdcch_drop_rate_den','sdcch_drop_rate_num'",
           'SDCCH Block Rate':"'sdcch_block_rate_den','sdcch_block_rate_num'",
           'Handover SR':"'hosr_den','hosr_num'",
           'Cell Availability':"'cell_avail_den','cell_avail_num','cell_avail_blck_num','cell_avail_blck_den'",
           'Cell Availability excl blck':"'cell_avail_den','cell_avail_num','cell_avail_blck_num','cell_avail_blck_den'",
           'TCH Availability':"'tch_avail_den','tch_avail_num'",
           'SDCCH Availability':"'sdcch_avail_den','sdcch_avail_num'",
           'TBF Est SR':"'tbf_est_sr_den','tbf_est_sr_num'",
           'TBF Drop Rate':"'tbf_drop_rate_den','tbf_drop_rate_num'",
           'CS Traffic, Erl':"'cs_traffic_erl'",
           'PS Traffic, MB':"'ps_traffic_mb'",
           'Combined Thrp, Kbps':"'comb_thrp_den','comb_thrp_num'",
           'Call Setup Fails':"'cssr_den1','cssr_den2','cssr_den3','cssr_num1','cssr_num2','cssr_num3'",
           'Call Setup Den':"'cssr_den1','cssr_den2','cssr_den3','cssr_num1','cssr_num2','cssr_num3'",
           'Call Drops':"'drop_rate_num','drop_rate_den'",
           'Call Blocks':"'call_block_rate_num','call_block_rate_den'",
           'SDCCH Drops':"'sdcch_drop_rate_den','sdcch_drop_rate_num'",
           'SDCCH Blocks':"'sdcch_block_rate_den','sdcch_block_rate_num'",
           'TBF Drops':"'tbf_drop_rate_den','tbf_drop_rate_num'",
           'TBF Est fails':"'tbf_est_sr_den','tbf_est_sr_num'",
           'Handover fails':"'hosr_den','hosr_num'", 'Handover attempts':"'hosr_den','hosr_num'"
           },
     '3G':{'Voice Call Setup SR':"'voice_sr_num1','voice_sr_den1','voice_sr_num2','voice_sr_den2'",
           'Voice Call DR':"'voice_dr_num','voice_dr_den'",
           'HSDPA RAB SR':"'hsdpa_sr_num','hsdpa_sr_den'",
           'HSUPA RAB SR':"'hsupa_sr_num','hsupa_sr_den'",
           'HSDPA Drop Rate':"'hsdpa_dr_num','hsdpa_dr_den'",
           'HSUPA Drop Rate':"'hsupa_dr_num','hsupa_dr_den'",
           'R99 Setup SR':"'r99_sr_num','r99_sr_den'",
           'R99 Drop Rate':"'r99_dr_num','r99_dr_den'",
           'CS Soft HOSR':"'cs_sho_ho_num','cs_sho_ho_den'",
           'PS Soft HOSR':"'ps_sho_ho_num','ps_sho_ho_den'",
           'CS IRAT HOSR':"'cs_inter_rat_ho_num','cs_inter_rat_ho_den'",
           'CS InterFreq HOSR':"'cs_inter_freq_ho_num','cs_inter_freq_ho_den'",
           'PS InterFreq HOSR':"'ps_inter_freq_ho_num','ps_inter_freq_ho_den'",
           'Cell Availability':"'cell_avail_num','cell_avail_den','cell_avail_blck_num','cell_avail_blck_den'",
           'Cell Availability excl blck':"'cell_avail_num','cell_avail_den','cell_avail_blck_num','cell_avail_blck_den'",
           'Average HSDPA user thrp, Kbps':"'hsdpa_thrp_num','hsdpa_thrp_den'",
           'CS Traffic, Erl':"'cs_traf'",
           'PS Traffic, MB':"'ps_traf'",
           'Interference':"'rtwp_num','rtwp_den'",
           'Call Setup fails':"'voice_sr_num1','voice_sr_den1','voice_sr_num2','voice_sr_den2'",
           'Call drops':"'voice_dr_num','voice_dr_den'",
           'Call Setup attempts':"'voice_sr_num1','voice_sr_den1','voice_sr_num2','voice_sr_den2'",
           'HSDPA Setup fails':"'hsdpa_sr_num','hsdpa_sr_den'",
           'HSDPA Setup attempts':"'hsdpa_sr_num','hsdpa_sr_den'",
           'HSDPA drops':"'hsdpa_dr_num','hsdpa_dr_den'",
           'HSUPA Setup fails':"'hsupa_sr_num','hsupa_sr_den'",
           'HSUPA Setup attempts':"'hsupa_sr_num','hsupa_sr_den'",
           'HSUPA drops':"'hsupa_dr_num','hsupa_dr_den'",
           'CS Soft HO fails':"'cs_sho_ho_num','cs_sho_ho_den'",
           'CS Soft HO attempts':"'cs_sho_ho_num','cs_sho_ho_den'",
           'PS Soft HO fails':"'ps_sho_ho_num','ps_sho_ho_den'",
           'PS Soft HO attempts':"'ps_sho_ho_num','ps_sho_ho_den'",
           'CS IRAT HO attempts':"'cs_inter_rat_ho_num','cs_inter_rat_ho_den'",
           'CS IRAT HO fails':"'cs_inter_rat_ho_num','cs_inter_rat_ho_den'",
           'CS InterFreq HO attempts':"'cs_inter_freq_ho_num','cs_inter_freq_ho_den'",
           'CS InterFreq HO fails':"'cs_inter_freq_ho_num','cs_inter_freq_ho_den'",
           'PS InterFreq HO attempts':"'ps_inter_freq_ho_num','ps_inter_freq_ho_den'",
           'PS InterFreq HO fails':"'ps_inter_freq_ho_num','ps_inter_freq_ho_den'"},
     '4G':{'RRC Setup SR':"'rrc_sr_num','rrc_sr_den'",
           'RAB Setup SR':"'rab_sr_num','rab_sr_den'",
           'Session Setup SR':"'rrc_sr_num','rrc_sr_den','rab_sr_num','rab_sr_den'",
           'CSFB SR':"'csfb_sr_num','csfb_sr_den'",
           'VoLTE Setup SR':"'volte_sr_num','volte_sr_den'",
           'VoLTE Drop Rate':"'volte_dr_num','volte_dr_den'",
            'VoLTE SRVCC SR':"'volte_srvcc_e2w_num','volte_srvcc_e2w_den'",
            'VoLTE DL Silent cals':"'Voice_DL_Silent_Num'",
            'VoLTE UL Silent cals':"'Voice_UL_Silent_Num'",
            'VQI DL Accept times': "'Voice_VQI_DL_Accept_Times'",
            'VQI DL Bad times':"'Voice_VQI_DL_Bad_Times'",
            'VQI DL Excellent times':"'Voice_VQI_DL_Excellent_Times'",
            'VQI DL Good times':"'Voice_VQI_DL_Good_Times'",
            'VQI DL Poor times':"'Voice_VQI_DL_Poor_Times'",
            'VQI UL Accept times':"'Voice_VQI_UL_Accept_Times'",
            'VQI UL Bad times':"'Voice_VQI_UL_Bad_Times'",
            'VQI UL Excellent times':"'Voice_VQI_UL_Excellent_Times'",
            'VQI UL Good times':"'Voice_VQI_UL_Good_Times'",
            'VQI UL Poor times':"'Voice_VQI_UL_Poor_Times'",
           'Session Drop Rate':"'dcr_num','dcr_den'",
           'Cell Availability':"'cell_avail_num','cell_avail_den','cell_avail_blck_num','cell_avail_blck_den'",
           'Cell Availability excl blck':"'cell_avail_num','cell_avail_den','cell_avail_blck_num','cell_avail_blck_den'",
           'IntraFreq HOSR':"'intra_freq_ho_num','intra_freq_ho_den'",
           '4G-3G IRAT HOSR':"'irat_ho_num','irat_ho_den'",
           'DL Traffic, MB':"'dl_ps_traf'",
           'UL Traffic, MB':"'ul_ps_traf'",
           'Total Traffic, MB':"'dl_ps_traf','ul_ps_traf'",
           'Volte PS Traffic, MB':"'volte_dl_ps_traf','volte_ul_ps_traf'",
           'Volte CS Traffic, Erl':"'volte_cs_traf'",
           'Interference':"'rtwp'",
           'DL Throughput, Kbps':"'dl_thrp_num','dl_thrp_den'",
           'UL Throughput, Kbps':"'ul_thrp_num','ul_thrp_den'",
           'RRC Setup fails':"'rrc_sr_num','rrc_sr_den'",
           'RRC Setup attempts':"'rrc_sr_num','rrc_sr_den'",
           'RAB Setup fails':"'rab_sr_num','rab_sr_den'",
           'RAB Setup attempts':"'rab_sr_num','rab_sr_den'",
           'Session drops':"'dcr_num','dcr_den'",
           'CSFB fails':"'csfb_sr_num','csfb_sr_den'",
           'CSFB attempts':"'csfb_sr_num','csfb_sr_den'",
           'Volte fails':"'volte_sr_num','volte_sr_den'",
           'Volte attempts':"'volte_sr_num','volte_sr_den'",
           'Volte drops':"'volte_dr_num','volte_dr_den'",
           'IntraFreq HO fails':"'intra_freq_ho_num','intra_freq_ho_den'",
           'IntraFreq HO attempts':"'intra_freq_ho_num','intra_freq_ho_den'",
           '4G-3G IRAT HO fails':"'irat_ho_num','irat_ho_den'",
           '4G-3G IRAT HO attempts':"'irat_ho_num','irat_ho_den'"}}
Formula={'2G':{'Call Setup SR':"100*(1-tt['cssr_num1']/tt['cssr_den1'])*tt['cssr_num2']/tt['cssr_den2']*tt['cssr_num3']/tt['cssr_den3']",
           'Call Drop Rate':"tt['drop_rate_num']/tt['drop_rate_den']*100",
           'Call Block Rate':"tt['call_block_rate_num']/tt['call_block_rate_den']*100",
           'SDCCH Drop Rate':"tt['sdcch_drop_rate_num']/tt['sdcch_drop_rate_den']*100",
           'SDCCH Block Rate':"tt['sdcch_block_rate_num']/tt['sdcch_block_rate_den']*100",
           'Handover SR':"tt['hosr_num']/tt['hosr_den']*100",
           'Cell Availability':"tt['cell_avail_num']/tt['cell_avail_den']*100",
           'Cell Availability excl blck':"(tt['cell_avail_num']+tt['cell_avail_blck_num'])/(tt['cell_avail_den']-tt['cell_avail_blck_den'])*100",
           'TCH Availability':"tt['tch_avail_num']/tt['tch_avail_den']*100",
           'SDCCH Availability':"tt['sdcch_avail_num']/tt['sdcch_avail_den']*100",
           'TBF Est SR':"tt['tbf_est_sr_num']/tt['tbf_est_sr_den']*100",
           'TBF Drop Rate':"tt['tbf_drop_rate_num']/tt['tbf_drop_rate_den']*100",
           'CS Traffic, Erl':"tt['cs_traffic_erl']",
           'PS Traffic, MB':"tt['ps_traffic_mb']",
           'Combined Thrp, Kbps':"tt['comb_thrp_num']/tt['comb_thrp_den']",
           'Call Setup Fails':"tt['cssr_num1']+tt['cssr_den2']+tt['cssr_den3']-tt['cssr_num2']-tt['cssr_num3']",
           'Call Setup Den':"tt['cssr_den1']+tt['cssr_den2']+tt['cssr_den3']",
           'Call Drops':"tt['drop_rate_num']",
           'Call Blocks':"tt['call_block_rate_num']",
           'SDCCH Drops':"tt['sdcch_drop_rate_num']",
           'SDCCH Blocks':"tt['sdcch_block_rate_num']",
           'TBF Drops':"tt['tbf_drop_rate_num']",
           'TBF Est fails':"tt['tbf_est_sr_den']-tt['tbf_est_sr_num']",
           'Handover fails':"tt['hosr_den']-tt['hosr_num']", 'Handover attempts':"tt['hosr_den']"
           },
        '3G':{'Voice Call Setup SR':"100*tt['voice_sr_num1']/tt['voice_sr_den1']*tt['voice_sr_num2']/tt['voice_sr_den2']",
           'Voice Call DR':"100*tt['voice_dr_num']/tt['voice_dr_den']",
           'HSDPA RAB SR':"100*tt['hsdpa_sr_num']/tt['hsdpa_sr_den']",
           'HSUPA RAB SR':"100*tt['hsupa_sr_num']/tt['hsupa_sr_den']",
           'HSDPA Drop Rate':"100*tt['hsdpa_dr_num']/tt['hsdpa_dr_den']",
           'HSUPA Drop Rate':"100*tt['hsupa_dr_num']/tt['hsupa_dr_den']",
           'R99 Setup SR':"100*tt['r99_sr_num']/tt['r99_sr_den']",
           'R99 Drop Rate':"100*tt['r99_dr_num']/tt['r99_dr_den']",
           'CS Soft HOSR':"100*tt['cs_sho_ho_num']/tt['cs_sho_ho_den']",
           'PS Soft HOSR':"100*tt['ps_sho_ho_num']/tt['ps_sho_ho_den']",
           'CS IRAT HOSR':"100*tt['cs_inter_rat_ho_num']/tt['cs_inter_rat_ho_den']",
           'CS InterFreq HOSR':"100*tt['cs_inter_freq_ho_num']/tt['cs_inter_freq_ho_den']",
           'PS InterFreq HOSR':"100*tt['ps_inter_freq_ho_num']/tt['ps_inter_freq_ho_den']",
           'Cell Availability':"100*tt['cell_avail_num']/tt['cell_avail_den']",
           'Cell Availability excl blck':"100*(tt['cell_avail_num']+tt['cell_avail_blck_num'])/(tt['cell_avail_den']-tt['cell_avail_blck_den'])",
           'Average HSDPA user thrp, Kbps':"tt['hsdpa_thrp_num']/tt['hsdpa_thrp_den']",
           'CS Traffic, Erl':"tt['cs_traf']",
           'PS Traffic, MB':"tt['ps_traf']",
           'Interference':"tt['rtwp_num']/tt['rtwp_den']",
           'Call Setup fails':"tt['voice_sr_den1']+tt['voice_sr_den2']-tt['voice_sr_num1']-tt['voice_sr_num2']",
           'Call drops':"tt['voice_dr_num']",
           'Call Setup attempts':"tt['voice_sr_den1']+tt['voice_sr_den2']",
           'HSDPA Setup fails':"tt['hsdpa_sr_den']-tt['hsdpa_sr_num']",
           'HSDPA Setup attempts':"tt['hsdpa_sr_den']",
           'HSDPA drops':"tt['hsdpa_dr_num']",
           'HSUPA Setup fails':"tt['hsupa_sr_den']-tt['hsupa_sr_num']",
           'HSUPA Setup attempts':"tt['hsupa_sr_den']",
           'HSUPA drops':"tt['hsupa_dr_num']",
           'CS Soft HO fails':"tt['cs_sho_ho_den']-tt['cs_sho_ho_num']",
           'CS Soft HO attempts':"tt['cs_sho_ho_den']",
           'PS Soft HO fails':"tt['ps_sho_ho_den']-tt['ps_sho_ho_num']",
           'PS Soft HO attempts':"tt['ps_sho_ho_den']",
           'CS IRAT HO attempts':"tt['cs_inter_rat_ho_den']",
           'CS IRAT HO fails':"tt['cs_inter_rat_ho_den']-tt['cs_inter_rat_ho_num']",
           'CS InterFreq HO attempts':"tt['cs_inter_freq_ho_den']",
           'CS InterFreq HO fails':"tt['cs_inter_freq_ho_den']-tt['cs_inter_freq_ho_num']",
           'PS InterFreq HO attempts':"tt['ps_inter_freq_ho_den']",
           'PS InterFreq HO fails':"tt['ps_inter_freq_ho_den']-tt['ps_inter_freq_ho_num']"},
        '4G':{'RRC Setup SR':"100*tt['rrc_sr_num']/tt['rrc_sr_den']",
           'RAB Setup SR':"100*tt['rab_sr_num']/tt['rab_sr_den']",
           'Session Setup SR':"100*tt['rrc_sr_num']/tt['rrc_sr_den']*tt['rab_sr_num']/tt['rab_sr_den']",
           'CSFB SR':"100*tt['csfb_sr_num']/tt['csfb_sr_den']",
           'Volte Setup SR':"100*tt['volte_sr_num']/tt['volte_sr_den']",
           'Volte Drop Rate':"100*tt['volte_dr_num']/tt['volte_dr_den']",
           'Session Drop Rate':"100*tt['dcr_num']/tt['dcr_den']",
            'VoLTE SRVCC SR':"100*tt['volte_srvcc_e2w_num']/tt['volte_srvcc_e2w_den']",
            'VoLTE DL Silent cals':"tt['Voice_DL_Silent_Num']",
            'VoLTE UL Silent cals':"tt['Voice_UL_Silent_Num']",   
            'VQI DL Accept times': "'Voice_VQI_DL_Accept_Times'",
            'VQI DL Bad times':"'Voice_VQI_DL_Bad_Times'",
            'VQI DL Excellent times':"'Voice_VQI_DL_Excellent_Times'",
            'VQI DL Good times':"'Voice_VQI_DL_Good_Times'",
            'VQI DL Poor times':"'Voice_VQI_DL_Poor_Times'",
            'VQI UL Accept times':"'Voice_VQI_UL_Accept_Times'",
            'VQI UL Bad times':"'Voice_VQI_UL_Bad_Times'",
            'VQI UL Excellent times':"'Voice_VQI_UL_Excellent_Times'",
            'VQI UL Good times':"'Voice_VQI_UL_Good_Times'",
            'VQI UL Poor times':"'Voice_VQI_UL_Poor_Times'",
           'Cell Availability':"100*tt['cell_avail_num']/tt['cell_avail_den']",
           'Cell Availability excl blck':"100*(tt['cell_avail_num']+tt['cell_avail_blck_num'])/(tt['cell_avail_den']-tt['cell_avail_blck_den'])",
           'IntraFreq HOSR':"100*tt['intra_freq_ho_num']/tt['intra_freq_ho_den']",
           '4G-3G IRAT HOSR':"100*tt['irat_ho_num']/tt['irat_ho_den']",
           'DL Traffic, MB':"tt['dl_ps_traf']",
           'UL Traffic, MB':"tt['ul_ps_traf']",
           'Total Traffic, MB':"tt['dl_ps_traf']+tt['ul_ps_traf']",
           'Volte PS Traffic, MB':"tt['volte_dl_ps_traf']+tt['volte_ul_ps_traf']",
           'Volte CS Traffic, Erl':"tt['volte_cs_traf']",
           'Interference':"tt['rtwp']",
           'DL Throughput, Kbps':"tt['dl_thrp_num']/tt['dl_thrp_den']",
           'UL Throughput, Kbps':"tt['ul_thrp_num']/tt['ul_thrp_den']",
           'RRC Setup fails':"tt['rrc_sr_den']-tt['rrc_sr_num']",
           'RRC Setup attempts':"tt['rrc_sr_den']",
           'RAB Setup fails':"tt['rab_sr_den']-tt['rab_sr_num']",
           'RAB Setup attempts':"tt['rab_sr_den']",
           'Session drops':"tt['dcr_num']",
           'CSFB fails':"tt['csfb_sr_den']-tt['csfb_sr_num']",
           'CSFB attempts':"tt['csfb_sr_den']",
           'Volte fails':"tt['volte_sr_den']-tt['volte_sr_num']",
           'Volte attempts':"tt['volte_sr_den']",
           'Volte drops':"tt['volte_dr_num']",
           'IntraFreq HO fails':"tt['intra_freq_ho_den']-tt['intra_freq_ho_num']",
           'IntraFreq HO attempts':"tt['intra_freq_ho_den']",
           '4G-3G IRAT HO fails':"tt['irat_ho_den']-tt['irat_ho_num']",
           '4G-3G IRAT HO attempts':"tt['irat_ho_den']"}}

class page:
    a=None

route=page()

@server.route("/", methods=('GET', 'POST'))
@server.route("/home", methods=('GET', 'POST'))
def home():
    return render_template('home.html')


@server.route("/result", methods=['POST'])
def result():
    # df=data.df
    # df['PERIOD_START_TIME']=pd.to_datetime(df['PERIOD_START_TIME'], format='%m.%d.%Y %H:%M:%S')

    class MyStr(str):
        """ Special string subclass to override the default representation method
            which puts single quotes around the result.
        """

    def __repr__(self):
        return super(MyStr, self).__repr__().strip('"')

    session["grp"] = []
    grp=list(session["grp"])

    if len(str(request.form.getlist("Clusterlist")).split(r'\r\n')) >= 5:
        yy = str(request.form.getlist("Clusterlist")).split(r'\r\n')
        yu = [i.replace("['", "").replace("']", "") for i in yy]
        session["NeSelected"] = yu
        session["filt"] = str("Site_name in NeSelected")
        session['select_level']='Cluster'
    elif len(request.form.getlist("BSClist")) != 0:
        session["NeSelected"] = request.form.getlist("BSClist")
        # filt=str("(df['BSC name'].isin(NeSelected))")
        session["filt"] = str("BSC_name=NeSelected")
        session['select_level'] = 'BSC'
    elif len(request.form.getlist("RNClist")) != 0:
        session["NeSelected"] = request.form.getlist("RNClist")
        # filt=str("(df['BSC name'].isin(NeSelected))")
        session["filt"] = str("RNC_name=NeSelected")
        session['select_level'] = 'RNC'
    elif len(request.form.getlist("Regionlist")) != 0:
        session["NeSelected"] = request.form.getlist("Regionlist")
        session["filt"] = str("Region=NeSelected")
        session['select_level'] = 'Region'
    elif len(request.form.getlist("Vendorlist")) != 0:
        session["NeSelected"] = request.form.getlist("Vendorlist")
        session["filt"] = str("Vendor=NeSelected")
        session['select_level'] = 'Vendor'
    elif len(request.form.getlist("Sitelist")) != 0:
        session["NeSelected"] = request.form.getlist("Sitelist")
        # filt=str("(df['BTS name'].apply(lambda x: x[:8]).isin(NeSelected))")
        session["filt"] = str("Site_name=NeSelected")
        session['select_level'] = 'Site'
    else:
        session["filt"] = str("Vendor=['Nokia','Huawei']")
    NeSelected=session["NeSelected"]
    select_level=session['select_level']
    filt=session["filt"]
    session["ReportLevel"] = request.form.get("report_level")
    session["TimeLevel"] = request.form.get("time_resolution")
    session["StartDate"] = request.form.get("start_date")
    session["EndDate"] = request.form.get("end_date")
    session["KPIselected"] = request.form.getlist("KPIlist")

    ReportLevel=pickle.dumps(session["ReportLevel"])
    TimeLevel=session["TimeLevel"]
    StartDate=session["StartDate"]
    EndDate=session["EndDate"]
    KPIselected=session["KPIselected"]

    col=['Date','Vendor','Region']
    if (pickle.loads(ReportLevel)!='Site_name') & (pickle.loads(ReportLevel)!='Cell_name') & (pickle.loads(ReportLevel)!='Cluster') & (select_level!='Site') & (select_level!='Cluster'):
        extension='/bsc'
    else:
        extension=''
        col.append('Site_name')
        col.append('Cell_name')


    # filt= filt + ' & ' + str("(df['PERIOD_START_TIME']>StartDate)") + ' & ' + str("(df['PERIOD_START_TIME']<EndDate)")

    tech=session['tech']
    if '3G' in tech:
        path='/threeG'+extension
        col.append('RNC_name')

    elif '2G' in tech:
        path='/twoG'+extension
        col.append('BSC_name')

    elif '4G' in tech:
        path='/fourG'+extension

    p = []
    [p.append(eval(MyStr(KPIs[tech][i]))) if KPIs[tech][i].find(',')<0 else [p.append(j) for j in eval(MyStr(KPIs[tech][i]))] for i in KPIselected]
    [col.append(i) for i in p]

    #files=pd.date_range(StartDate,datetime.datetime.strptime(EndDate,'%Y-%m-%d')+datetime.timedelta(days=30),
    #          freq='M').strftime("%B_%Y").tolist()
    files = pd.date_range(StartDate, EndDate, freq='24H').strftime('%Y-%m-%d').tolist()
    to_concat=[]
    for i in files:
        if os.path.isfile(os.path.join(r'/disk2/support_files/archive/ran',i+'.h5')):
            to_concat.append(pd.read_hdf(os.path.join(r'/disk2/support_files/archive/ran',i+'.h5'),key=path,
                                         where=['Date>=StartDate & Date<EndDate & ' + MyStr(filt)],columns=list(set(col))))
    df=pd.concat(to_concat)


    # df['Site name']=df['BTS name'].apply(lambda x: x[:8])

    grp.append(pickle.loads(ReportLevel))
    if pickle.loads(ReportLevel) == 'Network':
        grp = []

    # df=df[eval(MyStr(filt))]
    if TimeLevel == 'Monthly':
        grp.append(pd.DatetimeIndex(df['Date']).strftime('%B_%Y'))
    elif TimeLevel == 'Daily':
        grp.append(pd.DatetimeIndex(df['Date']).strftime('%d.%m.%Y'))
    else:
        grp.append(df['Date'])
    if ('4G' in tech) & ('Interference' in KPIselected):
        agg_dict = dict.fromkeys(list(set(p)), 'sum')
        agg_dict['rtwp'] = 'mean'
        tt = pd.DataFrame(df.groupby(grp).agg(agg_dict))
    else:
        tt = pd.DataFrame(df.groupby(grp).sum())
    ff = pd.DataFrame([round(10*np.log10(eval(MyStr(Formula[tech][d]), {}, {'tt': tt})),2) if (('3G' in tech) & (d=='Interference')) \
                           else round(eval(MyStr(Formula[tech][d]), {}, {'tt': tt}), 2) for d in KPIselected], index=KPIselected).T
    #if ('3G' in tech) & ('Interference' in KPIselected):
     #   ff['Interference']=round(10*np.log10(ff['Interference']),2)
    if ff.index.nlevels > 1:
        ff.index.rename('Date', level=1, inplace=True)
    else:
        ff.index.rename('Date', inplace=True)
    ff.reset_index(inplace=True)
    if TimeLevel == 'Monthly':
        ff['Date'] = pd.to_datetime(ff['Date'], format="%B_%Y")
    else:
        ff['Date'] = pd.to_datetime(ff['Date'], format="%d.%m.%Y")
    ff.sort_values(by='Date',inplace=True)

    if request.form.get("direct_export") == 'Yes':

        return send_file(filename_or_fp=r'/home/ismayil/Downloads/result.csv',
                         as_attachment=True, attachment_filename='result.csv', mimetype='/text-csv')

    else:
        return render_template('result.html', data=ff.to_dict(orient='records'), titles=ff.columns.values)


@server.route("/2G", methods=('GET', 'POST'))
def twoG():
    # df=pd.read_hdf(r"C:\Users\ismayilm\Downloads\Python\Practice\PRB_Utilization\UPMS\nokia_agr1.hdf",key='/data')
    # data.df=df
    session['tech'] = "2G"
    Sites = pd.read_csv(r'/home/ismayil/flask_dash/support_files/2G_sites.csv').values[:,
            1].tolist()

    return render_template('2G.html', title='2G', BSCs=BSCs, Vendors=Vendors, Sites=Sites, Regions=Regions,
                           KPIs=list(KPIs['2G'].keys()))


@server.route("/3G", methods=('GET', 'POST'))
def threeG():
    session['tech'] = "3G"
    Sites = pd.read_csv(r'/home/ismayil/flask_dash/support_files/3G_sites.csv').values[:,
            1].tolist()
    return render_template('3G.html', title='3G', RNCs=RNCs, Vendors=Vendors, Sites=Sites, Regions=Regions,
                           KPIs=list(KPIs['3G'].keys()))


@server.route("/4G", methods=('GET', 'POST'))
def fourG():
    session['tech'] = "4G"
    Sites = pd.read_csv(r'/home/ismayil/flask_dash/support_files/4G_sites.csv').values[:,
            1].tolist()
    return render_template('4G.html', title='4G', Vendors=Vendors, Sites=Sites, Regions=Regions, KPIs=list(KPIs['4G'].keys()))

@server.route('/worst_sites/')
def render_dashboard():
    return flask.redirect('/dash3')

#@server.route('/worst_sites_daily/')
#def render_dashboard2():
#    return flask.redirect('/dash1')

@server.route('/dashboard/')
def render_reports():
    return flask.redirect('/dash2')

@server.route('/core_dashboard/')
def render_core():
    return flask.redirect('/core')

@server.route('/anomality/')
def render_anom():
    return flask.redirect('/anom')

@server.route('/billing_dashboard/')
def render_bill():
    return flask.redirect('/billing')

#@server.route('/flm2/')
#def render_flm():
#    return flask.redirect('/flm_dash')

@server.route('/flm_mobile/')
def render_flm2():
    return flask.redirect('/flm_dash2')

#@server.route('/utilization/')
#def render_util():
#    return flask.redirect('/util')

@server.route('/change_password', methods=['GET', 'POST'])
def change_password():
    # getting input with name = fname in HTML form
    username = str(request.form.get("user_name"))
    # getting input with name = lname in HTML form
    old_password = str(request.form.get("old_pass"))
    new_password = str(request.form.get("new_pass"))
    print(new_password)
    print(old_password)
    print(username)
    print('hello')
    p = subprocess.Popen('mkdir /home/ismayil/nese', shell=True,executable='/bin/bash', stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                         env = {"PATH" : "/usr/local/bin/:/usr/bin"})
    output, err = p.communicate(b"input data that is passed to subprocess' stdin")
    rc = p.returncode
    print(rc, 'return values')
    #executable = "/bin/bash",
    if request.method == 'POST':
        print(request.form['check'])
        if request.form['check'] == 'Submit':
            if subprocess.call("htpasswd -vb /home/ismayil/flask_dash/.userpass " + username +" " + old_password, shell=True,executable = "/bin/bash",
                               stdin=subprocess.PIPE,stdout=subprocess.PIPE, stderr=subprocess.PIPE, env = {"PATH" : "/usr/local/bin/:/usr/bin"})==0:
                subprocess.call("htpasswd -b /home/ismayil/flask_dash/.userpass " + username +" " + new_password, shell=True,executable = "/bin/bash",
                                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env={"PATH": "/usr/local/bin/:/usr/bin"}
                                )
                flash(u'Password successfully changed', 'success')
                return redirect(url_for("change_password"))
                    #Response('<Why access is denied string goes here...>', 401, {'WWW-Authenticate':'Basic realm="Login Required"'})
            else:
                flash(u'Username or password is incorrect','error')
                return redirect(url_for("change_password"))
    return render_template("change_password.html")

@server.route("/logout")
def account():
    return redirect(url_for("home"), code=401)


#@server.route("/bi")
#def bi_test():
#    return redirect("http://10.240.104.155:55")


app = DispatcherMiddleware(server, {
#    '/dash1': dashapp1.server,
    '/dash2': dashapp2.server,
    '/dash3': dashapp2.server,
    '/core': core.server,
    '/anom': anomality.server,
    '/billing': billing.server,
#    '/flm_dash': flm.server,
    '/flm_dash2': flm2.server
 #   '/util': util.server
})


if __name__ == '__main__':
    app.run(host='0.0.0.0')
