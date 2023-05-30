from datetime  import datetime as dt

if dt.now().hour==9:
    try:
        import fetch_cacti
    except Exception as e:
        print(e)
        print('cacti error')
        1
    try:
        import cs_ps_traf
    except Exception as e:
        print(e)
        print('traffic error')
        1
#else:
try:
    import cs_kpi_report
except Exception as e:
    print(e)
    print('cs_kpi error')
    1
try:
    import users
except Exception as e:
    print(e)
    print('users error')
    1
try:
    import ps_kpi
except Exception as e:
    print(e)
    print('ps_kpi error')
    1
try:
    import lte_ps
except Exception as e:
    print(e)
    print('lte_ps_kpi error')
    1
try:
    import region
except Exception as e:
    print(e)
    print('region error')
