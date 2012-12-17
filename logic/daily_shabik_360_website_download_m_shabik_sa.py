import helper_sql_server
import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_file
import helper_regex
import helper_mysql
import config
import common_shabik_360

helper_mysql.quick_insert=True

'''

2012-06-10 16:04:34 GET / no=966554558300 212.118.143.43 HTTP/1.1 BlackBerry8520/4.6.1.314+Profile/MIDP-2.0+Configuration/CLDC-1.1+VendorID/600 - 302 473 437

'''
def stat_website(my_date):

    global stat_plan_detailed,partial_request_temp

    current_date=datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')
    get_current_date=lambda line:current_date

    #2011-08-13 16:02:12 GET /360/stc.jar - 84.235.75.80 HTTP/1.1 Nokia7230/5.0+(06.90)+Profile/MIDP-2.1+Configuration/CLDC-1.1 - 200 517172 34003

    oem_name='Shabik_360'
    stat_category='website'
    
    db_name='raw_data_device_shabik_360'

    # msisdn accessed download page

    stat_plan=Stat_plan()
    
    stat_sql_download_user_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                        select_count_distinct_collection={'msisdn':r'no=(\d+)'},\
                                        where={'filtered_sms_broadcast':r'(n)o=\d+'}, \
                                        group_by={'daily':get_current_date},
                                        db_name=db_name)
    
    stat_plan.add_stat_sql(stat_sql_download_user_daily)

    stat_plan.add_log_source(r'\\192.168.0.175\w3svc581074064\ex' \
           +datetime.fromtimestamp(my_date).strftime('%y%m%d') \
           +'.log')


    stat_plan.dump_sources()

    stat_plan.run()    
    

    # msisdn accessed download page and redirected

    stat_plan=Stat_plan()
    
    stat_sql_download_user_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                        select_count_distinct_collection={'msisdn':r'no=(\d+)'},\
                                        where={'filtered_sms_broadcast_directed':r'(n)o=\d+'}, \
                                        group_by={'daily':get_current_date},
                                        db_name=db_name)
    
    stat_plan.add_stat_sql(stat_sql_download_user_daily)

    stat_plan.add_url_sources(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'http://212.100.219.210/W3SVC1160380090/ex%(date)s%(hour)s.log', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360+8,date_format='%y%m%d'))


    stat_plan.dump_sources()

    stat_plan.run()    
    

 
if __name__=='__main__':
    for i in range(config.day_to_update_stat,0,-1):
        my_date=time.time()-3600*24*i
        stat_website(my_date)
