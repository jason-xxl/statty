import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config


def stat_moagent(my_date):

    oem_name='Viva_BH'
    stat_category='moagent_error_page'

    # 20 Apr 12:00:56,851 - ResponseCode: 404    url=http://mobileshabik.morange.com/http%3a%2f%2fmobileshabik.morange.com%2fmobile_chatroom.aspx%3faction%3dchatroom_enter%26roomId%3d27%26roomName%3d%d8%a7%d9%84%d8%a7%d8%b3%d9%87%d9%85_%d9%88%d8%a7%d9%84%d8%b9%d9%85%d9%84%d8%a7%d8%aa%26password%3d?monetid=12810307&moclientwidth=230&userAgent=Symbian+Configuration%2FUnknown+Profiles%2F3.0+Encoding%2FUTF-8+Locale%2FUnknown+Lang%2Fen+Morange%2F5.2.1&moclientheight=266&cli_ip=212.118.143.150

    stat_plan=Stat_plan(plan_name='daily-moagent-error-page-viva-bh')
    
    # by url and error type
    
    stat_sql_url_error_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_exist={'error':r'(.)'}, \
                                   where={'app_page':helper_regex.app_page_pattern}, \
                                   group_by={'by_type':r' - ([^:]+: [^\t]+)\turl=', \
                                             'by_url':helper_regex.get_simplified_url_unique_key, \
                                             'daily':r'(^\d+ \w+)'}, \
                                   db_name='data_url_pattern_viva_bh', \
                                   target_stat_portal_db_conn=config._conn_stat_portal_158_2)

    stat_plan.add_stat_sql(stat_sql_url_error_daily)
    
    # by error type
    
    stat_sql_error_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_exist={'error':r'(.)'}, \
                                   where={'app_page':helper_regex.app_page_pattern}, \
                                   group_by={'by_type':r' - ([^:]+: [^\t]+)\turl=', \
                                             'daily':r'(^\d+ \w+)'})

    stat_plan.add_stat_sql(stat_sql_error_daily)
    
    # by url
    
    stat_sql_error_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_exist={'error':r'(.)'}, \
                                   where={'app_page':helper_regex.app_page_pattern}, \
                                   group_by={'by_url':helper_regex.get_simplified_url_unique_key, \
                                             'daily':r'(^\d+ \w+)'}, \
                                   db_name='data_url_pattern_viva_bh', \
                                   target_stat_portal_db_conn=config._conn_stat_portal_158_2)

    stat_plan.add_stat_sql(stat_sql_error_daily)
            
    # by app
    
    stat_sql_by_app_error_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_exist={'error':r'(.)'}, \
                                   where={'app_page':helper_regex.app_page_pattern}, \
                                   group_by={'by_app':helper_regex.recognize_app_from_moagent_log_line, \
                                             'daily':r'(^\d+ \w+)'})

    stat_plan.add_stat_sql(stat_sql_by_app_error_daily)


    stat_plan.add_log_source(r'\\192.168.0.140\morange\moAgentBH\logs\PageError.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')
    stat_plan.add_log_source(r'\\192.168.0.141\logs_moagent_bh\PageError.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')
    stat_plan.add_log_source(r'\\192.168.0.174\logs_moagent_bh_360\PageError.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d'))


    stat_plan.add_log_source(r'\\192.168.0.140\morange\moAgentBH\logs\PageErrorInfo.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')
    stat_plan.add_log_source(r'\\192.168.0.141\logs_moagent_bh\PageErrorInfo.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')
    stat_plan.add_log_source(r'\\192.168.0.174\logs_moagent_bh_360\PageErrorInfo.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')

    stat_plan.run()    



if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        stat_moagent(time.time()-3600*24*i)
