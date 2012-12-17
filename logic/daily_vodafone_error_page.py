import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import helper_mysql
import config

helper_mysql.quick_insert=True

def stat_moagent(my_date):

    oem_name='Vodafone'
    stat_category='moagent_error_page'

    # 20 Apr 12:00:56,851 - ResponseCode: 404    url=http://mobileshabik.morange.com/http%3a%2f%2fmobileshabik.morange.com%2fmobile_chatroom.aspx%3faction%3dchatroom_enter%26roomId%3d27%26roomName%3d%d8%a7%d9%84%d8%a7%d8%b3%d9%87%d9%85_%d9%88%d8%a7%d9%84%d8%b9%d9%85%d9%84%d8%a7%d8%aa%26password%3d?monetid=12810307&moclientwidth=230&userAgent=Symbian+Configuration%2FUnknown+Profiles%2F3.0+Encoding%2FUTF-8+Locale%2FUnknown+Lang%2Fen+Morange%2F5.2.1&moclientheight=266&cli_ip=212.118.143.150

    # 06 Apr 19:36:00,843 - ResponseCode: 404    url=http://mobilevoda.morange.com/moeventfeed:ShowMain?monetid=55002627&moclientwidth=320&userAgent=iOS%2F4.1+CiOS%2F110330+Encoding%2FUTF-8+Locale%2Fen_GB+Lang%2Fen+Morange%2F6.0.1+Caps%2F221+PI%2F0db33783050f5a0754105d51098eb29b+Domain%2Fvoda_egypt&moclientheight=416&devicewidth=320&deviceheight=480&cli_ip=41.206.139.183
    
    current_date=helper_regex.translate_date(sg_timestamp=my_date,timezone_offset_to_sg=config.timezone_offset_vodafone)
    
    stat_plan=Stat_plan(plan_name='daily-moagent-error-page-vodafone')
    
    #print helper_regex.extract(r'09 Nov 20:01:56,062 - Exception: Illegal character in URL	url=http://aladdin-voda-eg.i.mozat.com/image/game/OA/',r'(\.aspx|\.ashx|\.php|\.erl|\.jsp|\.html|\.htm|\/\w+(?:\/\s|\s|\/$|$|\?|\/+\?))')
    #exit()

    # by url and error type
    
    stat_sql_url_error_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_exist={'error':r'(.)'}, \
                                   where={'app_page':helper_regex.app_page_pattern}, \
                                   group_by={'by_type':r' - ([^:]+: [^\t]+)\turl=', \
                                             'by_url':helper_regex.get_simplified_url_unique_key, \
                                             'daily':r'(^\d+ \w+)'}, \
                                   db_name='data_url_pattern_vodafone', \
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
                                   db_name='data_url_pattern_vodafone', \
                                   target_stat_portal_db_conn=config._conn_stat_portal_158_2)

    stat_plan.add_stat_sql(stat_sql_error_daily)
    
    # by app
    
    stat_sql_by_app_error_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_exist={'error':r'(.)'}, \
                                   where={'app_page':helper_regex.app_page_pattern}, \
                                   group_by={'by_app':helper_regex.recognize_app_from_moagent_log_line, \
                                             'daily':r'(^\d+ \w+)'})

    stat_plan.add_stat_sql(stat_sql_by_app_error_daily)

    stat_plan.add_log_source(r'\\192.168.1.37\logs_moagent_vodafone\PageErrorInfo.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*') #expired
    
    stat_plan.add_log_source(r'\\192.168.1.67\logs_vodafone_moagent\PageErrorInfo.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')

    stat_plan.add_log_source(r'\\192.168.1.52\logs_vodafone_moagent\PageErrorInfo.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')

    stat_plan.add_log_source(r'\\192.168.1.60\logs_moagent_vodafone\PageErrorInfo.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')

    stat_plan.add_log_source(r'\\192.168.1.68\logs_moagent_vodafone\PageErrorInfo.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')

    stat_plan.add_log_source(r'\\192.168.1.71\logs_moagent_vodafone\PageErrorInfo.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')

    stat_plan.add_log_source(r'\\192.168.1.71\logs_moagent_vodafone_2\PageErrorInfo.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')

    stat_plan.add_log_source(r'\\192.168.1.73\logs_moagent_vodafone\PageErrorInfo.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')

    stat_plan.add_log_source(r'\\192.168.1.73\logs_moagent_vodafone_2\PageErrorInfo.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')

    stat_plan.add_log_source(r'\\192.168.1.74\logs_moagent_vodafone\PageErrorInfo.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')

    stat_plan.add_log_source(r'\\192.168.1.74\logs_moagent_vodafone_2\PageErrorInfo.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')

    stat_plan.add_log_source(r'\\192.168.1.75\logs_moagent_vodafone\PageErrorInfo.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')

    stat_plan.add_log_source(r'\\192.168.1.75\logs_moagent_vodafone_2\PageErrorInfo.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')

    stat_plan.add_log_source(r'\\192.168.1.78\logs_moagent_vodafone\PageErrorInfo.log.' \
           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')

    stat_plan.run()    

if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        stat_moagent(time.time()-3600*24*i)
