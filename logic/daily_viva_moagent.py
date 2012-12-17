import os
from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import config
import helper_mysql

helper_mysql.quick_insert=True

def stat_moagent(my_date):

    current_date=datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')

    oem_name='Viva'
    stat_category='moagent'

    # [old version] 08 Apr 17:27:09,956    7678648    406    390    16    -1    http://mobileshabik.morange.com/mobile_homepage.aspx?monetid=13032541&moclientwidth=230&userAgent=Symbian+Configuration%2FUnknown+Profiles%2F3.0+Encoding%2FUTF-8+Locale%2FUnknown+Lang%2Far+Morange%2F5.2.1&moclientheight=266&cli_ip=212.118.143.147

    # [from 2010-11-19] 18 Nov 00:56:43,761 - 7706880   390 15  32  15  328 3648    http://i-telkomsel-oceanage.mozat.com:8080/OceanAge/main_frame.jsp?monetid=106532&moclientwidth=230&userAgent=Unknown_Device%2FUnknown_FVersion+Encoding%2FUTF-8+Lang%2Fin+Caps%2F1+Morange%2F5.2.1+CS60%2F2.0.60+S60%2F32+&moclientheight=266&cli_ip=182.1.55.143
    # msgid/total time/grab time/parse time1/parse time 2/send packet time/ packet size


    stat_plan=Stat_plan(plan_name='daily-moagent-viva')
    
    # app uv

    stat_sql_app_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'visitor':r'monetid=(\d+)'}, \
#                                   select_average={'internal_total_process_time':r'(?:[^\t]+\t){1}(\d+)', \
#                                                   'fetch_page_time':r'(?:[^\t]+\t){2}(\d+)',
#                                                   'html_parse_time':r'(?:[^\t]+\t){3}(\d+)',
#                                                   'script_parse_time':r'(?:[^\t]+\t){4}(\d+)',
#                                                   'send_packet_time':r'(?:[^\t]+\t){5}(\d+)',
#                                                   'page_size':r'(?:[^\t]+\t){6}(\d+)',}, \
                                   where={'app_page':helper_regex.app_page_pattern}, \
                                   group_by={'daily':r'(^\d+ \w+)','by_app':helper_regex.recognize_app_from_moagent_log_line})

    stat_plan.add_stat_sql(stat_sql_app_daily)
    

    # uv

    stat_sql_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'visitor':r'monetid=(\d+)'}, \
#                                   select_average={'internal_total_process_time':r'(?:[^\t]+\t){1}(\d+)', \
#                                                   'fetch_page_time':r'(?:[^\t]+\t){2}(\d+)',
#                                                   'html_parse_time':r'(?:[^\t]+\t){3}(\d+)',
#                                                   'script_parse_time':r'(?:[^\t]+\t){4}(\d+)',
#                                                   'send_packet_time':r'(?:[^\t]+\t){5}(\d+)',
#                                                   'page_size':r'(?:[^\t]+\t){6}(\d+)',}, \
                                   where={'app_page':helper_regex.app_page_pattern}, \
                                   group_by={'daily':r'(^\d+ \w+)'})

    stat_plan.add_stat_sql(stat_sql_daily)


    # by url key
    
    stat_sql_url_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct={'visitor':r'monetid=(\d+)'}, \
#                                   select_average={'internal_total_process_time':r'(?:[^\t]+\t){1}(\d+)', \
#                                                   'fetch_page_time':r'(?:[^\t]+\t){2}(\d+)',
#                                                   'html_parse_time':r'(?:[^\t]+\t){3}(\d+)',
#                                                   'script_parse_time':r'(?:[^\t]+\t){4}(\d+)',
#                                                   'send_packet_time':r'(?:[^\t]+\t){5}(\d+)',
#                                                   'page_size':r'(?:[^\t]+\t){6}(\d+)',}, \
                                   where={'app_page':helper_regex.app_page_pattern}, \
                                   group_by={'by_url_pattern':helper_regex.get_simplified_url_unique_key,'daily':r'(^\d+ \w+)'}, \
                                   db_name='data_url_pattern_viva', \
                                   target_stat_portal_db_conn=config._conn_stat_portal_158_2)

    stat_plan.add_stat_sql(stat_sql_url_daily)


    # phone model
    
    stat_sql_phone_model_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'user':r'monetid=(\d+)'}, \
                                   where={'app_page':helper_regex.app_page_pattern}, \
                                   group_by={'by_phone_model':helper_regex.extract_client_phone_model,'daily':r'(^\d+ \w+)'}, \
                                   db_name='raw_data_phone_model')

    stat_plan.add_stat_sql(stat_sql_phone_model_daily)

    # morange version
    
    stat_sql_version_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'user':r'monetid=(\d+)'}, \
                                   where={'app_page':helper_regex.app_page_pattern}, \
                                   group_by={'by_morange_version':helper_regex.extract_client_morange_version,'daily':r'(^\d+ \w+)'})

    stat_plan.add_stat_sql(stat_sql_version_daily)

    # screen size
    
    stat_sql_screen_size_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'user':r'monetid=(\d+)'}, \
                                   where={'app_page':helper_regex.app_page_pattern}, \
                                   group_by={'by_client_screen_size':helper_regex.extract_client_screen_size,'daily':r'(^\d+ \w+)'})

    stat_plan.add_stat_sql(stat_sql_screen_size_daily)

 
    # morange version type
    
    stat_sql_version_type_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'user':r'monetid=(\d+)'}, \
                                   where={'app_page':helper_regex.app_page_pattern}, \
                                   group_by={'by_morange_version_type':helper_regex.extract_client_morange_version_type_with_s3_s5, \
                                             'daily':lambda line:current_date},
                                   db_name='raw_data')

    stat_plan.add_stat_sql(stat_sql_version_type_daily)


#    stat_plan.add_log_source(r'\\192.168.0.122\logs_moagent\internal_perf.log.' \
#           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')
#
#    stat_plan.add_log_source(r'\\192.168.0.123\logs_moagent\internal_perf.log.' \
#           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')
#
#    stat_plan.add_log_source(r'\\192.168.0.174\logs_moagent_kw_360\internal_perf.log.' \
#           +datetime.fromtimestamp(my_date).strftime('%Y-%m-%d')+'*')

    #internal_perf.log.2010-08-17 - internal_perf.log.2012-04-24-12
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
        my_date,r'\\192.168.0.123\logs_moagent\internal_perf.log.%(date)s-%(hour)s', \
        timezone_offset_to_sg=config.timezone_offset_viva,date_format='%Y-%m-%d'))
    
    # 2012-02-01-00 - internal_perf.log.2012-05-22-13
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
        my_date,r'\\192.168.0.122\logs_moagent\internal_perf.log.%(date)s-%(hour)s', \
        timezone_offset_to_sg=config.timezone_offset_viva,date_format='%Y-%m-%d'))
    
    # internal_perf.log.2012-07-10-00
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
        my_date,r'\\192.168.0.174\logs_moagent_kw_360\internal_perf.log.%(date)s-%(hour)s', \
        timezone_offset_to_sg=config.timezone_offset_viva,date_format='%Y-%m-%d'))

    stat_plan.run()    



if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        stat_moagent(time.time()-3600*24*i)
