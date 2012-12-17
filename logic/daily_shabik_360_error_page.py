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

    oem_name='Shabik_360'
    stat_category='moagent_error_page'
    
    # 20 Apr 12:00:56,851 - ResponseCode: 404    url=http://mobileshabik.morange.com/http%3a%2f%2fmobileshabik.morange.com%2fmobile_chatroom.aspx%3faction%3dchatroom_enter%26roomId%3d27%26roomName%3d%d8%a7%d9%84%d8%a7%d8%b3%d9%87%d9%85_%d9%88%d8%a7%d9%84%d8%b9%d9%85%d9%84%d8%a7%d8%aa%26password%3d?monetid=12810307&moclientwidth=230&userAgent=Symbian+Configuration%2FUnknown+Profiles%2F3.0+Encoding%2FUTF-8+Locale%2FUnknown+Lang%2Fen+Morange%2F5.2.1&moclientheight=266&cli_ip=212.118.143.150
    
    current_date=helper_regex.translate_date(sg_timestamp=my_date,timezone_offset_to_sg=config.timezone_offset_shabik_360)

    stat_plan=Stat_plan()

    get_url_type=lambda line:'app_page' if helper_regex.is_app_page(line) else 'non_app_page'
    get_error_type=lambda line:helper_regex.extract(line,r' - ([^:]+: [^\t]+)\turl=') or 'unknown'
    get_current_date=lambda line:current_date

    # by url type
    
    stat_sql_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_exist={'error':r'(.)'}, \
                                   select_count_distinct={'affected_user':r'monetid=(\d+)'}, \
                                   where={'is_url':r'(http:)'}, \
                                   group_by={'by_url_type':get_url_type, \
                                             'daily':get_current_date}, \
                                   db_name='raw_data_shabik_360')

    stat_plan.add_stat_sql(stat_sql_daily)

    # by url type, error type, url pattern
    
    stat_sql_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_exist={'error':r'(.)'}, \
                                   select_count_distinct={'affected_user':r'monetid=(\d+)'}, \
                                   where={'is_url':r'(http:)'}, \
                                   group_by={'1.by_url_type':get_url_type, \
                                             '2.by_error_type':get_error_type, \
                                             '3.by_url_pattern':helper_regex.get_simplified_url_unique_key, \
                                             'daily':lambda line:current_date}, \
                                   db_name='data_url_pattern_shabik_360', \
                                   target_stat_portal_db_conn=config._conn_stat_portal_158_2)

    stat_plan.add_stat_sql(stat_sql_daily)

    # by app, url type
    
    stat_sql_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_exist={'error':r'(.)'}, \
                                   select_count_distinct={'affected_user':r'monetid=(\d+)'}, \
                                   where={'is_url':r'(http:)'}, \
                                   group_by={'by_app':helper_regex.recognize_app_from_moagent_log_line, \
                                             'by_url_type':get_url_type, \
                                             'daily':get_current_date}, \
                                   db_name='raw_data_shabik_360')

    stat_plan.add_stat_sql(stat_sql_daily)

    # by app, error type and url pattern
    
    stat_sql_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_exist={'error':r'(.)'}, \
                                   select_count_distinct={'affected_user':r'monetid=(\d+)'}, \
                                   where={'is_url':r'(http:)'}, \
                                   group_by={'1.by_app':helper_regex.recognize_app_from_moagent_log_line, \
                                             '2.by_url_type':get_url_type, \
                                             '3.by_error_type':get_error_type, \
                                             '4.by_url_pattern':helper_regex.get_simplified_url_unique_key, \
                                             'daily':get_current_date}, \
                                   db_name='data_url_pattern_shabik_360', \
                                   target_stat_portal_db_conn=config._conn_stat_portal_158_2)

    stat_plan.add_stat_sql(stat_sql_daily)
    

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.174\logs_moagent\PageErrorInfo.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.177\moAgent\PageErrorInfo.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.117\moAgent\PageErrorInfo.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.118\moAgent\PageErrorInfo.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.75\logs_moagent_shabik_360\PageErrorInfo.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.107\moAgent\PageErrorInfo.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.108\moAgent\PageErrorInfo.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.195\logs_moagent_shabik_360\PageErrorInfo.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))
    
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.185\logs_moagent_shabik_360\PageErrorInfo.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))
    
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.0.196\logs_moagent_shabik_360\PageErrorInfo.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_shabik_360,date_format='%Y-%m-%d'))
    stat_plan.run()    



if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):
        stat_moagent(time.time()-3600*24*i)
