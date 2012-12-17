from stat_plan import Stat_plan
from stat_sql import Stat_sql
from datetime import datetime
import time
import helper_regex
import helper_mysql
import common_vodafone
import config
import os

helper_mysql.quick_insert=True

current_date=''

def get_current_date(line):#bcz one log contains multiple dates
    global current_date
    return current_date

def stat_moagent(my_date):

    global current_date
    current_date=helper_regex.translate_date(sg_timestamp=my_date,timezone_offset_to_sg=config.timezone_offset_vodafone)

    oem_name='Vodafone'
    stat_category='moagent'

    # [old version] 08 Apr 17:27:09,956    7678648    406    390    16    -1    http://mobileshabik.morange.com/mobile_homepage.aspx?monetid=13032541&moclientwidth=230&userAgent=Symbian+Configuration%2FUnknown+Profiles%2F3.0+Encoding%2FUTF-8+Locale%2FUnknown+Lang%2Far+Morange%2F5.2.1&moclientheight=266&cli_ip=212.118.143.147

    # [from 2010-11-19] 18 Nov 00:56:43,761 - 7706880	390	15	32	15	328	3648	http://i-telkomsel-oceanage.mozat.com:8080/OceanAge/main_frame.jsp?monetid=106532&moclientwidth=230&userAgent=Unknown_Device%2FUnknown_FVersion+Encoding%2FUTF-8+Lang%2Fin+Caps%2F1+Morange%2F5.2.1+CS60%2F2.0.60+S60%2F32+&moclientheight=266&cli_ip=182.1.55.143
    # msgid/total time/grab time/parse time1/parse time 2/send packet time/ packet size

    stat_plan=Stat_plan(plan_name='daily-moagent-vodafone')

    # by url key
    
    stat_sql_url_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'visitor':{'key':r'monetid=(\d+)','date_units':[1,2,3]}}, \
                                   select_average={'internal_total_process_time':r'(?:[^\t]+\t){1}(\d+)', \
                                                   'fetch_page_time':r'(?:[^\t]+\t){2}(\d+)',
                                                   'html_parse_time':r'(?:[^\t]+\t){3}(\d+)',
                                                   'script_parse_time':r'(?:[^\t]+\t){4}(\d+)',
                                                   'send_packet_time':r'(?:[^\t]+\t){5}(\d+)',
                                                   'page_size':r'(?:[^\t]+\t){6}(\d+)',}, \
                                   where={'app_page':helper_regex.app_page_pattern}, \
                                   group_by={'by_url_pattern':helper_regex.get_simplified_url_unique_key,'daily':get_current_date}, \
                                   db_name='data_url_pattern_vodafone', \
                                   target_stat_portal_db_conn=config._conn_stat_portal_158_2)

    stat_plan.add_stat_sql(stat_sql_url_daily)

    """
    # phone model
    
    stat_sql_phone_model_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'user':r'monetid=(\d+)'}, \
                                   where={'app_page':helper_regex.app_page_pattern}, \
                                   group_by={'by_phone_model':helper_regex.extract_client_phone_model,'daily':get_current_date}, \
                                   db_name='raw_data_phone_model')

    stat_plan.add_stat_sql(stat_sql_phone_model_daily)

    # morange version
    
    stat_sql_version_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'user':r'monetid=(\d+)'}, \
                                   where={'app_page':helper_regex.app_page_pattern}, \
                                   group_by={'by_morange_version':helper_regex.extract_client_morange_version, \
                                             'daily':get_current_date})

    stat_plan.add_stat_sql(stat_sql_version_daily)

    # screen size

    stat_sql_screen_size_daily=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'user':r'monetid=(\d+)'}, \
                                   where={'app_page':helper_regex.app_page_pattern}, \
                                   group_by={'by_client_screen_size':helper_regex.extract_client_screen_size, \
                                             'daily':get_current_date})

    stat_plan.add_stat_sql(stat_sql_screen_size_daily)

    # useragent
    
    stat_sql_user_agent_str=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_first_text_value={'user_agent':r'userAgent=(.*?)(?:&|\s*$)'}, \
                                   #select_first_int_value={'client_width':r'moclientwidth=(\d+)', \
                                   #                        'client_height':r'moclientheight=(\d+)', \
                                   #                        'ip':helper_regex.ip_to_number}, \
                                   where={'from_app_request':r'(u)serAgent=[^&$]'}, \
                                   group_by={'by_monet_id':r'monetid=(\d+)'}, \
                                   db_name='data_int_user_info_vodafone', \
                                   target_stat_portal_db_conn=config._conn_stat_portal_158_2)

    stat_plan.add_stat_sql(stat_sql_user_agent_str)

    # by useragent 
    
    stat_sql_user_agent_key=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'visitor':r'monetid=(\d+)'}, \
                                   where={'from_app_request':r'(\.aspx|\.ashx|\.php|\.erl|\.jsp|\.html|\.htm|\/\w+(?:\/\s|\s|\/$|$|\?|\/+\?))'}, \
                                   group_by={'by_user_agent_key':helper_regex.extract_useragent_key, \
                                             'daily':get_current_date}, \
                                   db_name='raw_data_device')
    
    stat_plan.add_stat_sql(stat_sql_user_agent_key)

    """

    # special links
    
    stat_sql_special_links=Stat_sql(oem_name=oem_name,stat_category=stat_category, \
                                   select_count_distinct_collection={'visitor':r'monetid=(\d+)'}, \
                                   select_average={'fetch_page_time':r'(?:[^\t]+\t){2}(\d+)'}, \
                                   where={'special_link':r'(mobilevoda\.morange\.com|voda-oa\.|matrix)'}, \
                                   group_by={'link_pattern':r'(mobilevoda\.morange\.com|voda-oa\.|matrix)', \
                                             'hourly':r'(\d+ \w+ \d+)'}, \
                                   db_name='raw_data_device')
    
    stat_plan.add_stat_sql(stat_sql_special_links)


    """
    # add filter for recent new user

    filter_2d_new_user=common_vodafone.get_new_user_2d_filter(current_date,pattern=r'monetid=(\d+)')
    stat_plan.add_stat_brunch_filters({'only_2d_new_user':filter_2d_new_user})
    """
    
    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.37\logs_moagent_vodafone\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.67\logs_vodafone_moagent\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.52\logs_vodafone_moagent\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.60\logs_moagent_vodafone\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.68\logs_moagent_vodafone\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.71\logs_moagent_vodafone\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.71\logs_moagent_vodafone_2\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.73\logs_moagent_vodafone\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.73\logs_moagent_vodafone_2\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.74\logs_moagent_vodafone\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.74\logs_moagent_vodafone_2\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.75\logs_moagent_vodafone\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.75\logs_moagent_vodafone_2\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))

    stat_plan.add_log_source_list(helper_regex.translate_iis_website_hourly_log_path(
                                        my_date,r'\\192.168.1.78\logs_moagent_vodafone\internal_perf.log.%(date)s-%(hour)s', \
                                        timezone_offset_to_sg=config.timezone_offset_vodafone,date_format='%Y-%m-%d'))

    stat_plan.run()    

    stat_plan.dump_sources()





if __name__=='__main__':

    for i in range(config.day_to_update_stat,0,-1):#[1,2,3,4,5,6,7,8,10,12,13,14]:#
        stat_moagent(time.time()-3600*24*i)
